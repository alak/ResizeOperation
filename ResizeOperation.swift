/*
	Copyright (C) 2015 Apple Inc. All Rights Reserved.
	See LICENSE.txt for this sample’s licensing information

	Abstract:
	Defines a subclass of NSOperation that adjusts the color of a video file.
*/

import AVFoundation
import Dispatch


class ResizeOperation: NSOperation {
    // MARK: Types

	enum Result {
		case Success
		case Cancellation
		case Failure(NSError?)
	}

    // MARK: Properties

    override var executing: Bool {
        return result == nil
    }

    override var finished: Bool {
        return result != nil
    }

    private let asset: AVAsset

    private let outputURL: NSURL
    
    private let size: CGSize
    
    var result: Result? {
        willSet {
            willChangeValueForKey("isExecuting")
            willChangeValueForKey("isFinished")
        }
        didSet {
            didChangeValueForKey("isExecuting")
            didChangeValueForKey("isFinished")
        }
    }

    // MARK: Initialization

    init(sourceURL: NSURL, outputURL: NSURL, size: CGSize) {
		asset = AVURLAsset(URL: sourceURL, options: nil)
        self.size = size
		self.outputURL = outputURL
	}

    override var asynchronous: Bool {
        return true
    }

	// Every path through `start()` must call `finish()` exactly once.
	override func start() {

		if cancelled {
			finish(.Cancellation)
			return
		}


		// Load asset properties in the background, to avoid blocking the caller with synchronous I/O.
		asset.loadValuesAsynchronouslyForKeys(["tracks"]) { [weak self] in
            if let strongSelf = self {
                if strongSelf.cancelled {
                    strongSelf.finish(.Cancellation)
                    return
                }

                // These are all initialized in the below 'do' block, assuming no errors are thrown.
                let assetReader: AVAssetReader
                let assetWriter: AVAssetWriter
                let videoReaderOutputsAndWriterInputs: [ReaderOutputAndWriterInput]
                let audioReaderOutputsAndWriterInputs: [ReaderOutputAndWriterInput]
                let passthroughReaderOutputsAndWriterInputs: [ReaderOutputAndWriterInput]

            
            
                var error: NSError?
                strongSelf.asset.statusOfValueForKey("tracks", error: &error) != .Loaded
                if strongSelf.asset.statusOfValueForKey("tracks", error: &error) != .Loaded {
                    strongSelf.finish(.Failure(error))
                    return
                }

                let tracks = strongSelf.asset.tracks
                
                // Create reader/writer objects.
    
                assetReader = AVAssetReader(asset: strongSelf.asset, error: &error)
                if error != nil {
                    strongSelf.finish(.Failure(error))
                    return
                }
                
                
                assetWriter = AVAssetWriter(URL: strongSelf.outputURL, fileType: AVFileTypeMPEG4, error: &error)
                if error != nil {
                    strongSelf.finish(.Failure(error))
                    return
                }

                let (videoReaderOutputs, audioReaderOutputs, passthroughReaderOutputs) = strongSelf.makeReaderOutputsForTracks(tracks as! [AVAssetTrack], availableMediaTypes: assetWriter.availableMediaTypes as! [String], error: &error)
                
                if error != nil {
                    strongSelf.finish(.Failure(error))
                    return
                }
                
                videoReaderOutputsAndWriterInputs =  strongSelf.makeVideoWriterInputsForVideoReaderOutputs(videoReaderOutputs, error: &error)
                
                if error != nil {
                    strongSelf.finish(.Failure(error))
                    return
                }
                
                audioReaderOutputsAndWriterInputs =  strongSelf.makeAudioWriterInputsForAudioReaderOutputs(audioReaderOutputs, error: &error)
                
                if error != nil {
                    strongSelf.finish(.Failure(error))
                    return
                }
                
                passthroughReaderOutputsAndWriterInputs = strongSelf.makePassthroughWriterInputsForPassthroughReaderOutputs(passthroughReaderOutputs, error: &error)
                
                if error != nil {
                    strongSelf.finish(.Failure(error))
                    return
                }
                
                // Hook everything up.
                
                for (readerOutput, writerInput) in videoReaderOutputsAndWriterInputs {
                    assetReader.addOutput(readerOutput)
                    assetWriter.addInput(writerInput)
                }
                
                for (readerOutput, writerInput) in audioReaderOutputsAndWriterInputs {
                    assetReader.addOutput(readerOutput)
                    assetWriter.addInput(writerInput)
                }
                
                for (readerOutput, writerInput) in passthroughReaderOutputsAndWriterInputs {
                    assetReader.addOutput(readerOutput)
                    assetWriter.addInput(writerInput)
                }
                
                /*
                Remove file if necessary. AVAssetWriter will not overwrite
                an existing file.
                */
                
                let fileManager = NSFileManager()
                if let outputPath = strongSelf.outputURL.path where fileManager.fileExistsAtPath(outputPath) {
                    fileManager.removeItemAtPath(outputPath, error: &error)
                    
                    if error != nil {
                        strongSelf.finish(.Failure(error))
                        return
                    }
                }
                
                // Start reading/writing.
                if !assetReader.startReading() {
                    strongSelf.finish(.Failure(assetReader.error))
                    return
                }
                
                if !assetWriter.startWriting() {
                    // `error` is non-nil when startWriting returns false.
                    strongSelf.finish(.Failure(assetWriter.error))
                    return
                }
                
                assetWriter.startSessionAtSourceTime(kCMTimeZero)
                
                let writingGroup = dispatch_group_create()
                
                // Transfer data from input file to output file.
                strongSelf.transferVideoTracks(videoReaderOutputsAndWriterInputs, group: writingGroup)
                strongSelf.transferAudioTracks(audioReaderOutputsAndWriterInputs, group: writingGroup)
                strongSelf.transferPassthroughTracks(passthroughReaderOutputsAndWriterInputs, group: writingGroup)
                
                // Handle completion.
                let queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0)
                
                dispatch_group_notify(writingGroup, queue) {
                    // `readingAndWritingDidFinish()` is guaranteed to call `finish()` exactly once.
                    strongSelf.readingAndWritingDidFinish(assetReader, assetWriter: assetWriter)
                }
            }
		}
	}

    /**
        A type used for correlating an `AVAssetWriterInput` with the `AVAssetReaderOutput`
        that is the source of appended samples.
    */
	private typealias ReaderOutputAndWriterInput = (readerOutput: AVAssetReaderOutput, writerInput: AVAssetWriterInput)

    private func makeReaderOutputsForTracks(tracks: [AVAssetTrack], availableMediaTypes: [String], error: NSErrorPointer) -> (videoReaderOutputs: [AVAssetReaderTrackOutput], audioReaderOutputs: [AVAssetReaderTrackOutput], passthroughReaderOutputs: [AVAssetReaderTrackOutput]) {
		// Decompress source video to 32ARGB.
        let videoDecompressionSettings = [kCVPixelBufferPixelFormatTypeKey as String: Int(kCVPixelFormatType_32BGRA)]
        let audioDecompressionSettings = [AVFormatIDKey: kAudioFormatLinearPCM]
		// Partition tracks into "video" and "passthrough" buckets, create reader outputs.

		var videoReaderOutputs = [AVAssetReaderTrackOutput]()
        var audioReaderOutputs = [AVAssetReaderTrackOutput]()
		var passthroughReaderOutputs = [AVAssetReaderTrackOutput]()

		for track in tracks {
            if !contains(availableMediaTypes, track.mediaType) {
                continue
            }
			switch track.mediaType {
                case AVMediaTypeVideo:
                    let videoReaderOutput = AVAssetReaderTrackOutput(track: track, outputSettings: videoDecompressionSettings)
                    videoReaderOutputs += [videoReaderOutput]
                case AVMediaTypeAudio:
                    let audioReaderOutput = AVAssetReaderTrackOutput(track: track, outputSettings: audioDecompressionSettings)
                    audioReaderOutputs += [audioReaderOutput]
                
                default:
                    // `nil` output settings means "passthrough."
                    let passthroughReaderOutput = AVAssetReaderTrackOutput(track: track, outputSettings: nil)
                    passthroughReaderOutputs += [passthroughReaderOutput]
			}
		}

		return (videoReaderOutputs, audioReaderOutputs, passthroughReaderOutputs)
	}

    private func makeVideoWriterInputsForVideoReaderOutputs(videoReaderOutputs: [AVAssetReaderTrackOutput], error: NSErrorPointer) -> [ReaderOutputAndWriterInput] {
        
		// Compress modified source frames to H.264.
        let videoCleanApertureSettings = [AVVideoCleanApertureWidthKey: size.width, AVVideoCleanApertureHeightKey: size.height, AVVideoCleanApertureHorizontalOffsetKey: 10, AVVideoCleanApertureVerticalOffsetKey: 10]
        let codecSettings = [AVVideoAverageBitRateKey: 2000000, AVVideoMaxKeyFrameIntervalKey: 1, AVVideoCleanApertureKey: videoCleanApertureSettings]
        let videoCompressionSettings: [String: AnyObject] = [AVVideoCodecKey: AVVideoCodecH264, AVVideoCompressionPropertiesKey: codecSettings, AVVideoWidthKey: size.width, AVVideoHeightKey: size.height]

		/*
			In order to find the source format we need to create a temporary asset
            reader, plus a temporary track output for each "real" track output.
            We will only read as many samples (typically just one) as necessary
            to discover the format of the buffers that will be read from each "real"
            track output.
		*/
    
        
        let tempAssetReader = AVAssetReader(asset: asset, error: error)
        
        if tempAssetReader == nil {
            return []
        }

        let videoReaderOutputsAndTempVideoReaderOutputs: [(videoReaderOutput: AVAssetReaderTrackOutput, tempVideoReaderOutput: AVAssetReaderTrackOutput)] = videoReaderOutputs.map { videoReaderOutput in
            let tempVideoReaderOutput = AVAssetReaderTrackOutput(track: videoReaderOutput.track, outputSettings: videoReaderOutput.outputSettings)

            tempAssetReader.addOutput(tempVideoReaderOutput)

            return (videoReaderOutput, tempVideoReaderOutput)
        }

		// Start reading.
        if !tempAssetReader.startReading() {
            error.memory = tempAssetReader.error
			return []
		}

		/*
            Create video asset writer inputs, using the source format hints read
            from the "temporary" reader outputs.
        */

		var videoReaderOutputsAndWriterInputs = [ReaderOutputAndWriterInput]()

		for (videoReaderOutput, tempVideoReaderOutput) in videoReaderOutputsAndTempVideoReaderOutputs {
			// Fetch format of source sample buffers.

			var videoFormatHint: CMFormatDescriptionRef?

			while videoFormatHint == nil {
                if let sampleBuffer = tempVideoReaderOutput.copyNextSampleBuffer() {
                    videoFormatHint = CMSampleBufferGetFormatDescription(sampleBuffer)
                } else {
                    error.memory = NSError(domain: "CyanifyError.NoMediaData", code: -1, userInfo: [:])
                    return []
                }
			}

			// Create asset writer input.

			let videoWriterInput = AVAssetWriterInput(mediaType: AVMediaTypeVideo, outputSettings: videoCompressionSettings, sourceFormatHint: videoFormatHint)
			videoReaderOutputsAndWriterInputs.append((readerOutput: videoReaderOutput, writerInput: videoWriterInput))
		}

		// Shut down processing pipelines, since only a subset of the samples were read.
		tempAssetReader.cancelReading()

		return videoReaderOutputsAndWriterInputs
	}
    
    private func makeAudioWriterInputsForAudioReaderOutputs(audioReaderOutputs: [AVAssetReaderTrackOutput], error: NSErrorPointer) -> [ReaderOutputAndWriterInput] {
        
        // Compress modified source frames to H.264.
        //		let videoCompressionSettings: [String: AnyObject] = [
        //			AVVideoCodecKey: AVVideoCodecH264
        //		]
        
        let audioCompressionSettings: [String: AnyObject] = [AVFormatIDKey: kAudioFormatMPEG4AAC, AVNumberOfChannelsKey: 1, AVSampleRateKey: 44100, AVEncoderBitRateKey: 64000]
        
        /*
        In order to find the source format we need to create a temporary asset
        reader, plus a temporary track output for each "real" track output.
        We will only read as many samples (typically just one) as necessary
        to discover the format of the buffers that will be read from each "real"
        track output.
        */
        
        
        let tempAssetReader = AVAssetReader(asset: asset, error: error)
        
        if tempAssetReader == nil {
            return []
        }
        
        let audioReaderOutputsAndTempAudioReaderOutputs: [(audioReaderOutput: AVAssetReaderTrackOutput, tempAudioReaderOutput: AVAssetReaderTrackOutput)] = audioReaderOutputs.map { audioReaderOutput in
            let tempAudioReaderOutput = AVAssetReaderTrackOutput(track: audioReaderOutput.track, outputSettings: audioReaderOutput.outputSettings)
            
            tempAssetReader.addOutput(tempAudioReaderOutput)
            
            return (audioReaderOutput, tempAudioReaderOutput)
        }
        
        // Start reading.
        if !tempAssetReader.startReading() {
            error.memory = tempAssetReader.error
            return []
        }
        
        /*
        Create video asset writer inputs, using the source format hints read
        from the "temporary" reader outputs.
        */
        
        var audioReaderOutputsAndWriterInputs = [ReaderOutputAndWriterInput]()
        
        for (audioReaderOutput, tempAudioReaderOutput) in audioReaderOutputsAndTempAudioReaderOutputs {
            // Fetch format of source sample buffers.
            
            var audioFormatHint: CMFormatDescriptionRef?
            
            while audioFormatHint == nil {
                if let sampleBuffer = tempAudioReaderOutput.copyNextSampleBuffer() {
                    audioFormatHint = CMSampleBufferGetFormatDescription(sampleBuffer)
                } else {
                    error.memory = NSError(domain: "CyanifyError.NoMediaData", code: -1, userInfo: [:])
                    return []
                }
            }
            
            // Create asset writer input.
            
            let audioWriterInput = AVAssetWriterInput(mediaType: AVMediaTypeAudio, outputSettings: audioCompressionSettings, sourceFormatHint: audioFormatHint)
            audioReaderOutputsAndWriterInputs.append((readerOutput: audioReaderOutput, writerInput: audioWriterInput))
        }
        
        // Shut down processing pipelines, since only a subset of the samples were read.
        tempAssetReader.cancelReading()
        
        return audioReaderOutputsAndWriterInputs
    }

    private func makePassthroughWriterInputsForPassthroughReaderOutputs(passthroughReaderOutputs: [AVAssetReaderTrackOutput], error: NSErrorPointer) -> [ReaderOutputAndWriterInput] {
		/*
            Create passthrough writer inputs, using the source track's format
            descriptions as the format hint for each writer input.
        */


		var passthroughReaderOutputsAndWriterInputs = [ReaderOutputAndWriterInput]()

		for passthroughReaderOutput in passthroughReaderOutputs {
			/*
                For passthrough, we can simply ask the track for its format
                description and use that as the writer input's format hint.
            */
			let trackFormatDescriptions = passthroughReaderOutput.track.formatDescriptions as! [CMFormatDescriptionRef]

			if let passthroughFormatHint = trackFormatDescriptions.first {
                // Create asset writer input with nil (passthrough) output settings
                let passthroughWriterInput = AVAssetWriterInput(mediaType: passthroughReaderOutput.mediaType, outputSettings: nil, sourceFormatHint: passthroughFormatHint)
                
                passthroughReaderOutputsAndWriterInputs.append((readerOutput: passthroughReaderOutput, writerInput: passthroughWriterInput))
            } else {
                error.memory = NSError(domain: "CyanifyError.NoMediaData", code: -1, userInfo: [:])
                return []
            }

			
		}

		return passthroughReaderOutputsAndWriterInputs
	}

	private func transferVideoTracks(videoReaderOutputsAndWriterInputs: [ReaderOutputAndWriterInput], group: dispatch_group_t) {
		for (videoReaderOutput, videoWriterInput) in videoReaderOutputsAndWriterInputs {
			let perTrackDispatchQueue = dispatch_queue_create("Track data transfer queue: \(videoReaderOutput) -> \(videoWriterInput).", nil)

			// A block for changing color values of each video frame.
//            let videoProcessor: CMSampleBufferRef throws -> Void = { sampleBuffer in
//				if let imageBuffer = CMSampleBufferGetImageBuffer(sampleBuffer),
//                       pixelBuffer: CVPixelBufferRef = imageBuffer
//                    where CFGetTypeID(imageBuffer) == CVPixelBufferGetTypeID() {
//
//                    let redComponentIndex = 1
//                    try pixelBuffer.removeARGBColorComponentAtIndex(redComponentIndex)
//                }
//			}

			dispatch_group_enter(group)
			transferSamplesAsynchronouslyFromReaderOutput(videoReaderOutput, toWriterInput: videoWriterInput, onQueue: perTrackDispatchQueue) {
				dispatch_group_leave(group)
			}
		}
	}
    
    private func transferAudioTracks(audioReaderOutputsAndWriterInputs: [ReaderOutputAndWriterInput], group: dispatch_group_t) {
        for (audioReaderOutput, audioWriterInput) in audioReaderOutputsAndWriterInputs {
            let perTrackDispatchQueue = dispatch_queue_create("Track data transfer queue: \(audioReaderOutput) -> \(audioWriterInput).", nil)
            
            dispatch_group_enter(group)
            transferSamplesAsynchronouslyFromReaderOutput(audioReaderOutput, toWriterInput: audioWriterInput, onQueue: perTrackDispatchQueue) {
                dispatch_group_leave(group)
            }
        }
    }

	private func transferPassthroughTracks(passthroughReaderOutputsAndWriterInputs: [ReaderOutputAndWriterInput], group: dispatch_group_t) {
        for (passthroughReaderOutput, passthroughWriterInput) in passthroughReaderOutputsAndWriterInputs {
			let perTrackDispatchQueue = dispatch_queue_create("Track data transfer queue: \(passthroughReaderOutput) -> \(passthroughWriterInput).", nil)

			dispatch_group_enter(group)
			transferSamplesAsynchronouslyFromReaderOutput(passthroughReaderOutput, toWriterInput: passthroughWriterInput, onQueue: perTrackDispatchQueue) {
				dispatch_group_leave(group)
			}
		}
	}

	private func transferSamplesAsynchronouslyFromReaderOutput(readerOutput: AVAssetReaderOutput, toWriterInput writerInput: AVAssetWriterInput, onQueue queue: dispatch_queue_t, sampleBufferProcessor: CMSampleBufferRef? = nil, completionHandler: Void -> Void) {

		// Provide the asset writer input with a block to invoke whenever it wants to request more samples

		writerInput.requestMediaDataWhenReadyOnQueue(queue) {
			var isDone = false

			/*
				Loop, transferring one sample per iteration, until the asset writer
                input has enough samples. At that point, exit the callback block
                and the asset writer input will invoke the block again when it
                needs more samples.
			*/
			while writerInput.readyForMoreMediaData {
                
                if self.cancelled {
                    isDone = true
                    return
                }

                let sampleBuffer = readerOutput.copyNextSampleBuffer()
				// Grab next sample from the asset reader output.
				if sampleBuffer == nil {
					/*
                        At this point, the asset reader output has no more samples
                        to vend.
                    */
					isDone = true
					break
				}

//				// Process the sample, if requested.
//				do {
//					try sampleBufferProcessor?(sampleBuffer: sampleBuffer)
//				}
//				catch {
//					// This error will be picked back up in `readingAndWritingDidFinish()`.
//					self.sampleTransferError = error
//					isDone = true
//				}

				// Append the sample to the asset writer input.
                if !writerInput.appendSampleBuffer(sampleBuffer) {
                    isDone = true
                    break
                }
			}

			if isDone {
				/*
					Calling `markAsFinished()` on the asset writer input will both:
						1. Unblock any other inputs that need more samples.
						2. Cancel further invocations of this "request media data"
                           callback block.
				*/
				writerInput.markAsFinished()

				// Tell the caller that we are done transferring samples.
				completionHandler()
			}
		}
	}

	private func readingAndWritingDidFinish(assetReader: AVAssetReader, assetWriter: AVAssetWriter) {
		if cancelled {
			assetReader.cancelReading()
			assetWriter.cancelWriting()
		}

		// Deal with any error that occurred during processing of the video.
//		if sampleTransferError != nil  {
//			assetReader.cancelReading()
//			assetWriter.cancelWriting()
//			finish(.Failure(sampleTransferError!))
//			return
//		}

		// Evaluate result of reading samples.

		if assetReader.status != .Completed {
			let result: Result

			switch assetReader.status {
                case .Cancelled:
                    assetWriter.cancelWriting()
                    result = .Cancellation

                case .Failed:
                    // `error` property is non-nil in the `.Failed` status.
                    result = .Failure(assetReader.error!)

                default:
                    fatalError("Unexpected terminal asset reader status: \(assetReader.status).")
			}

			finish(result)

            return
		}

		// Finish writing, (asynchronously) evaluate result of writing samples.

		assetWriter.finishWritingWithCompletionHandler {
			let result: Result

			switch assetWriter.status {
                case .Completed:
                    result = .Success

                case .Cancelled:
                    result = .Cancellation

                case .Failed:
                    // `error` property is non-nil in the `.Failed` status.
                    result = .Failure(assetWriter.error!)

                default:
                    fatalError("Unexpected terminal asset writer status: \(assetWriter.status).")
			}

			self.finish(result)
		}
	}

	func finish(result: Result) {
		self.result = result
	}
}

//extension CVPixelBufferRef {
//	/**
//        Iterates through each pixel in the receiver (assumed to be in ARGB format)
//        and overwrites the color component at the given index with a zero. This
//        has the effect of "cyanifying," "rosifying," etc (depending on the chosen
//        color component) the overall image represented by the pixel buffer.
//    */
//	func removeARGBColorComponentAtIndex(componentIndex: size_t) throws {
//		let lockBaseAddressResult = CVPixelBufferLockBaseAddress(self, 0)
//
//		guard lockBaseAddressResult == kCVReturnSuccess else {
//			throw NSError(domain: NSOSStatusErrorDomain, code: Int(lockBaseAddressResult), userInfo: nil)
//		}
//
//		let bufferHeight = CVPixelBufferGetHeight(self)
//
//        let bufferWidth = CVPixelBufferGetWidth(self)
//
//        let bytesPerRow = CVPixelBufferGetBytesPerRow(self)
//
//        let bytesPerPixel = bytesPerRow / bufferWidth
//
//        let base = UnsafeMutablePointer<Int8>(CVPixelBufferGetBaseAddress(self))
//
//		// For each pixel, zero out selected color component.
//		for row in 0..<bufferHeight {
//			for column in 0..<bufferWidth {
//				let pixel: UnsafeMutablePointer<Int8> = base + (row * bytesPerRow) + (column * bytesPerPixel)
//				pixel[componentIndex] = 0
//			}
//		}
//
//		let unlockBaseAddressResult = CVPixelBufferUnlockBaseAddress(self, 0)
//
//		guard unlockBaseAddressResult == kCVReturnSuccess else {
//			throw NSError(domain: NSOSStatusErrorDomain, code: Int(unlockBaseAddressResult), userInfo: nil)
//		}
//	}
//}
