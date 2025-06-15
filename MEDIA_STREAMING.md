# Media Streaming in RS2

RS2 includes a comprehensive media streaming system with support for file and live streaming, codec operations, chunk processing, and priority-based delivery.

## Overview

The media streaming components in RS2 provide a robust framework for processing and streaming media content. The system supports both file-based and live streaming with features like chunk processing, priority-based delivery, and adaptive quality control.

## Core Components

- **MediaStreamingService**: High-level API for media streaming
- **MediaCodec**: Encoding and decoding of media data
- **ChunkProcessor**: Processing pipeline for media chunks
- **MediaPriorityQueue**: Priority-based delivery of media chunks

## Examples

### Basic File Streaming

For examples of streaming media from a file, see [examples/media_streaming/basic_file_streaming.rs](examples/basic_file_streaming.rs).

This example demonstrates:
- Creating a MediaStreamingService
- Configuring a media stream
- Starting streaming from a file
- Processing and displaying the media chunks

### Live Streaming

For examples of setting up a live stream, see [examples/media_streaming/live_streaming.rs](examples/live_streaming.rs).

This example demonstrates:
- Creating a MediaStreamingService for live streaming
- Configuring a live media stream
- Starting a live stream
- Processing and displaying the media chunks
- Monitoring stream metrics in real-time

### Custom Codec Configuration

For examples of configuring a custom codec, see [examples/media_streaming/custom_codec.rs](examples/custom_codec.rs).

This example demonstrates:
- Creating a custom codec configuration
- Creating a MediaCodec with the custom configuration
- Using the codec to encode and decode media data
- Monitoring codec performance

### Handling Stream Events

For examples of handling media stream events, see [examples/media_streaming/stream_events.rs](examples/stream_events.rs).

This example demonstrates:
- Creating and handling MediaStreamEvent objects
- Converting events to UserActivity for analytics
- Processing events in a stream
- Implementing a simple event handler

## Documentation

For comprehensive documentation on the media streaming components, see the [Media Streaming README](docs/media_streaming_readme.md).