package com.clickhouse.client.api.internal;

import net.jpountz.lz4.LZ4Factory;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.http.nio.DataStreamChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Async entity producer that streams data from an InputStream with optional LZ4 compression.
 * Supports on-the-fly compression without buffering the entire payload in memory.
 *
 * <p>For compression, data flows: User InputStream → Compression → PipedStream → NIO output</p>
 */
public class StreamingAsyncEntityProducer implements AsyncEntityProducer {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingAsyncEntityProducer.class);
    private static final int DEFAULT_BUFFER_SIZE = 8 * 1024; // 8KB read buffer
    private static final int PIPE_BUFFER_SIZE = 512 * 1024; // 512KB pipe buffer

    private final ContentType contentType;
    private final InputStream sourceStream;
    private final boolean compressData;
    private final boolean useHttpCompression;
    private final int compressionBufferSize;
    private final LZ4Factory lz4Factory;

    private final ByteBuffer readBuffer;
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicReference<Exception> error = new AtomicReference<>();

    // For compression: compress in background thread, read compressed data here
    private PipedInputStream compressedInputStream;
    private Thread compressionThread;
    private InputStream activeInputStream;

    public StreamingAsyncEntityProducer(InputStream sourceStream, ContentType contentType) {
        this(sourceStream, contentType, false, false, 0, null);
    }

    public StreamingAsyncEntityProducer(InputStream sourceStream, ContentType contentType,
                                        boolean compressData, boolean useHttpCompression,
                                        int compressionBufferSize, LZ4Factory lz4Factory) {
        this.sourceStream = sourceStream;
        this.contentType = contentType;
        this.compressData = compressData;
        this.useHttpCompression = useHttpCompression;
        this.compressionBufferSize = compressionBufferSize > 0 ? compressionBufferSize : ClickHouseLZ4OutputStream.UNCOMPRESSED_BUFF_SIZE;
        this.lz4Factory = lz4Factory;
        this.readBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        this.readBuffer.flip(); // Start empty
    }

    private void initializeStreams() throws IOException {
        if (activeInputStream != null) {
            return; // Already initialized
        }

        if (compressData && lz4Factory != null) {
            // Setup compression pipeline: sourceStream → compress → pipedStream → NIO
            PipedOutputStream compressedOutputStream = new PipedOutputStream();
            compressedInputStream = new PipedInputStream(compressedOutputStream, PIPE_BUFFER_SIZE);
            activeInputStream = compressedInputStream;

            // Start compression in background thread
            compressionThread = new Thread(() -> {
                try {
                    OutputStream compressingStream;
                    if (useHttpCompression) {
                        compressingStream = new org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream(compressedOutputStream);
                    } else {
                        compressingStream = new ClickHouseLZ4OutputStream(compressedOutputStream, lz4Factory.fastCompressor(), compressionBufferSize);
                    }

                    try {
                        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
                        int bytesRead;
                        while ((bytesRead = sourceStream.read(buffer)) != -1) {
                            compressingStream.write(buffer, 0, bytesRead);
                        }
                    } finally {
                        compressingStream.close();
                        compressedOutputStream.close();
                    }
                } catch (IOException e) {
                    error.set(e);
                    try {
                        compressedOutputStream.close();
                    } catch (IOException ignored) {}
                }
            }, "async-compression-thread");
            compressionThread.setDaemon(true);
            compressionThread.start();
        } else {
            // No compression - read directly from source
            activeInputStream = sourceStream;
        }
    }

    @Override
    public boolean isRepeatable() {
        return false; // Streaming is not repeatable
    }

    @Override
    public String getContentType() {
        return contentType != null ? contentType.toString() : null;
    }

    @Override
    public long getContentLength() {
        return -1; // Unknown length for streaming
    }

    @Override
    public int available() {
        try {
            initializeStreams();
            if (readBuffer.hasRemaining()) {
                return readBuffer.remaining();
            }
            return activeInputStream.available();
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public String getContentEncoding() {
        return null; // Content-Encoding header is set separately
    }

    @Override
    public boolean isChunked() {
        return true; // Always chunked for streaming
    }

    @Override
    public Set<String> getTrailerNames() {
        return null;
    }

    @Override
    public void produce(DataStreamChannel channel) throws IOException {
        initializeStreams();

        // Check for compression errors
        Exception compressionError = error.get();
        if (compressionError != null) {
            throw new IOException("Compression failed", compressionError);
        }

        // If buffer has data, write it first
        if (readBuffer.hasRemaining()) {
            channel.write(readBuffer);
            if (readBuffer.hasRemaining()) {
                return; // Channel couldn't accept all data, will be called again
            }
        }

        // Read more data from stream
        readBuffer.clear();
        byte[] array = readBuffer.array();
        int bytesRead = activeInputStream.read(array, 0, array.length);

        if (bytesRead == -1) {
            // End of stream
            completed.set(true);
            channel.endStream();
        } else if (bytesRead > 0) {
            readBuffer.limit(bytesRead);
            channel.write(readBuffer);
        }
    }

    @Override
    public void failed(Exception cause) {
        LOG.debug("Streaming entity producer failed", cause);
        error.set(cause);
        releaseResources();
    }

    @Override
    public void releaseResources() {
        completed.set(true);
        try {
            if (activeInputStream != null) {
                activeInputStream.close();
            }
            if (sourceStream != activeInputStream) {
                sourceStream.close();
            }
        } catch (IOException e) {
            LOG.debug("Error closing streams", e);
        }

        if (compressionThread != null && compressionThread.isAlive()) {
            compressionThread.interrupt();
            try {
                compressionThread.join(1000); // Wait up to 1 second for clean shutdown
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
