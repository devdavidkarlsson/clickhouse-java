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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
    private static final AtomicLong THREAD_COUNTER = new AtomicLong(0); // For unique thread names

    /**
     * Shared thread pool for compression tasks - bounded to prevent thread explosion under high concurrency.
     * Uses daemon threads so it won't prevent JVM shutdown if shutdown is not called.
     *
     * <p>Lifecycle management: Call {@link #acquireExecutor()} when creating an async client and
     * {@link #releaseExecutor()} when closing it. The pool is lazily created on first acquire and
     * shut down when the last client releases it.</p>
     */
    private static final int COMPRESSION_POOL_SIZE = Math.max(2, Runtime.getRuntime().availableProcessors());
    private static final Object EXECUTOR_LOCK = new Object();
    private static final AtomicInteger EXECUTOR_REF_COUNT = new AtomicInteger(0);
    private static volatile ExecutorService compressionExecutor = null;

    /**
     * Acquires a reference to the shared compression executor.
     * Call this when creating an async HTTP client that may use compression.
     * Must be paired with a call to {@link #releaseExecutor()} when the client is closed.
     */
    public static void acquireExecutor() {
        synchronized (EXECUTOR_LOCK) {
            if (EXECUTOR_REF_COUNT.getAndIncrement() == 0) {
                compressionExecutor = createCompressionExecutor();
                LOG.debug("Created compression executor pool");
            }
        }
    }

    /**
     * Releases a reference to the shared compression executor.
     * When the last reference is released, the executor is gracefully shut down.
     */
    public static void releaseExecutor() {
        synchronized (EXECUTOR_LOCK) {
            if (EXECUTOR_REF_COUNT.decrementAndGet() == 0 && compressionExecutor != null) {
                LOG.debug("Shutting down compression executor pool");
                compressionExecutor.shutdown();
                try {
                    if (!compressionExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        compressionExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    compressionExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                compressionExecutor = null;
            }
        }
    }

    private static ExecutorService createCompressionExecutor() {
        return new ThreadPoolExecutor(
                COMPRESSION_POOL_SIZE,
                COMPRESSION_POOL_SIZE,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000), // Bounded queue to provide backpressure
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "ch-async-compress-" + THREAD_COUNTER.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy() // If queue full, run in caller thread (backpressure)
        );
    }

    private static ExecutorService getExecutor() {
        ExecutorService exec = compressionExecutor;
        if (exec == null || exec.isShutdown()) {
            // Fallback: if executor not acquired properly, create inline (caller's thread)
            // This handles edge cases but logs a warning
            LOG.warn("Compression executor not acquired - compression will run in caller thread");
            return null;
        }
        return exec;
    }

    private final ContentType contentType;
    private final InputStream sourceStream;
    private final boolean compressData;
    private final boolean useHttpCompression;
    private final int compressionBufferSize;
    private final LZ4Factory lz4Factory;

    private final ByteBuffer readBuffer;
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicReference<Exception> error = new AtomicReference<>();

    // For compression: compress in thread pool, read compressed data here
    private PipedInputStream compressedInputStream;
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

            // Submit compression task to shared thread pool (or run inline if not available)
            ExecutorService executor = getExecutor();
            Runnable compressionTask = () -> {
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
                    } catch (IOException ignored) {
                    }
                    try {
                        sourceStream.close();
                    } catch (IOException ignored) {
                    }
                }
            };

            if (executor != null) {
                executor.submit(compressionTask);
            } else {
                // Fallback: run compression in a new thread (less efficient but functional)
                Thread t = new Thread(compressionTask, "ch-compress-fallback-" + THREAD_COUNTER.incrementAndGet());
                t.setDaemon(true);
                t.start();
            }
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
        // Closing streams will cause any running compression task to fail and exit
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
    }
}
