package com.clickhouse.client.api.internal;

import org.apache.hc.client5.http.async.methods.AbstractBinResponseConsumer;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Async response consumer that streams response body through a PipedInputStream.
 * Data is written to PipedOutputStream as it arrives from the NIO thread,
 * and can be read from the connected PipedInputStream in the user's thread.
 *
 * <p>IMPORTANT: The {@link #getHeadersFuture()} completes as soon as headers arrive,
 * allowing the caller to start reading from the stream immediately. This prevents
 * deadlock - the NIO thread can write while the user thread reads concurrently.</p>
 */
public class StreamingAsyncResponseConsumer extends AbstractBinResponseConsumer<StreamingAsyncResponseConsumer.StreamingResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingAsyncResponseConsumer.class);
    private static final int DEFAULT_PIPE_SIZE = 512 * 1024; // 512KB pipe buffer
    private static final int CAPACITY_INCREMENT = 8 * 1024; // 8KB chunks

    private final PipedInputStream pipedInputStream;
    private final PipedOutputStream pipedOutputStream;
    private final CompletableFuture<StreamingResponse> headersFuture;
    private final CompletableFuture<Void> streamCompleteFuture;
    private final AtomicBoolean outputClosed = new AtomicBoolean(false);

    private HttpResponse response;
    private ContentType contentType;
    private volatile Exception streamError;

    public StreamingAsyncResponseConsumer() {
        this(DEFAULT_PIPE_SIZE);
    }

    public StreamingAsyncResponseConsumer(int pipeSize) {
        this.pipedInputStream = new PipedInputStream(pipeSize);
        this.headersFuture = new CompletableFuture<>();
        this.streamCompleteFuture = new CompletableFuture<>();
        try {
            this.pipedOutputStream = new PipedOutputStream(pipedInputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create piped streams", e);
        }
    }

    /**
     * Returns a future that completes when HTTP headers are received.
     * Use this to get the response early and start reading from the stream
     * before all data has arrived. This prevents deadlock.
     */
    public CompletableFuture<StreamingResponse> getHeadersFuture() {
        return headersFuture;
    }

    @Override
    protected void start(HttpResponse response, ContentType contentType) throws IOException {
        this.response = response;
        this.contentType = contentType;
        LOG.debug("Streaming response started: status={}, contentType={}",
                response.getCode(), contentType);

        // Complete headers future immediately so caller can start reading
        StreamingResponse streamingResponse = new StreamingResponse(
                response, contentType, pipedInputStream, streamCompleteFuture);
        headersFuture.complete(streamingResponse);
    }

    @Override
    protected int capacityIncrement() {
        return CAPACITY_INCREMENT;
    }

    @Override
    protected void data(ByteBuffer src, boolean endOfStream) throws IOException {
        if (streamError != null) {
            return;
        }

        try {
            if (src.hasRemaining()) {
                byte[] bytes = new byte[src.remaining()];
                src.get(bytes);
                pipedOutputStream.write(bytes);
            }

            if (endOfStream) {
                closeOutputStream();
            }
        } catch (IOException e) {
            streamError = e;
            closeOutputStream();
            throw e;
        }
    }

    @Override
    protected StreamingResponse buildResult() {
        // Return the same response that was provided via headersFuture
        return new StreamingResponse(response, contentType, pipedInputStream, streamCompleteFuture);
    }

    @Override
    public void releaseResources() {
        closeOutputStream();
    }

    @Override
    public void failed(Exception cause) {
        LOG.debug("Streaming response failed", cause);
        streamError = cause;
        closeOutputStream();

        // Complete both futures exceptionally if not already completed
        headersFuture.completeExceptionally(cause);
        streamCompleteFuture.completeExceptionally(cause);
    }

    private void closeOutputStream() {
        if (outputClosed.compareAndSet(false, true)) {
            try {
                pipedOutputStream.close();
                streamCompleteFuture.complete(null);
            } catch (IOException e) {
                LOG.debug("Error closing piped output stream", e);
            }
        }
    }

    /**
     * Result object containing HTTP response metadata and the streaming InputStream.
     */
    public static class StreamingResponse {
        private final HttpResponse httpResponse;
        private final ContentType contentType;
        private final InputStream inputStream;
        private final CompletableFuture<Void> completeFuture;

        StreamingResponse(HttpResponse httpResponse, ContentType contentType,
                         InputStream inputStream, CompletableFuture<Void> completeFuture) {
            this.httpResponse = httpResponse;
            this.contentType = contentType;
            this.inputStream = inputStream;
            this.completeFuture = completeFuture;
        }

        public HttpResponse getHttpResponse() {
            return httpResponse;
        }

        public int getCode() {
            return httpResponse.getCode();
        }

        public Header getFirstHeader(String name) {
            return httpResponse.getFirstHeader(name);
        }

        public boolean containsHeader(String name) {
            return httpResponse.containsHeader(name);
        }

        public ContentType getContentType() {
            return contentType;
        }

        public InputStream getInputStream() {
            return inputStream;
        }

        public CompletableFuture<Void> getCompleteFuture() {
            return completeFuture;
        }

        public void close() throws IOException {
            inputStream.close();
        }
    }
}
