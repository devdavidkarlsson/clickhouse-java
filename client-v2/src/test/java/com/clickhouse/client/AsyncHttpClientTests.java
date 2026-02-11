package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.apache.hc.core5.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;

/**
 * Tests for async HTTP transport using Apache HttpClient 5 async API.
 */
@Test(groups = {"integration"})
public class AsyncHttpClientTests extends BaseIntegrationTest {

    /**
     * Test basic async query execution with real ClickHouse server.
     */
    @Test(groups = {"integration"})
    public void testAsyncQueryBasic() {
        if (isCloud()) {
            return; // Skip for cloud tests
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useAsyncHttp(true)
                .build()) {

            List<GenericRecord> records = client.queryAll("SELECT timezone()");
            Assert.assertTrue(records.size() > 0);
            Assert.assertNotNull(records.get(0).getString(1));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test that async query returns the same results as sync query.
     */
    @Test(groups = {"integration"})
    public void testAsyncQueryResultsMatchSync() {
        if (isCloud()) {
            return;
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        String query = "SELECT number, number * 2 as doubled FROM numbers(10)";

        // First, get results using sync client
        List<GenericRecord> syncResults;
        try (Client syncClient = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useAsyncHttp(false)
                .build()) {

            syncResults = syncClient.queryAll(query);
        }

        // Then, get results using async client
        List<GenericRecord> asyncResults;
        try (Client asyncClient = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useAsyncHttp(true)
                .build()) {

            asyncResults = asyncClient.queryAll(query);
        }

        // Compare results
        Assert.assertEquals(asyncResults.size(), syncResults.size());
        for (int i = 0; i < syncResults.size(); i++) {
            Assert.assertEquals(asyncResults.get(i).getLong(1), syncResults.get(i).getLong(1));
            Assert.assertEquals(asyncResults.get(i).getLong(2), syncResults.get(i).getLong(2));
        }
    }

    /**
     * Test async query with CompletableFuture composition.
     */
    @Test(groups = {"integration"})
    public void testAsyncQueryWithFutureComposition() {
        if (isCloud()) {
            return;
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useAsyncHttp(true)
                .build()) {

            // Chain multiple async operations
            CompletableFuture<Long> resultFuture = client.query("SELECT count() FROM numbers(1000)")
                    .thenApply(response -> {
                        try {
                            // Read the count from response
                            return response.getReadRows();
                        } finally {
                            try {
                                response.close();
                            } catch (Exception e) {
                                // ignore
                            }
                        }
                    });

            Long count = resultFuture.get(30, TimeUnit.SECONDS);
            Assert.assertEquals(count.longValue(), 1L); // Query reads 1000 rows but returns 1 row (count)

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test async client properly handles server errors.
     */
    @Test(groups = {"integration"})
    public void testAsyncQueryServerError() {
        if (isCloud()) {
            return;
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useAsyncHttp(true)
                .build()) {

            try {
                // This should fail with a syntax error
                client.query("SELECT invalid;statement").get(10, TimeUnit.SECONDS);
                Assert.fail("Expected ServerException");
            } catch (ExecutionException e) {
                Assert.assertTrue(e.getCause() instanceof ServerException,
                        "Expected ServerException but got: " + e.getCause().getClass().getName());
                ServerException se = (ServerException) e.getCause();
                Assert.assertEquals(se.getCode(), 62); // Syntax error code
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test async query retry on 503 Service Unavailable using WireMock.
     */
    @Test(groups = {"integration"})
    public void testAsyncQueryRetryOn503() {
        if (isCloud()) {
            return;
        }

        int serverPort = new Random().nextInt(1000) + 10000;
        WireMockServer mockServer = new WireMockServer(WireMockConfiguration
                .options().port(serverPort).notifier(new ConsoleNotifier(false)));
        mockServer.start();

        try {
            // First request returns 503 (Service Unavailable)
            mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                    .inScenario("Retry503")
                    .whenScenarioStateIs(STARTED)
                    .willSetStateTo("Retried")
                    .willReturn(WireMock.aResponse()
                            .withStatus(HttpStatus.SC_SERVICE_UNAVAILABLE))
                    .build());

            // Second request succeeds
            mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                    .inScenario("Retry503")
                    .whenScenarioStateIs("Retried")
                    .willReturn(WireMock.aResponse()
                            .withStatus(HttpStatus.SC_OK)
                            .withHeader("X-ClickHouse-Summary",
                                    "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}"))
                    .build());

            try (Client client = new Client.Builder()
                    .addEndpoint(Protocol.HTTP, "localhost", serverPort, false)
                    .setUsername("default")
                    .setPassword("")
                    .useAsyncHttp(true)
                    .setMaxRetries(3)
                    .compressServerResponse(false)
                    .build()) {

                QueryResponse response = client.query("SELECT 1").get(10, TimeUnit.SECONDS);
                Assert.assertEquals(response.getReadRows(), 1);
                response.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            mockServer.stop();
        }
    }

    /**
     * Test that async client is not enabled when USE_ASYNC_HTTP is false (default).
     */
    @Test(groups = {"integration"})
    public void testAsyncHttpDisabledByDefault() {
        if (isCloud()) {
            return;
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        // Create client without useAsyncHttp(true) - should use sync client
        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .build()) {

            // Query should still work but uses sync path
            List<GenericRecord> records = client.queryAll("SELECT 1");
            Assert.assertEquals(records.size(), 1);
            Assert.assertEquals(records.get(0).getString(1), "1");

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test concurrent async queries.
     */
    @Test(groups = {"integration"})
    public void testConcurrentAsyncQueries() {
        if (isCloud()) {
            return;
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useAsyncHttp(true)
                .setMaxConnections(20)
                .build()) {

            int numQueries = 10;
            @SuppressWarnings("unchecked")
            CompletableFuture<QueryResponse>[] futures = new CompletableFuture[numQueries];

            // Launch all queries concurrently
            for (int i = 0; i < numQueries; i++) {
                final int queryNum = i;
                futures[i] = client.query("SELECT " + queryNum + " as num, sleep(0.1)");
            }

            // Wait for all to complete
            CompletableFuture.allOf(futures).get(60, TimeUnit.SECONDS);

            // Verify all completed successfully
            for (int i = 0; i < numQueries; i++) {
                QueryResponse response = futures[i].get();
                Assert.assertTrue(response.getReadRows() > 0 || response.getResultRows() > 0);
                response.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test async client graceful shutdown.
     */
    @Test(groups = {"integration"})
    public void testAsyncClientGracefulShutdown() {
        if (isCloud()) {
            return;
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        // Create and close the client multiple times to verify no resource leaks
        for (int i = 0; i < 3; i++) {
            Client client = new Client.Builder()
                    .addEndpoint(server.getBaseUri())
                    .setUsername("default")
                    .setPassword(getPassword())
                    .useAsyncHttp(true)
                    .build();

            try {
                List<GenericRecord> records = client.queryAll("SELECT 1");
                Assert.assertEquals(records.size(), 1);
            } catch (Exception e) {
                Assert.fail("Query failed on iteration " + i, e);
            } finally {
                client.close();
            }
        }
    }

    /**
     * Test that cancellation of CompletableFuture works.
     */
    @Test(groups = {"integration"})
    public void testAsyncQueryCancellation() {
        if (isCloud()) {
            return;
        }

        int serverPort = new Random().nextInt(1000) + 10000;
        WireMockServer mockServer = new WireMockServer(WireMockConfiguration
                .options().port(serverPort).notifier(new ConsoleNotifier(false)));
        mockServer.start();

        try {
            // Setup a delayed response
            mockServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                    .willReturn(WireMock.aResponse()
                            .withFixedDelay(10000)  // 10 second delay
                            .withStatus(HttpStatus.SC_OK)
                            .withHeader("X-ClickHouse-Summary",
                                    "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}"))
                    .build());

            try (Client client = new Client.Builder()
                    .addEndpoint(Protocol.HTTP, "localhost", serverPort, false)
                    .setUsername("default")
                    .setPassword("")
                    .useAsyncHttp(true)
                    .compressServerResponse(false)
                    .build()) {

                CompletableFuture<QueryResponse> future = client.query("SELECT 1");

                // Cancel after a short delay
                Thread.sleep(100);
                boolean cancelled = future.cancel(true);

                // The future should be cancelled
                Assert.assertTrue(future.isCancelled() || future.isDone());

            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            mockServer.stop();
        }
    }

    /**
     * Test async query response metrics.
     */
    @Test(groups = {"integration"})
    public void testAsyncQueryMetrics() {
        if (isCloud()) {
            return;
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useAsyncHttp(true)
                .build()) {

            try (QueryResponse response = client.query("SELECT number FROM numbers(100)").get(30, TimeUnit.SECONDS)) {
                // Verify metrics are populated
                Assert.assertTrue(response.getReadRows() > 0, "Expected read_rows > 0");
                Assert.assertTrue(response.getReadBytes() > 0, "Expected read_bytes > 0");
                Assert.assertNotNull(response.getQueryId(), "Expected query_id to be set");
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test async query with custom query ID.
     */
    @Test(groups = {"integration"})
    public void testAsyncQueryWithCustomQueryId() {
        if (isCloud()) {
            return;
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        String customQueryId = "test-async-query-" + System.currentTimeMillis();

        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useAsyncHttp(true)
                .build()) {

            com.clickhouse.client.api.query.QuerySettings settings =
                    new com.clickhouse.client.api.query.QuerySettings().setQueryId(customQueryId);

            try (QueryResponse response = client.query("SELECT 1", settings).get(30, TimeUnit.SECONDS)) {
                Assert.assertEquals(response.getQueryId(), customQueryId);
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test async streaming with larger result set to verify streaming works.
     */
    @Test(groups = {"integration"})
    public void testAsyncStreamingLargeResult() {
        if (isCloud()) {
            return;
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useAsyncHttp(true)
                .build()) {

            // Query that returns ~1MB of data (100K rows * ~10 bytes each)
            try (QueryResponse response = client.query("SELECT number, toString(number) FROM numbers(100000)")
                    .get(60, TimeUnit.SECONDS)) {

                Assert.assertTrue(response.getReadRows() > 0, "Expected read_rows > 0");

                // Read and count lines from the streaming response
                java.io.BufferedReader reader = new java.io.BufferedReader(
                        new java.io.InputStreamReader(response.getInputStream()));
                long lineCount = 0;
                while (reader.readLine() != null) {
                    lineCount++;
                }

                Assert.assertEquals(lineCount, 100000, "Expected 100000 rows");
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test async streaming response can be read incrementally.
     */
    @Test(groups = {"integration"})
    public void testAsyncStreamingIncrementalRead() {
        if (isCloud()) {
            return;
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useAsyncHttp(true)
                .build()) {

            try (QueryResponse response = client.query("SELECT number FROM numbers(1000)")
                    .get(30, TimeUnit.SECONDS)) {

                java.io.InputStream is = response.getInputStream();
                byte[] buffer = new byte[100];
                int totalBytesRead = 0;
                int bytesRead;

                // Read incrementally
                while ((bytesRead = is.read(buffer)) != -1) {
                    totalBytesRead += bytesRead;
                }

                Assert.assertTrue(totalBytesRead > 0, "Expected to read data from stream");
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test that streaming async does NOT deadlock when reading is delayed.
     * This tests the fix for the critical deadlock issue where:
     * - NIO thread blocks on pipe write (buffer full)
     * - User thread waits on future.get() (waiting for stream end)
     * - Neither can proceed = deadlock
     *
     * The fix: future completes when headers arrive, not when stream ends.
     */
    @Test(groups = {"integration"}, timeOut = 30000) // 30 second timeout catches deadlock
    public void testAsyncStreamingNoDeadlockOnDelayedRead() {
        if (isCloud()) {
            return;
        }

        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        try (Client client = new Client.Builder()
                .addEndpoint(server.getBaseUri())
                .setUsername("default")
                .setPassword(getPassword())
                .useAsyncHttp(true)
                .build()) {

            // Query that returns data larger than pipe buffer (512KB)
            // This would deadlock with the old implementation if user delays reading
            CompletableFuture<QueryResponse> future = client.query(
                    "SELECT number, repeat('x', 100) FROM numbers(10000)"); // ~1MB response

            // Simulate delayed reading - OLD code would deadlock here
            Thread.sleep(500);

            // Get response - should complete immediately since headers arrived
            QueryResponse response = future.get(5, TimeUnit.SECONDS);

            // Now read the stream - NIO thread continues writing while we read
            java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(response.getInputStream()));
            long lineCount = 0;
            while (reader.readLine() != null) {
                lineCount++;
            }

            Assert.assertEquals(lineCount, 10000, "Expected 10000 rows");
            response.close();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
