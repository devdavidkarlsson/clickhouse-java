package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Manual validation script for Async HTTP features.
 *
 * Run with:
 *   mvn exec:java -pl client-v2 \
 *     -Dexec.mainClass="com.clickhouse.client.AsyncHttpManualValidation" \
 *     -Dexec.classpathScope=test \
 *     -Dexec.args="http://localhost:8123 default password"
 *
 * Or for ClickHouse Cloud:
 *   mvn exec:java -pl client-v2 \
 *     -Dexec.mainClass="com.clickhouse.client.AsyncHttpManualValidation" \
 *     -Dexec.classpathScope=test \
 *     -Dexec.args="https://your-host.clickhouse.cloud:8443 default your-password"
 */
public class AsyncHttpManualValidation {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        String endpoint = args.length > 0 ? args[0] : "http://localhost:8123";
        String username = args.length > 1 ? args[1] : "default";
        String password = args.length > 2 ? args[2] : "";

        System.out.println("============================================================");
        System.out.println("Async HTTP Manual Validation");
        System.out.println("============================================================");
        System.out.println("Endpoint: " + endpoint);
        System.out.println("Username: " + username);
        System.out.println();

        // Test with SYNC client first (baseline)
        System.out.println("--- SYNC CLIENT (Baseline) ---");
        try (Client syncClient = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(username)
                .setPassword(password)
                .useAsyncHttp(false)
                .build()) {

            testBasicQuery(syncClient, "SYNC");
        } catch (Exception e) {
            System.out.println("SYNC baseline failed: " + e.getMessage());
        }

        System.out.println();
        System.out.println("--- ASYNC CLIENT ---");

        // Test with ASYNC client
        try (Client asyncClient = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(username)
                .setPassword(password)
                .useAsyncHttp(true)
                .build()) {

            // Phase 1: Basic async queries
            testBasicQuery(asyncClient, "ASYNC");
            testQueryMetrics(asyncClient);
            testConcurrentQueries(asyncClient);

            // Phase 2: Streaming responses
            testStreamingResponse(asyncClient);
            testLargeResultStreaming(asyncClient);

            // Phase 3: Request compression
            testQueryWithHttpCompression(asyncClient, endpoint, username, password);
            testQueryWithNativeLZ4(asyncClient, endpoint, username, password);

            // Phase 4: Async inserts
            testBasicInsert(asyncClient);
            testLargeInsert(asyncClient);
            testInsertWithCompression(asyncClient, endpoint, username, password);

        } catch (Exception e) {
            e.printStackTrace();
            failed++;
        }

        System.out.println();
        System.out.println("============================================================");
        System.out.println("RESULTS: " + passed + " passed, " + failed + " failed");
        System.out.println("============================================================");

        System.exit(failed > 0 ? 1 : 0);
    }

    private static void testBasicQuery(Client client, String mode) {
        String testName = mode + " Basic Query";
        try {
            List<GenericRecord> records = client.queryAll("SELECT 1 as num, 'hello' as greeting");
            if (records.size() == 1 && records.get(0).getLong("num") == 1) {
                pass(testName);
            } else {
                fail(testName, "Unexpected result: " + records);
            }
        } catch (Exception e) {
            fail(testName, e);
        }
    }

    private static void testQueryMetrics(Client client) {
        String testName = "Query Metrics";
        try {
            QueryResponse response = client.query("SELECT number FROM numbers(100)").get(30, TimeUnit.SECONDS);
            if (response.getReadRows() > 0 && response.getQueryId() != null) {
                pass(testName + " (rows=" + response.getReadRows() + ", queryId=" + response.getQueryId() + ")");
            } else {
                fail(testName, "Missing metrics");
            }
            response.close();
        } catch (Exception e) {
            fail(testName, e);
        }
    }

    private static void testConcurrentQueries(Client client) {
        String testName = "Concurrent Queries (10)";
        try {
            @SuppressWarnings("unchecked")
            CompletableFuture<QueryResponse>[] futures = new CompletableFuture[10];
            for (int i = 0; i < 10; i++) {
                futures[i] = client.query("SELECT " + i + " as num, sleep(0.05)");
            }
            CompletableFuture.allOf(futures).get(60, TimeUnit.SECONDS);

            int successCount = 0;
            for (CompletableFuture<QueryResponse> f : futures) {
                if (f.get().getReadRows() > 0) successCount++;
                f.get().close();
            }

            if (successCount == 10) {
                pass(testName);
            } else {
                fail(testName, "Only " + successCount + "/10 succeeded");
            }
        } catch (Exception e) {
            fail(testName, e);
        }
    }

    private static void testStreamingResponse(Client client) {
        String testName = "Streaming Response";
        try {
            // Use TabSeparated format so we can count lines with BufferedReader
            QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.TabSeparated);
            QueryResponse response = client.query("SELECT number FROM numbers(1000)", settings).get(30, TimeUnit.SECONDS);
            BufferedReader reader = new BufferedReader(new InputStreamReader(response.getInputStream()));
            int lineCount = 0;
            while (reader.readLine() != null) lineCount++;
            response.close();

            if (lineCount == 1000) {
                pass(testName + " (1000 rows streamed)");
            } else {
                fail(testName, "Expected 1000 rows, got " + lineCount);
            }
        } catch (Exception e) {
            fail(testName, e);
        }
    }

    private static void testLargeResultStreaming(Client client) {
        String testName = "Large Result Streaming (100K rows)";
        try {
            long startMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

            // Use TabSeparated format so we can count lines with BufferedReader
            QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.TabSeparated);
            QueryResponse response = client.query("SELECT number, toString(number) FROM numbers(100000)", settings)
                    .get(60, TimeUnit.SECONDS);
            BufferedReader reader = new BufferedReader(new InputStreamReader(response.getInputStream()));
            int lineCount = 0;
            while (reader.readLine() != null) lineCount++;
            response.close();

            long endMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long memUsed = (endMem - startMem) / 1024 / 1024;

            if (lineCount == 100000) {
                pass(testName + " (mem delta ~" + memUsed + "MB)");
            } else {
                fail(testName, "Expected 100000 rows, got " + lineCount);
            }
        } catch (Exception e) {
            fail(testName, e);
        }
    }

    private static void testQueryWithHttpCompression(Client client, String endpoint, String user, String pass) {
        String testName = "Query with HTTP LZ4 Compression";
        try (Client compressClient = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(pass)
                .useAsyncHttp(true)
                .compressClientRequest(true)
                .useHttpCompression(true)
                .build()) {

            List<GenericRecord> records = compressClient.queryAll("SELECT number FROM numbers(100)");
            if (records.size() == 100) {
                pass(testName);
            } else {
                fail(testName, "Expected 100 rows, got " + records.size());
            }
        } catch (Exception e) {
            fail(testName, e);
        }
    }

    private static void testQueryWithNativeLZ4(Client client, String endpoint, String user, String pass) {
        String testName = "Query with Native LZ4 Compression";
        try (Client compressClient = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(pass)
                .useAsyncHttp(true)
                .compressClientRequest(true)
                .useHttpCompression(false)
                .build()) {

            List<GenericRecord> records = compressClient.queryAll("SELECT number FROM numbers(50)");
            if (records.size() == 50) {
                pass(testName);
            } else {
                fail(testName, "Expected 50 rows, got " + records.size());
            }
        } catch (Exception e) {
            fail(testName, e);
        }
    }

    private static void testBasicInsert(Client client) {
        String testName = "Basic Async Insert";
        String tableName = "async_test_basic_" + System.currentTimeMillis();
        try {
            client.query("CREATE TABLE " + tableName + " (id UInt64, name String) ENGINE = Memory")
                    .get(10, TimeUnit.SECONDS).close();

            String csvData = "1,Alice\n2,Bob\n3,Charlie\n";
            ByteArrayInputStream stream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));
            InsertResponse response = client.insert(tableName, stream, ClickHouseFormat.CSV)
                    .get(10, TimeUnit.SECONDS);

            List<GenericRecord> records = client.queryAll("SELECT count() FROM " + tableName);
            long count = records.get(0).getLong(1);

            client.query("DROP TABLE " + tableName).get(10, TimeUnit.SECONDS).close();

            if (count == 3) {
                pass(testName + " (3 rows inserted)");
            } else {
                fail(testName, "Expected 3 rows, got " + count);
            }
        } catch (Exception e) {
            fail(testName, e);
            try { client.query("DROP TABLE IF EXISTS " + tableName).get(5, TimeUnit.SECONDS); } catch (Exception ignored) {}
        }
    }

    private static void testLargeInsert(Client client) {
        String testName = "Large Async Insert (10K rows)";
        String tableName = "async_test_large_" + System.currentTimeMillis();
        try {
            client.query("CREATE TABLE " + tableName + " (id UInt64, data String) ENGINE = Memory")
                    .get(10, TimeUnit.SECONDS).close();

            StringBuilder csv = new StringBuilder();
            for (int i = 0; i < 10000; i++) {
                csv.append(i).append(",data_row_").append(i).append("\n");
            }
            ByteArrayInputStream stream = new ByteArrayInputStream(csv.toString().getBytes(StandardCharsets.UTF_8));

            long start = System.currentTimeMillis();
            client.insert(tableName, stream, ClickHouseFormat.CSV).get(60, TimeUnit.SECONDS);
            long elapsed = System.currentTimeMillis() - start;

            List<GenericRecord> records = client.queryAll("SELECT count() FROM " + tableName);
            long count = records.get(0).getLong(1);

            client.query("DROP TABLE " + tableName).get(10, TimeUnit.SECONDS).close();

            if (count == 10000) {
                pass(testName + " (" + elapsed + "ms)");
            } else {
                fail(testName, "Expected 10000 rows, got " + count);
            }
        } catch (Exception e) {
            fail(testName, e);
            try { client.query("DROP TABLE IF EXISTS " + tableName).get(5, TimeUnit.SECONDS); } catch (Exception ignored) {}
        }
    }

    private static void testInsertWithCompression(Client client, String endpoint, String user, String pass) {
        String testName = "Insert with Compression";
        String tableName = "async_test_compress_" + System.currentTimeMillis();
        try (Client compressClient = new Client.Builder()
                .addEndpoint(endpoint)
                .setUsername(user)
                .setPassword(pass)
                .useAsyncHttp(true)
                .compressClientRequest(true)
                .useHttpCompression(true)
                .build()) {

            compressClient.query("CREATE TABLE " + tableName + " (id UInt64, value String) ENGINE = Memory")
                    .get(10, TimeUnit.SECONDS).close();

            StringBuilder csv = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                csv.append(i).append(",value_").append(i).append("\n");
            }
            ByteArrayInputStream stream = new ByteArrayInputStream(csv.toString().getBytes(StandardCharsets.UTF_8));
            compressClient.insert(tableName, stream, ClickHouseFormat.CSV).get(30, TimeUnit.SECONDS);

            List<GenericRecord> records = compressClient.queryAll("SELECT count() FROM " + tableName);
            long count = records.get(0).getLong(1);

            compressClient.query("DROP TABLE " + tableName).get(10, TimeUnit.SECONDS).close();

            if (count == 1000) {
                pass(testName + " (1000 rows)");
            } else {
                fail(testName, "Expected 1000 rows, got " + count);
            }
        } catch (Exception e) {
            fail(testName, e);
        }
    }

    private static void pass(String testName) {
        System.out.println("[PASS] " + testName);
        passed++;
    }

    private static void fail(String testName, String reason) {
        System.out.println("[FAIL] " + testName + " - " + reason);
        failed++;
    }

    private static void fail(String testName, Exception e) {
        System.out.println("[FAIL] " + testName + " - " + e.getClass().getSimpleName() + ": " + e.getMessage());
        failed++;
    }
}
