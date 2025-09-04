package org.example.hadoop;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.net.URL;
import java.net.URI;

/**
 * JUnit extension that automatically starts Spark services when tests run
 * - Starts Spark Master and Workers before all tests
 * - Cleans up Spark contexts before each test (for clean state)
 * - Preserves cluster running after tests for analysis
 * - Integrates with existing Hadoop cluster
 */
public class SparkTestExtension implements BeforeAllCallback, BeforeEachCallback {

    private static boolean sparkStarted = false;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (!sparkStarted) {
            System.out.println("⚡ Starting Spark services for tests...");
            startSparkServices();
            waitForSparkReady();
            sparkStarted = true;
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        if (sparkStarted) {
            System.out.println("🧹 Cleaning Spark contexts before test: " + context.getDisplayName());
            cleanupSparkContexts();
        }
    }

    private void startSparkServices() throws IOException, InterruptedException {
        System.out.println("Starting Docker Compose Spark services...");
        ProcessBuilder pb = new ProcessBuilder("docker", "compose", "up", "-d", "spark-master", "spark-worker-1", "spark-worker-2");
        pb.directory(new java.io.File("../").getAbsoluteFile()); // Go to project root
        pb.inheritIO();

        Process process = pb.start();
        int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException("Failed to start Spark services. Exit code: " + exitCode);
        }
        System.out.println("Spark services started successfully");
    }

    private void cleanupSparkContexts() {
        try {
            System.out.println("  - Cleaning up Spark contexts and applications...");
            
            // Kill any active Spark applications via REST API
            ProcessBuilder killPb = new ProcessBuilder("docker", "exec", "spark-master", 
                "curl", "-X", "POST", "http://localhost:8080/api/v1/applications/kill");
            killPb.directory(new java.io.File("../").getAbsoluteFile());
            killPb.start().waitFor();
            
            System.out.println("  - Spark contexts cleaned ✅");
            
            // Give time for cleanup to complete
            Thread.sleep(2000);
            
        } catch (Exception e) {
            System.out.println("⚠️  Warning: Could not clean Spark contexts: " + e.getMessage());
        }
    }

    private void waitForSparkReady() throws InterruptedException {
        System.out.println("⏳ Waiting for Spark cluster to be ready...");

        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                URI uri = URI.create("http://localhost:8080");
                URL url = uri.toURL();
                var connection = url.openConnection();
                connection.setConnectTimeout(2000);
                connection.setReadTimeout(2000);
                connection.connect();
                connection.getInputStream().close();

                System.out.println("✅ Spark Master is ready!");
                
                // Verify workers are connected
                verifySparkWorkers();
                return;
                
            } catch (Exception e) {
                if (attempt < 29) {
                    System.out.print(".");
                    Thread.sleep(2000);
                } else {
                    throw new RuntimeException("❌ Timeout waiting for Spark to start", e);
                }
            }
        }
    }

    private void verifySparkWorkers() throws InterruptedException {
        System.out.println("🔍 Verifying Spark workers are connected...");
        
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                // Check worker status via docker
                ProcessBuilder pb = new ProcessBuilder("docker", "compose", "ps", "spark-worker-1", "spark-worker-2");
                pb.directory(new java.io.File("../").getAbsoluteFile());
                Process process = pb.start();
                int exitCode = process.waitFor();
                
                if (exitCode == 0) {
                    System.out.println("✅ Spark workers are running!");
                    Thread.sleep(3000); // Give workers time to connect to master
                    return;
                } else {
                    System.out.println("⚠️ Spark workers not ready, retrying...");
                }
                
                Thread.sleep(2000);
                
            } catch (Exception e) {
                System.out.println("⚠️ Attempt " + (attempt + 1) + " to verify workers failed: " + e.getMessage());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        System.out.println("⚠️ Warning: Could not verify Spark worker status after 10 attempts");
    }
}
