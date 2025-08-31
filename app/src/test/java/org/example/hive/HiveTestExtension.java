package org.example.hive;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * JUnit extension that automatically starts Hive services when tests run
 * - Starts Postgres, Hive Metastore, and HiveServer2 before all tests
 * - Waits for services to be ready before proceeding
 * - Works alongside HadoopTestExtension for complete big data testing setup
 */
public class HiveTestExtension implements BeforeAllCallback {

    private static boolean hiveStarted = false;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (!hiveStarted) {
            System.out.println("🐝 Starting Hive services for tests...");
            startHiveServices();
            waitForHiveReady();
            hiveStarted = true;
        }
    }

    private void startHiveServices() throws IOException, InterruptedException {
        System.out.println("Starting Docker Compose Hive services...");
        
        // Start Hive service (Apache Hive with embedded metastore)
        ProcessBuilder pb = new ProcessBuilder("docker", "compose", "up", "-d", "hive");
        pb.directory(new java.io.File("../").getAbsoluteFile()); // Go to project root
        pb.inheritIO();

        Process process = pb.start();
        int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException("Failed to start Hive services. Exit code: " + exitCode);
        }
        System.out.println("✅ Hive services started successfully");
    }
    
    private void waitForHiveReady() throws InterruptedException {
        System.out.println("⏳ Waiting for Hive services to be ready...");

        // Wait for HiveServer2 with embedded metastore
        waitForHiveServer();
        
        System.out.println("🎉 Hive service is ready!");
    }

    private void waitForHiveServer() throws InterruptedException {
        System.out.println("⏳ Waiting for HiveServer2...");

        for (int attempt = 0; attempt < 180; attempt++) { // 6 minutes timeout for HiveServer2
            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress("localhost", 10000), 5000);
                socket.close();
                
                System.out.println("✅ HiveServer2 is ready!");
                
                // Give HiveServer2 additional time to fully initialize
                System.out.println("⏳ Allowing HiveServer2 additional initialization time...");
                Thread.sleep(10000); // Wait additional 10 seconds for full initialization
                
                System.out.println("✅ HiveServer2 fully initialized!");
                return;
                
            } catch (Exception e) {
                if (attempt < 179) {
                    System.out.print(".");
                    Thread.sleep(2000);
                } else {
                    System.err.println("❌ Timeout waiting for HiveServer2 to start");
                    System.err.println("💡 Check service status with: docker compose ps");
                    System.err.println("💡 Check logs with: docker compose logs hive");
                    throw new RuntimeException("Timeout waiting for HiveServer2 to start", e);
                }
            }
        }
    }
}