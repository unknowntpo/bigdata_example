package org.example.hadoop;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;

import java.io.IOException;
import java.net.URL;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * JUnit extension that automatically starts Hadoop services when tests run
 * - Starts services before all tests
 * - Cleans HDFS data before each test (for clean state)
 * - Preserves data created by tests after completion
 * - Keeps services running after tests for analysis
 */
public class HadoopTestExtension implements BeforeAllCallback, BeforeEachCallback {

    private static boolean hadoopStarted = false;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (!hadoopStarted) {
            System.out.println("🚀 Starting Hadoop services for tests...");
            startHadoopServices();
            waitForHadoopReady();
            hadoopStarted = true;
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        if (hadoopStarted) {
            System.out.println("🧹 Cleaning HDFS data before test: " + context.getDisplayName());
            // Only clean if this is a fresh test run, not if directories were just created
            cleanupHDFSData();
        }
    }

    private void startHadoopServices() throws IOException, InterruptedException {
        System.out.println("Starting Docker Compose Hadoop services...");
        ProcessBuilder pb = new ProcessBuilder("docker", "compose", "up", "-d", "namenode", "datanode");
        pb.directory(new java.io.File("../").getAbsoluteFile()); // Go to project root
        pb.inheritIO();

        Process process = pb.start();
        int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new RuntimeException("Failed to start Hadoop services. Exit code: " + exitCode);
        }
        System.out.println("Hadoop services started successfully");
    }

    private void cleanupHDFSData() {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            conf.set("dfs.client.use.datanode.hostname", "false");
            conf.setBoolean("dfs.permissions.enabled", false);

            FileSystem fs = FileSystem.get(conf);

            // Clean up common test directories
            String[] cleanupPaths = {"/test", "/data", "/tmp/test"};

            for (String pathStr : cleanupPaths) {
                Path path = new Path(pathStr);
                if (fs.exists(path)) {
                    boolean deleted = fs.delete(path, true);
                    System.out.println("  - Cleaned " + pathStr + ": " + (deleted ? "✅" : "❌"));
                }
            }

            fs.close();
        } catch (IOException e) {
            System.out.println("⚠️  Warning: Could not clean HDFS data: " + e.getMessage());
        }
    }

    private void waitForHadoopReady() throws InterruptedException {
        System.out.println("⏳ Waiting for Hadoop services to be ready...");

        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                URI uri = URI.create("http://localhost:9870");
                URL url = uri.toURL();
                var connection = url.openConnection();
                connection.setConnectTimeout(2000);
                connection.setReadTimeout(2000);
                connection.connect();
                connection.getInputStream().close();

                System.out.println("✅ Hadoop namenode is ready!");
                // Ensure HDFS is out of safe mode
                ensureHDFSOutOfSafeMode();
                return;
            } catch (Exception e) {
                if (attempt < 29) {
                    System.out.print(".");
                    Thread.sleep(2000);
                } else {
                    throw new RuntimeException("❌ Timeout waiting for Hadoop to start", e);
                }
            }
        }
    }

    private void ensureHDFSOutOfSafeMode() {
        // Try multiple approaches to ensure HDFS exits safe mode
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                System.out.println("🔓 Ensuring HDFS is out of safe mode (attempt " + (attempt + 1) + ")...");
                
                // First check if we're in safe mode
                ProcessBuilder checkPb = new ProcessBuilder("docker", "exec", "namenode", "hdfs", "dfsadmin", "-safemode", "get");
                checkPb.directory(new java.io.File("../").getAbsoluteFile());
                Process checkProcess = checkPb.start();
                checkProcess.waitFor();
                
                // Force leave safe mode
                ProcessBuilder leavePb = new ProcessBuilder("docker", "exec", "namenode", "hdfs", "dfsadmin", "-safemode", "leave");
                leavePb.directory(new java.io.File("../").getAbsoluteFile());
                leavePb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                leavePb.redirectError(ProcessBuilder.Redirect.INHERIT);
                Process leaveProcess = leavePb.start();
                int exitCode = leaveProcess.waitFor();
                
                if (exitCode == 0) {
                    System.out.println("✅ HDFS is out of safe mode");
                    Thread.sleep(2000); // Give it a moment to fully exit safe mode
                    return;
                } else {
                    System.out.println("⚠️ Could not take HDFS out of safe mode (exit code: " + exitCode + ")");
                }
                
                // Wait before retry
                Thread.sleep(3000);
                
            } catch (Exception e) {
                System.out.println("⚠️ Attempt " + (attempt + 1) + " failed: " + e.getMessage());
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        System.out.println("⚠️ Warning: Could not ensure HDFS safe mode after 5 attempts");
    }

}
