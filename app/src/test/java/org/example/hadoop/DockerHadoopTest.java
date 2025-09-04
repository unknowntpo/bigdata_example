package org.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;


import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test that connects to Docker Compose Hadoop cluster
 *
 * Works both ways:
 * - From IDE: Click test button → Hadoop auto-starts via extension
 * - From Gradle: ./gradlew testHadoop → Hadoop starts via Gradle tasks
 */
@Tag("docker-test")
@ExtendWith(HadoopTestExtension.class)
public class DockerHadoopTest {

    private FileSystem fileSystem;

    @BeforeEach
    void setUp() throws IOException {
        Configuration conf = new Configuration();

        // Connect to Docker Compose Hadoop cluster
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("dfs.client.use.datanode.hostname", "false");
        conf.setBoolean("dfs.permissions.enabled", false);

        try {
            fileSystem = FileSystem.get(conf);
            System.out.println("Connected to Hadoop cluster at: hdfs://localhost:9000");
        } catch (IOException e) {
            System.err.println("Failed to connect to Hadoop. Make sure Docker services are running:");
            System.err.println("docker compose up namenode datanode -d");
            throw e;
        }
    }

    @Test
    void testHDFSConnection() throws IOException {
        // Test basic HDFS operations
        Path testDir = new Path("/test");

        // Clean up if exists
        if (fileSystem.exists(testDir)) {
            fileSystem.delete(testDir, true);
        }

        // Create directory
        assertTrue(fileSystem.mkdirs(testDir));
        System.out.println("Created test directory: " + testDir);

        // Verify it exists
        assertTrue(fileSystem.exists(testDir));
        assertTrue(fileSystem.isDirectory(testDir));
    }

    @Test
    void testCreateDataDirectories() throws IOException {
        // Create the data structure we'll use for Twitter analytics
        String[] dataDirs = {
            "/data",
            "/data/tweets",
            "/data/users",
            "/data/events"
        };

        for (String dir : dataDirs) {
            Path path = new Path(dir);
            if (!fileSystem.exists(path)) {
                fileSystem.mkdirs(path);
                System.out.println("Created directory: " + dir);
            } else {
                System.out.println("Directory already exists: " + dir);
            }
        }

        // Verify all directories exist
        for (String dir : dataDirs) {
            assertTrue(fileSystem.exists(new Path(dir)), "Directory should exist: " + dir);
        }
    }
}
