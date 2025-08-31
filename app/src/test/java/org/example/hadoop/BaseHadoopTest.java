package org.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

/**
 * Base class for tests that need embedded Hadoop cluster
 */
public abstract class BaseHadoopTest {
    
    protected MiniDFSCluster cluster;
    protected FileSystem fileSystem;
    protected Configuration conf;
    
    @TempDir
    File tempDir;
    
    @BeforeEach
    void setUpHadoop() throws IOException {
        conf = new Configuration();
        
        // Configure minicluster
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());
        conf.setBoolean("dfs.permissions.enabled", false);
        conf.set("hadoop.security.authentication", "simple");
        conf.setInt("dfs.namenode.handler.count", 1);
        conf.setInt("dfs.datanode.handler.count", 1);
        conf.setLong("dfs.block.size", 1048576L); // 1MB blocks for testing
        
        // Start cluster
        cluster = new MiniDFSCluster.Builder(conf)
                .numDataNodes(1)
                .build();
        
        cluster.waitClusterUp();
        fileSystem = cluster.getFileSystem();
        
        System.out.println("Embedded Hadoop cluster started at: " + cluster.getURI());
    }
    
    @AfterEach
    void tearDownHadoop() {
        if (fileSystem != null) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        if (cluster != null) {
            cluster.shutdown();
        }
    }
    
    /**
     * Get HDFS URI for connecting to the embedded cluster
     */
    protected String getHDFSUri() {
        return cluster.getURI().toString();
    }
}