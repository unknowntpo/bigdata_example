package org.example.hadoop;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class HDFSWriter {
    private final FileSystem fileSystem;
    private final ObjectMapper objectMapper;
    
    public HDFSWriter(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
        this.objectMapper = new ObjectMapper();
    }
    
    public HDFSWriter(String hdfsUri) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        this.fileSystem = FileSystem.get(conf);
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * Write a list of objects to HDFS as JSON lines
     */
    public <T> void writeAsJsonLines(List<T> objects, String hdfsPath) throws IOException {
        Path path = new Path(hdfsPath);
        
        // Create parent directories if they don't exist
        Path parent = path.getParent();
        if (parent != null && !fileSystem.exists(parent)) {
            fileSystem.mkdirs(parent);
        }
        
        try (FSDataOutputStream outputStream = fileSystem.create(path, true)) {
            for (T object : objects) {
                String json = objectMapper.writeValueAsString(object);
                outputStream.writeBytes(json);
                outputStream.writeBytes("\n");
            }
            outputStream.hflush();
        }
        
        System.out.println("Successfully wrote " + objects.size() + " objects to " + hdfsPath);
    }
    
    /**
     * Write objects to partitioned path (year/month/day/hour)
     */
    public <T> void writePartitioned(List<T> objects, String basePath, String dataType) throws IOException {
        LocalDateTime now = LocalDateTime.now();
        String partitionedPath = String.format("%s/%s/year=%d/month=%02d/day=%02d/hour=%02d/%s_%s.json",
            basePath,
            dataType,
            now.getYear(),
            now.getMonthValue(),
            now.getDayOfMonth(),
            now.getHour(),
            dataType,
            now.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
        );
        
        writeAsJsonLines(objects, partitionedPath);
    }
    
    /**
     * Check if path exists
     */
    public boolean exists(String hdfsPath) throws IOException {
        return fileSystem.exists(new Path(hdfsPath));
    }
    
    /**
     * List files in directory
     */
    public void listFiles(String hdfsPath) throws IOException {
        Path path = new Path(hdfsPath);
        if (fileSystem.exists(path)) {
            org.apache.hadoop.fs.FileStatus[] files = fileSystem.listStatus(path);
            System.out.println("Files in " + hdfsPath + ":");
            for (org.apache.hadoop.fs.FileStatus file : files) {
                System.out.println("  " + file.getPath().getName() + " (" + file.getLen() + " bytes)");
            }
        } else {
            System.out.println("Path does not exist: " + hdfsPath);
        }
    }
    
    public void close() throws IOException {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }
}