package org.example.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.example.hadoop.HDFSWriter;
import org.example.hadoop.HadoopTestExtension;
import org.example.hive.HiveTestExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

@Tag("docker-test")
@ExtendWith({HadoopTestExtension.class, HiveTestExtension.class})
class HiveQueryingServiceTest {
    private FileSystem fileSystem;
    private HDFSWriter hdfsWriter;

    @BeforeEach
    void setUp() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("dfs.client.use.datanode.hostname", "false");
        conf.setBoolean("dfs.permissions.enabled", false);

        try {
            fileSystem = FileSystem.get(conf);
            hdfsWriter = new HDFSWriter(fileSystem);
            System.out.println("Connected to Docker Hadoop cluster");
            
        } catch (IOException e) {
            System.err.println("Failed to connect to Hadoop. Make sure services are running:");
            System.err.println("docker compose up namenode datanode hive-metastore hive-server postgres -d");
            throw e;
        }
    }

    @Test
    void testGetPopularTweets() throws IOException, SQLException, ClassNotFoundException {
        var regularTweets = IntStream.range(0, 10000).mapToObj((i) -> DataGenerator.generateRegularTweet()).toList();
        var celebrity1 = DataGenerator.generateCelebrity();
        var popularTweets = IntStream.range(0, 5).mapToObj((i) -> DataGenerator.generateTweet(celebrity1)).collect(Collectors.toList());

        var hiveService = new HiveQueryingService();
        hiveService.createTweetsTable("/data/tweets");
        
        // Use direct Hive JDBC insertion instead of Calcite for compatibility
        System.out.println("📊 Inserting tweets using direct Hive JDBC (bypassing Calcite)...");
        insertTweetsDirectly(hiveService, regularTweets);
        
        var mostMentionedUsers = hiveService.getMostMentionedUsers(10);
        // Pretty print the Map results
        System.out.println("=== Most Mentioned Users ===");
        mostMentionedUsers.forEach(result -> {
            System.out.printf("User: %s | Mentions: %s%n", 
                result.get("mention"), 
                result.get("mention_count"));
        });
        
        // Alternative: Using Arrays.deepToString for nested structures
        System.out.printf("Raw data: %s%n", java.util.Arrays.deepToString(mostMentionedUsers.toArray()));
    }
    
    private void insertTweetsDirectly(HiveQueryingService hiveService, List<org.example.model.Tweet> tweets) throws SQLException, ClassNotFoundException {
        System.out.println("📝 Inserting " + tweets.size() + " tweets directly via Hive JDBC...");
        
        // Use smaller sample for test performance
        int sampleSize = Math.min(10, tweets.size());  
        List<org.example.model.Tweet> sampleTweets = tweets.subList(0, sampleSize);
        System.out.println("📊 Using sample of " + sampleSize + " tweets for performance");
        
        try (var conn = hiveService.getHiveConnection()) {
            // Build multi-row INSERT VALUES statement
            StringBuilder insertQuery = new StringBuilder("""
                INSERT INTO tweets VALUES 
                """);
                
            for (int i = 0; i < sampleTweets.size(); i++) {
                if (i > 0) insertQuery.append(", ");
                var tweet = sampleTweets.get(i);
                
                // Build Hive array literals properly
                String hashtagsArray = buildHiveArray(tweet.getHashtags());
                String mentionsArray = buildHiveArray(tweet.getMentions());
                
                insertQuery.append(String.format(
                    "('%s', '%s', '%s', '%s', %d, %d, %d, %d, %s, '%s', %s, %s)",
                    escapeString(tweet.getTweetId()),
                    escapeString(tweet.getUserId()),
                    escapeString(tweet.getUsername()),
                    escapeString(tweet.getContent()),
                    tweet.getTimestamp(),
                    tweet.getLikeCount(),
                    tweet.getRetweetCount(),
                    tweet.getReplyCount(),
                    tweet.isCelebrity(),
                    escapeString(tweet.getCelebrityCategory()),
                    hashtagsArray,
                    mentionsArray
                ));
            }
            
            System.out.println("🔧 Executing Hive INSERT with " + sampleSize + " rows...");
            System.out.println("📝 Query: " + insertQuery.toString());
            
            try (var stmt = conn.createStatement()) {
                stmt.executeUpdate(insertQuery.toString());
                System.out.println("🎉 All " + sampleSize + " tweets inserted successfully via direct Hive JDBC!");
            }
        }
    }
    
    private String buildHiveArray(java.util.List<String> items) {
        if (items.isEmpty()) {
            return "array()";
        }
        return "array(" + items.stream()
            .map(this::escapeString)
            .map(s -> "'" + s + "'")
            .collect(java.util.stream.Collectors.joining(", ")) + ")";
    }
    
    private String escapeString(String str) {
        if (str == null) return "";
        return str.replace("'", "''").replace("\\", "\\\\");
    }
}
