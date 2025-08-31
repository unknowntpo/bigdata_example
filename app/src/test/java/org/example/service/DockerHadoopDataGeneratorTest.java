package org.example.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.example.hadoop.HDFSWriter;
import org.example.model.Tweet;
import org.example.model.User;
import org.example.model.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.example.hadoop.HadoopTestExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test data generation with Docker Compose Hadoop cluster
 * 
 * Works both ways:
 * - From IDE: Click test button → Hadoop auto-starts via extension
 * - From Gradle: ./gradlew testHadoop → Hadoop starts via Gradle tasks
 */
@Tag("hadoop-docker-test")
@ExtendWith(HadoopTestExtension.class)
public class DockerHadoopDataGeneratorTest {
    
    private HDFSWriter hdfsWriter;
    private FileSystem fileSystem;
    
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
            System.err.println("docker compose up namenode datanode -d");
            throw e;
        }
    }
    
    @Test
    void testGenerateAndWriteToHadoop() throws IOException {
        System.out.println("=== Generating Twitter Analytics Data ===");
        
        // Generate celebrities (users with >100K followers)
        List<User> celebrities = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            User celebrity = DataGenerator.generateCelebrity();
            celebrities.add(celebrity);
            System.out.println("Generated celebrity: " + celebrity.getDisplayName() + 
                             " (" + celebrity.getFollowerCount() + " followers)");
        }
        
        // Generate regular users  
        List<User> regularUsers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            User user = DataGenerator.generateRegularUser();
            regularUsers.add(user);
        }
        
        // Combine all users
        List<User> allUsers = new ArrayList<>();
        allUsers.addAll(celebrities);
        allUsers.addAll(regularUsers);
        
        // Generate tweets from celebrities (higher engagement)
        List<Tweet> celebrityTweets = new ArrayList<>();
        for (User celebrity : celebrities) {
            for (int i = 0; i < 3; i++) { // 3 tweets per celebrity
                Tweet tweet = DataGenerator.generateTweet(celebrity);
                celebrityTweets.add(tweet);
            }
        }
        
        // Generate tweets from regular users
        List<Tweet> regularTweets = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Tweet tweet = DataGenerator.generateRegularTweet();
            regularTweets.add(tweet);
        }
        
        // Combine all tweets
        List<Tweet> allTweets = new ArrayList<>();
        allTweets.addAll(celebrityTweets);
        allTweets.addAll(regularTweets);
        
        // Generate engagement events
        List<Event> events = new ArrayList<>();
        for (Tweet tweet : allTweets) {
            // Generate 1-5 events per tweet
            int eventCount = 1 + (int)(Math.random() * 5);
            for (int i = 0; i < eventCount; i++) {
                Event event = DataGenerator.generateEvent(
                    "user_" + (int)(Math.random() * 1000),
                    tweet.getTweetId(),
                    tweet.isCelebrity(),
                    tweet.isCelebrity() ? tweet.getUserId() : null
                );
                events.add(event);
            }
        }
        
        System.out.println("\n=== Writing to HDFS ===");
        System.out.println("Users: " + allUsers.size() + " (Celebrities: " + celebrities.size() + ")");
        System.out.println("Tweets: " + allTweets.size() + " (Celebrity: " + celebrityTweets.size() + ")");
        System.out.println("Events: " + events.size());
        
        // Write to partitioned HDFS structure
        hdfsWriter.writePartitioned(allUsers, "/data", "users");
        hdfsWriter.writePartitioned(allTweets, "/data", "tweets");  
        hdfsWriter.writePartitioned(events, "/data", "events");
        
        // Verify data was written
        assertTrue(hdfsWriter.exists("/data/users"));
        assertTrue(hdfsWriter.exists("/data/tweets"));
        assertTrue(hdfsWriter.exists("/data/events"));
        
        System.out.println("\n=== HDFS Directory Structure ===");
        hdfsWriter.listFiles("/data");
        hdfsWriter.listFiles("/data/users");
        hdfsWriter.listFiles("/data/tweets");
        hdfsWriter.listFiles("/data/events");
        
        System.out.println("\n✅ Successfully generated and wrote Twitter analytics data to Hadoop!");
        System.out.println("You can view the files at: http://localhost:9870/explorer.html#/data");
    }
    
    @Test
    void testDataValidation() throws IOException {
        System.out.println("=== Testing Data Validation ===");
        
        // Generate and validate all data types
        User celebrity = DataGenerator.generateCelebrity();
        User regular = DataGenerator.generateRegularUser();
        Tweet celebrityTweet = DataGenerator.generateTweet(celebrity);
        Tweet regularTweet = DataGenerator.generateTweet(regular);
        Event event = DataGenerator.generateEvent("test_user", celebrityTweet.getTweetId(), true, celebrity.getUserId());
        
        // Validate all objects
        assertTrue(celebrity.isValid(), "Celebrity should be valid");
        assertTrue(regular.isValid(), "Regular user should be valid");
        assertTrue(celebrityTweet.isValid(), "Celebrity tweet should be valid");
        assertTrue(regularTweet.isValid(), "Regular tweet should be valid");
        assertTrue(event.isValid(), "Event should be valid");
        
        // Verify celebrity detection
        assertTrue(celebrity.isCelebrity(), "Generated celebrity should have >100K followers");
        assertFalse(regular.isCelebrity(), "Generated regular user should have <100K followers");
        assertTrue(celebrityTweet.isCelebrity(), "Celebrity tweet should be marked as celebrity");
        assertFalse(regularTweet.isCelebrity(), "Regular tweet should not be marked as celebrity");
        
        System.out.println("✅ All data validation tests passed!");
    }
    
    @Test 
    void testCelebriTyEngagementDifference() throws IOException {
        System.out.println("=== Testing Celebrity vs Regular User Engagement ===");
        
        User celebrity = DataGenerator.generateCelebrity();
        User regular = DataGenerator.generateRegularUser();
        
        Tweet celebrityTweet = DataGenerator.generateTweet(celebrity);
        Tweet regularTweet = DataGenerator.generateTweet(regular);
        
        // Celebrity tweets should have higher engagement
        System.out.println("Celebrity tweet engagement: " + 
                          "Likes=" + celebrityTweet.getLikeCount() + 
                          ", Retweets=" + celebrityTweet.getRetweetCount() + 
                          ", Replies=" + celebrityTweet.getReplyCount());
        
        System.out.println("Regular tweet engagement: " + 
                          "Likes=" + regularTweet.getLikeCount() + 
                          ", Retweets=" + regularTweet.getRetweetCount() + 
                          ", Replies=" + regularTweet.getReplyCount());
        
        // On average, celebrity tweets should have much higher engagement
        assertTrue(celebrityTweet.getLikeCount() >= regularTweet.getLikeCount(), 
                  "Celebrity tweets should generally have higher likes");
        
        System.out.println("✅ Celebrity engagement test completed!");
    }
}