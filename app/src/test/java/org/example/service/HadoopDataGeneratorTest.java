package org.example.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.example.hadoop.HDFSWriter;
import org.example.hadoop.HadoopTestExtension;
import org.example.model.Tweet;
import org.example.model.User;
import org.example.model.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("hadoop-docker-test")
@ExtendWith(HadoopTestExtension.class)
public class HadoopDataGeneratorTest {

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
    void testGenerateAndWriteTweets() throws IOException {
        // Generate test data
        List<User> celebrities = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            celebrities.add(DataGenerator.generateCelebrity());
        }

        List<Tweet> tweets = new ArrayList<>();
        for (User celebrity : celebrities) {
            tweets.add(DataGenerator.generateTweet(celebrity));
        }

        // Add some regular tweets
        for (int i = 0; i < 2; i++) {
            tweets.add(DataGenerator.generateRegularTweet());
        }

        // Write to HDFS
        HDFSWriter writer = new HDFSWriter(fileSystem);

        // Test basic write
        String tweetsPath = "/data/tweets/test_tweets.json";
        writer.writeAsJsonLines(tweets, tweetsPath);

        // Verify file exists and has content
        assertTrue(fileSystem.exists(new Path(tweetsPath)));
        long fileSize = fileSystem.getFileStatus(new Path(tweetsPath)).getLen();
        assertTrue(fileSize > 0, "File should have content");

        System.out.println("Generated " + tweets.size() + " tweets");
        System.out.println("File size: " + fileSize + " bytes");

        // List files to verify
        writer.listFiles("/data/tweets/");
    }

    @Test
    void testPartitionedWrite() throws IOException {
        // Generate celebrities and their tweets
        List<User> users = new ArrayList<>();
        List<Tweet> tweets = new ArrayList<>();
        List<Event> events = new ArrayList<>();

        // Generate 2 celebrities
        for (int i = 0; i < 2; i++) {
            User celebrity = DataGenerator.generateCelebrity();
            users.add(celebrity);

            // Generate 2 tweets per celebrity
            for (int j = 0; j < 2; j++) {
                Tweet tweet = DataGenerator.generateTweet(celebrity);
                tweets.add(tweet);

                // Generate events for each tweet
                events.add(DataGenerator.generateEvent("user_" + i, tweet.getTweetId(), true, celebrity.getUserId()));
                events.add(DataGenerator.generateEvent("user_" + (i + 10), tweet.getTweetId(), true, celebrity.getUserId()));
            }
        }

        HDFSWriter writer = new HDFSWriter(fileSystem);

        // Write partitioned data
        writer.writePartitioned(users, "/data", "users");
        writer.writePartitioned(tweets, "/data", "tweets");
        writer.writePartitioned(events, "/data", "events");

        // Verify partitioned structure exists
        assertTrue(writer.exists("/data/users"));
        assertTrue(writer.exists("/data/tweets"));
        assertTrue(writer.exists("/data/events"));

        // List the partitioned structure
        writer.listFiles("/data");
        writer.listFiles("/data/users");
        writer.listFiles("/data/tweets");

        System.out.println("Successfully created partitioned data structure");
        System.out.println("Users: " + users.size() + ", Tweets: " + tweets.size() + ", Events: " + events.size());
    }

    @Test
    void testDataValidation() throws IOException {
        // Generate data and validate before writing
        List<Tweet> tweets = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            User user = i % 2 == 0 ? DataGenerator.generateCelebrity() : DataGenerator.generateRegularUser();
            Tweet tweet = DataGenerator.generateTweet(user);

            // Validate tweet before adding
            assertTrue(tweet.isValid(), "Generated tweet should be valid");
            tweets.add(tweet);
        }

        HDFSWriter writer = new HDFSWriter(fileSystem);
        writer.writeAsJsonLines(tweets, "/data/validated_tweets.json");

        assertTrue(writer.exists("/data/validated_tweets.json"));
        System.out.println("All " + tweets.size() + " tweets passed validation and were written to HDFS");
    }
}
