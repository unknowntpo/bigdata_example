package org.example.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TweetTest {

    @Test
    void serde() throws IOException {
        // Test data variables
        String tweetId = "tweet_123";
        String userId = "user_456";
        String username = "john_doe";
        String content = "Hello #bigdata world!";
        long timestamp = 1756259147L;
        int likeCount = 42;
        int retweetCount = 15;
        int replyCount = 3;
        boolean isCelebrity = true;
        String celebrityCategory = "tech";

        // Create tweet using helper
        var tweet = createTestTweet(tweetId, userId, username, content, timestamp,
                                   likeCount, retweetCount, replyCount, isCelebrity, celebrityCategory);

        ObjectMapper mapper = new ObjectMapper();
        String s = mapper.writeValueAsString(tweet);

        String expected = """
{"tweet_id":"%s","user_id":"%s","username":"%s","content":"%s","timestamp":%d,"hashtags":["#bigdata","#hadoop"],"mentions":["@apache_spark"],"retweet_count":%d,"like_count":%d,"reply_count":%d,"is_celebrity":%s,"celebrity_category":"%s"}""".formatted(
            tweetId, userId, username, content, timestamp, retweetCount, likeCount, replyCount, isCelebrity, celebrityCategory);

        assertEquals(expected, s);

        // Test deserialization
        Tweet deserializedTweet = mapper.readValue(s, Tweet.class);
        assertEquals("tech", deserializedTweet.getCelebrityCategory());
        assertEquals("tweet_123", deserializedTweet.getTweetId());
        assertEquals(42, deserializedTweet.getLikeCount());
        assertTrue(deserializedTweet.isCelebrity());
    }

    @Test
    void testValidTweet() {
        var tweet = new Tweet();
        tweet.setTweetId("tweet_123");
        tweet.setUserId("user_456");
        tweet.setUsername("john_doe");
        tweet.setContent("Hello #bigdata world!");
        tweet.setTimestamp(1756259147L);
        tweet.setHashtags(List.of("#bigdata", "#hadoop"));
        tweet.setMentions(List.of("@apache_spark"));
        tweet.setCelebrityCategory("tech");
        tweet.setLikeCount(42);
        tweet.setRetweetCount(15);
        tweet.setReplyCount(3);
        tweet.setCelebrity(true);

        // Should be valid
        assertTrue(tweet.isValid());
        assertDoesNotThrow(() -> tweet.validateOrThrow());
    }

    @Test
    void testInvalidBlankFields() {
        var tweet = new Tweet();
        tweet.setTweetId(""); // Invalid: blank
        tweet.setUserId("user_456");
        tweet.setUsername(""); // Invalid: blank
        tweet.setContent(""); // Invalid: blank
        tweet.setTimestamp(1756259147L);
        tweet.setHashtags(List.of());
        tweet.setMentions(List.of());
        tweet.setCelebrityCategory(""); // Invalid: blank

        // Should be invalid
        assertFalse(tweet.isValid());

        var errors = tweet.getValidationErrors();
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Tweet ID cannot be blank")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Username cannot be blank")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Content cannot be blank")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Celebrity category cannot be blank")));
    }

    @Test
    void testContentTooLong() {
        var tweet = new Tweet();
        tweet.setTweetId("tweet_123");
        tweet.setUserId("user_456");
        tweet.setUsername("john_doe");
        tweet.setContent("A".repeat(281)); // Invalid: too long (>280 chars)
        tweet.setTimestamp(1756259147L);
        tweet.setHashtags(List.of());
        tweet.setMentions(List.of());
        tweet.setCelebrityCategory("tech");

        assertFalse(tweet.isValid());

        var errors = tweet.getValidationErrors();
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("cannot exceed 280 characters")));
    }

    @Test
    void testNegativeCounts() {
        var tweet = new Tweet();
        tweet.setTweetId("tweet_123");
        tweet.setUserId("user_456");
        tweet.setUsername("john_doe");
        tweet.setContent("Hello world!");
        tweet.setTimestamp(1756259147L);
        tweet.setHashtags(List.of());
        tweet.setMentions(List.of());
        tweet.setCelebrityCategory("tech");
        tweet.setLikeCount(-1); // Invalid: negative
        tweet.setRetweetCount(-5); // Invalid: negative
        tweet.setReplyCount(-2); // Invalid: negative

        assertFalse(tweet.isValid());

        var errors = tweet.getValidationErrors();
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Like count cannot be negative")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Retweet count cannot be negative")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Reply count cannot be negative")));
    }

    @Test
    void testInvalidCelebrityCategory() {
        var tweet = new Tweet();
        tweet.setTweetId("tweet_123");
        tweet.setUserId("user_456");
        tweet.setUsername("john_doe");
        tweet.setContent("Hello world!");
        tweet.setTimestamp(1756259147L);
        tweet.setHashtags(List.of());
        tweet.setMentions(List.of());
        tweet.setCelebrityCategory("invalid_category"); // Invalid: not in allowed values

        assertFalse(tweet.isValid());

        var errors = tweet.getValidationErrors();
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Celebrity category must be one of")));
    }

    @Test
    void testTooManyHashtagsAndMentions() {
        var tweet = new Tweet();
        tweet.setTweetId("tweet_123");
        tweet.setUserId("user_456");
        tweet.setUsername("john_doe");
        tweet.setContent("Hello world!");
        tweet.setTimestamp(1756259147L);
        tweet.setHashtags(List.of("#1", "#2", "#3", "#4", "#5", "#6", "#7", "#8", "#9", "#10", "#11")); // 11 hashtags - too many
        tweet.setMentions(List.of("@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9", "@10", "@11")); // 11 mentions - too many
        tweet.setCelebrityCategory("tech");

        assertFalse(tweet.isValid());

        var errors = tweet.getValidationErrors();
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Cannot have more than 10 hashtags")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Cannot mention more than 10 users")));
    }

    @Test
    void testValidateOrThrowException() {
        var tweet = new Tweet();
        tweet.setTweetId(""); // Invalid
        tweet.setUserId("user_456");
        tweet.setUsername("john_doe");
        tweet.setContent("Hello world!");
        tweet.setTimestamp(1756259147L);
        tweet.setHashtags(List.of());
        tweet.setMentions(List.of());
        tweet.setCelebrityCategory("tech");

        // Should throw exception with validation message
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> tweet.validateOrThrow());
        assertTrue(exception.getMessage().contains("Tweet validation failed"));
        assertTrue(exception.getMessage().contains("Tweet ID cannot be blank"));
    }


    // Helper method to create test tweets with variables
    private Tweet createTestTweet(String tweetId, String userId, String username,
                                  String content, long timestamp, int likeCount,
                                  int retweetCount, int replyCount, boolean isCelebrity,
                                  String celebrityCategory) {
        var tweet = new Tweet();
        tweet.setTweetId(tweetId);
        tweet.setUserId(userId);
        tweet.setUsername(username);
        tweet.setContent(content);
        tweet.setTimestamp(timestamp);
        tweet.setHashtags(List.of("#bigdata", "#hadoop"));
        tweet.setMentions(List.of("@apache_spark"));
        tweet.setLikeCount(likeCount);
        tweet.setRetweetCount(retweetCount);
        tweet.setReplyCount(replyCount);
        tweet.setCelebrity(isCelebrity);
        tweet.setCelebrityCategory(celebrityCategory);
        return tweet;
    }
}
