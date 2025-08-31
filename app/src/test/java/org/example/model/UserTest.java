package org.example.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserTest {

    private User createTestUser(String userId, String username, String displayName,
                               Long followerCount, Long followingCount, int tweetCount,
                               boolean verified, String bio, String category) {
        var user = new User();
        user.setUserId(userId);
        user.setUsername(username);
        user.setDisplayName(displayName);
        user.setFollowerCount(followerCount);
        user.setFollowingCount(followingCount);
        user.setTweetCount(tweetCount);
        user.setVerified(verified);
        user.setBio(bio);
        user.setCategory(category);
        return user;
    }

    @Test
    void serde() throws IOException {
        String userId = "user_123";
        String username = "john_doe";
        String displayName = "John Doe";
        Long followerCount = 150000L; // Celebrity level
        Long followingCount = 500L;
        int tweetCount = 2500;
        boolean verified = true;
        String bio = "Tech entrepreneur";
        String category = "tech";

        var user = createTestUser(userId, username, displayName, followerCount,
                                 followingCount, tweetCount, verified, bio, category);

        ObjectMapper mapper = new ObjectMapper();
        String s = mapper.writeValueAsString(user);

        // Note: is_celebrity is computed based on follower_count >= 100000
        String expected = """
{"user_id":"%s","username":"%s","display_name":"%s","follower_count":%d,"following_count":%d,"tweet_count":%d,"verified":%s,"bio":"%s","category":"%s","is_celebrity":%s}""".formatted(
            userId, username, displayName, followerCount, followingCount,
            tweetCount, verified, bio, category, true); // true because 150K > 100K

        assertEquals(expected, s);

        User deserializedUser = mapper.readValue(s, User.class);
        assertEquals("tech", deserializedUser.getCategory());
        assertEquals("user_123", deserializedUser.getUserId());
        assertEquals(150000L, deserializedUser.getFollowerCount());
        assertTrue(deserializedUser.isCelebrity()); // 150K followers = celebrity
    }

    @Test
    void testCelebrityThreshold() {
        // Test regular user (below threshold)
        var regularUser = createTestUser("user_1", "john", "John", 50000L,
                                        100L, 100, false, "Regular user", "other");
        assertFalse(regularUser.isCelebrity());

        // Test celebrity (above threshold)
        var celebrity = createTestUser("user_2", "celebrity", "Celebrity", 200000L,
                                      1000L, 5000, true, "Famous person", "entertainment");
        assertTrue(celebrity.isCelebrity());

        // Test exactly at threshold
        var atThreshold = createTestUser("user_3", "threshold", "Threshold", 100000L,
                                        500L, 1000, false, "At threshold", "tech");
        assertTrue(atThreshold.isCelebrity()); // >= 100K is celebrity
    }

    @Test
    void testValidUser() {
        var user = createTestUser("user_123", "john_doe", "John Doe", 50000L,
                                 500L, 1500, true, "Software developer", "tech");

        assertTrue(user.isValid());
        assertDoesNotThrow(() -> user.validateOrThrow());
    }

    @Test
    void testInvalidBlankFields() {
        var user = new User();
        user.setUserId(""); // Invalid: blank
        user.setUsername(""); // Invalid: blank
        user.setDisplayName(""); // Invalid: blank
        user.setFollowerCount(1000L);
        user.setFollowingCount(100L);

        assertFalse(user.isValid());

        var errors = user.getValidationErrors();
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("User ID cannot be blank")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Username cannot be blank")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Display name cannot be blank")));
    }

    @Test
    void testInvalidCategory() {
        var user = createTestUser("user_123", "john_doe", "John Doe", 50000L,
                                 500L, 1500, true, "Bio", "invalid_category");

        assertFalse(user.isValid());

        var errors = user.getValidationErrors();
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Category must be one of")));
    }

    @Test
    void testNegativeCounts() {
        var user = new User();
        user.setUserId("user_123");
        user.setUsername("john_doe");
        user.setDisplayName("John Doe");
        user.setFollowerCount(-1L); // Invalid: negative
        user.setFollowingCount(-5L); // Invalid: negative
        user.setTweetCount(-10); // Invalid: negative

        assertFalse(user.isValid());

        var errors = user.getValidationErrors();
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Follower count cannot be negative")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Following count cannot be negative")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Tweet count cannot be negative")));
    }

    @Test
    void testGetCelebrityCategory() {
        // Celebrity with specific category
        var techCelebrity = createTestUser("user_1", "tech_guru", "Tech Guru", 200000L,
                                          1000L, 5000, true, "Tech leader", "tech");
        assertEquals("tech", techCelebrity.getCelebrityCategory());

        // Celebrity with null category should default to "other"
        var celebrityNullCategory = createTestUser("user_2", "celeb", "Celebrity", 150000L,
                                                  800L, 3000, true, "Famous", null);
        assertEquals("other", celebrityNullCategory.getCelebrityCategory());

        // Regular user should return "other"
        var regularUser = createTestUser("user_3", "regular", "Regular", 5000L,
                                        100L, 50, false, "Normal user", "tech");
        assertEquals("other", regularUser.getCelebrityCategory());
    }
}
