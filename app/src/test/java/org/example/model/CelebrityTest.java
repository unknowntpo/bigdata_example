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

class CelebrityTest {

    private Celebrity createTestCelebrity(String celebrityId, String name, String username,
                                        String category, Long followerCount, Long followingCount,
                                        int tweetCount, boolean verified, String bio) {
        var celebrity = new Celebrity();
        celebrity.setCelebrityId(celebrityId);
        celebrity.setName(name);
        celebrity.setUsername(username);
        celebrity.setCategory(category);
        celebrity.setFollowerCount(followerCount);
        celebrity.setFollowingCount(followingCount);
        celebrity.setTweetCount(tweetCount);
        celebrity.setVerified(verified);
        celebrity.setBio(bio);
        return celebrity;
    }

    @Test
    void serde() throws IOException {
        String celebrityId = "celeb_123";
        String name = "John Celebrity";
        String username = "john_celeb";
        String category = "tech";
        Long followerCount = 1000000L;
        Long followingCount = 500L;
        int tweetCount = 2500;
        boolean verified = true;
        String bio = "Tech entrepreneur and innovator";

        var celebrity = createTestCelebrity(celebrityId, name, username, category,
                                          followerCount, followingCount, tweetCount, verified, bio);

        ObjectMapper mapper = new ObjectMapper();
        String s = mapper.writeValueAsString(celebrity);

        String expected = """
{"celebrity_id":"%s","name":"%s","username":"%s","category":"%s","follower_count":%d,"following_count":%d,"tweet_count":%d,"verified":%s,"bio":"%s"}""".formatted(
            celebrityId, name, username, category, followerCount, followingCount, tweetCount, verified, bio);

        assertEquals(expected, s);

        Celebrity deserializedCelebrity = mapper.readValue(s, Celebrity.class);
        assertEquals("tech", deserializedCelebrity.getCategory());
        assertEquals("celeb_123", deserializedCelebrity.getCelebrityId());
        assertEquals(1000000L, deserializedCelebrity.getFollowerCount());
        assertTrue(deserializedCelebrity.isVerified());
    }

    @Test
    void testValidCelebrity() {
        var celebrity = createTestCelebrity("celeb_123", "John Celebrity", "john_celeb",
                                          "tech", 1000000L, 500L, 2500, true,
                                          "Tech entrepreneur and innovator");

        assertTrue(celebrity.isValid());
        assertDoesNotThrow(() -> celebrity.validateOrThrow());
    }

    @Test
    void testInvalidBlankFields() {
        var celebrity = new Celebrity();
        celebrity.setCelebrityId(""); // Invalid: blank
        celebrity.setName(""); // Invalid: blank
        celebrity.setUsername(""); // Invalid: blank
        celebrity.setCategory(""); // Invalid: blank
        celebrity.setFollowerCount(1000L);
        celebrity.setFollowingCount(100L);

        assertFalse(celebrity.isValid());

        var errors = celebrity.getValidationErrors();
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Celebrity ID cannot be blank")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Name cannot be blank")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Username cannot be blank")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Category cannot be blank")));
    }

    @Test
    void testInvalidCategory() {
        var celebrity = createTestCelebrity("celeb_123", "John Celebrity", "john_celeb",
                                          "invalid_category", 1000000L, 500L, 2500, true, "Bio");

        assertFalse(celebrity.isValid());

        var errors = celebrity.getValidationErrors();
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Category must be one of")));
    }

    @Test
    void testNegativeCounts() {
        var celebrity = new Celebrity();
        celebrity.setCelebrityId("celeb_123");
        celebrity.setName("John Celebrity");
        celebrity.setUsername("john_celeb");
        celebrity.setCategory("tech");
        celebrity.setFollowerCount(-1L); // Invalid: negative
        celebrity.setFollowingCount(-5L); // Invalid: negative
        celebrity.setTweetCount(-10); // Invalid: negative

        assertFalse(celebrity.isValid());

        var errors = celebrity.getValidationErrors();
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Follower count cannot be negative")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Following count cannot be negative")));
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Tweet count cannot be negative")));
    }

    @Test
    void testBioTooLong() {
        var celebrity = createTestCelebrity("celeb_123", "John Celebrity", "john_celeb",
                                          "tech", 1000000L, 500L, 2500, true, "A".repeat(501)); // Too long

        assertFalse(celebrity.isValid());

        var errors = celebrity.getValidationErrors();
        assertTrue(errors.stream().anyMatch(v -> v.getMessage().contains("Bio cannot exceed 500 characters")));
    }

    @Test
    void testValidateOrThrowException() {
        var celebrity = new Celebrity();
        celebrity.setCelebrityId(""); // Invalid
        celebrity.setName("John Celebrity");
        celebrity.setUsername("john_celeb");
        celebrity.setCategory("tech");
        celebrity.setFollowerCount(1000000L);
        celebrity.setFollowingCount(500L);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> celebrity.validateOrThrow());
        assertTrue(exception.getMessage().contains("Celebrity validation failed"));
        assertTrue(exception.getMessage().contains("Celebrity ID cannot be blank"));
    }
}
