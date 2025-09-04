package org.example.service;

import org.example.model.User;
import org.example.model.Event;
import org.example.model.Tweet;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class DataGenerator {
    private static final Random RANDOM = new Random();

    private static final String[] CELEBRITY_CATEGORIES = {
        "sports", "entertainment", "politics", "tech", "business", "other"
    };

    private static final String[] EVENT_TYPES = {
        "like", "retweet", "reply", "mention", "follow", "unfollow", "tweet"
    };

    private static final String[] TECH_CELEBRITIES = {
        "Elon Musk", "Bill Gates", "Tim Cook", "Satya Nadella", "Jeff Bezos"
    };

    private static final String[] SPORTS_CELEBRITIES = {
        "LeBron James", "Cristiano Ronaldo", "Serena Williams", "Tom Brady", "Lionel Messi"
    };

    private static final String[] ENTERTAINMENT_CELEBRITIES = {
        "Taylor Swift", "Dwayne Johnson", "Oprah Winfrey", "Ellen DeGeneres", "Ryan Reynolds"
    };

    private static final String[] SAMPLE_HASHTAGS = {
        "#bigdata", "#hadoop", "#spark", "#kafka", "#analytics", "#ml", "#ai", "#tech", "#innovation", "#data"
    };

    private static final String[] SAMPLE_MENTIONS = {
        "@ironman", "@spiderman", "@batman", "@superman", "@wonderwoman",
        "@captain_america", "@thor", "@hulk", "@blackwidow", "@hawkeye",
        "@flash", "@aquaman", "@greenlantern", "@deadpool", "@wolverine",
        "@antman", "@wasp", "@falcon", "@scarletwitch", "@vision",
        "@warmachine", "@starlord", "@gamora", "@drax", "@rocket",
        "@groot", "@mantis", "@nebula", "@blackpanther", "@shuri",
        "@drstrange", "@wanda", "@loki", "@valkyrie", "@captainmarvel"
    };

    private static final String[] TWEET_TEMPLATES = {
        "Just discovered %s! This is going to change everything in %s #%s",
        "Working on some exciting %s projects. The future of %s looks bright! %s",
        "Big announcement coming soon about %s and %s! Stay tuned %s #%s",
        "Love seeing the progress in %s technology. %s is the way forward! %s",
        "Thoughts on the latest %s trends? %s seems to be gaining momentum %s"
    };

    public static User generateCelebrity() {
        var user = new User();
        user.setUserId("user_" + UUID.randomUUID().toString().substring(0, 8));

        String category = CELEBRITY_CATEGORIES[RANDOM.nextInt(CELEBRITY_CATEGORIES.length)];
        user.setCategory(category);

        String name = switch (category) {
            case "tech" -> TECH_CELEBRITIES[RANDOM.nextInt(TECH_CELEBRITIES.length)];
            case "sports" -> SPORTS_CELEBRITIES[RANDOM.nextInt(SPORTS_CELEBRITIES.length)];
            case "entertainment" -> ENTERTAINMENT_CELEBRITIES[RANDOM.nextInt(ENTERTAINMENT_CELEBRITIES.length)];
            default -> "Celebrity " + RANDOM.nextInt(1000);
        };

        user.setDisplayName(name);
        user.setUsername(name.toLowerCase().replace(" ", "_") + "_" + RANDOM.nextInt(100));
        user.setFollowerCount(ThreadLocalRandom.current().nextLong(100_000, 50_000_000)); // Celebrity level
        user.setFollowingCount(ThreadLocalRandom.current().nextLong(100, 10_000));
        user.setTweetCount(RANDOM.nextInt(50_000) + 1000);
        user.setVerified(RANDOM.nextDouble() > 0.3); // 70% verified
        user.setBio(String.format("%s expert and thought leader in %s", category, category));

        return user;
    }

    public static Tweet generateTweet(User user) {
        var tweet = new Tweet();
        tweet.setTweetId("tweet_" + UUID.randomUUID().toString().substring(0, 8));
        tweet.setUserId(user.getUserId());
        tweet.setUsername(user.getUsername());
        tweet.setTimestamp(Instant.now().getEpochSecond());

        // Generate realistic content based on user category
        String template = TWEET_TEMPLATES[RANDOM.nextInt(TWEET_TEMPLATES.length)];
        String category = user.getCategory() != null ? user.getCategory() : "tech";
        String content = String.format(template,
            category,
            "technology",
            category,
            randomHashtag());
        content = content.substring(0, Math.min(280, content.length()));

        tweet.setContent(content);

        // Add hashtags and mentions
        List<String> hashtags = List.of(
            SAMPLE_HASHTAGS[RANDOM.nextInt(SAMPLE_HASHTAGS.length)],
            "#" + category
        );
        tweet.setHashtags(hashtags);

        // Generate random number of mentions (1 to 3)
        int mentionCount = RANDOM.nextInt(3) + 1;
        var mentions = new HashSet<String>();

        for (int i = 0; i < mentionCount; i++) {
            mentions.add(SAMPLE_MENTIONS[RANDOM.nextInt(SAMPLE_MENTIONS.length)]);
        }
        tweet.setMentions(mentions.stream().toList());

        // Generate engagement metrics (higher for celebrities)
        boolean isCelebrity = user.isCelebrity();
        int multiplier = isCelebrity ? 10 : 1;
        tweet.setLikeCount(RANDOM.nextInt(1000 * multiplier));
        tweet.setRetweetCount(RANDOM.nextInt(500 * multiplier));
        tweet.setReplyCount(RANDOM.nextInt(100 * multiplier));

        // Set celebrity fields
        tweet.setCelebrity(isCelebrity);
        tweet.setCelebrityCategory(category);

        return tweet;
    }

    public static Event generateEvent(String userId, String targetId, boolean celebrityInvolved, String celebrityId) {
        var event = new Event();
        event.setEventId("event_" + UUID.randomUUID().toString().substring(0, 8));
        event.setEventType(EVENT_TYPES[RANDOM.nextInt(EVENT_TYPES.length)]);
        event.setUserId(userId);
        event.setTargetId(targetId);
        event.setTimestamp(Instant.now().getEpochSecond());
        event.setCelebrityInvolved(celebrityInvolved);
        event.setCelebrityId(celebrityId);

        // Generate metadata
        String metadata = String.format("{\"location\":\"%s\",\"device\":\"%s\"}",
            randomLocation(), randomDevice());
        event.setMetadata(metadata);

        return event;
    }

    public static User generateRegularUser() {
        var user = new User();
        user.setUserId("user_" + UUID.randomUUID().toString().substring(0, 8));
        user.setUsername("user_" + RANDOM.nextInt(100000));
        user.setDisplayName("User " + RANDOM.nextInt(10000));
        user.setFollowerCount(ThreadLocalRandom.current().nextLong(10, 50_000)); // Below celebrity threshold
        user.setFollowingCount(ThreadLocalRandom.current().nextLong(50, 2000));
        user.setTweetCount(RANDOM.nextInt(1000) + 10);
        user.setVerified(RANDOM.nextDouble() > 0.95); // 5% verified
        user.setBio("Just a regular user sharing thoughts");
        user.setCategory("other");

        return user;
    }

    public static Tweet generateRegularTweet() {
        User regularUser = generateRegularUser();
        return generateTweet(regularUser); // Reuse the main tweet generation logic
    }

    private static String randomHashtag() {
        return SAMPLE_HASHTAGS[RANDOM.nextInt(SAMPLE_HASHTAGS.length)];
    }

    private static String randomLocation() {
        String[] locations = {"US", "UK", "CA", "DE", "FR", "JP", "AU", "BR"};
        return locations[RANDOM.nextInt(locations.length)];
    }

    private static String randomDevice() {
        String[] devices = {"mobile", "desktop", "tablet"};
        return devices[RANDOM.nextInt(devices.length)];
    }
}
