package org.example.service;

import lombok.NonNull;
import org.example.model.Tweet;

import java.util.ArrayList;
import java.util.List;

public class HadoopDataGeneratorService implements DataGeneratorService {
    @Override
    public @NonNull List<Tweet> generateTweets(int count) {
        List<Tweet> tweets = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            Tweet tweet = new Tweet();
            tweet.setTweetId("tweet_" + i);
            tweet.setUserId("user_" + i);
            tweet.setUsername("testuser" + i);
            tweet.setContent("Sample tweet content " + i);
            tweet.setTimestamp(System.currentTimeMillis());
            tweet.setHashtags(List.of("hashtag" + i));
            tweet.setMentions(List.of());
            tweet.setRetweetCount(0);
            tweet.setLikeCount(0);
            tweet.setReplyCount(0);
            tweet.setCelebrity(false);
            tweet.setCelebrityCategory("other");
            
            tweets.add(tweet);
        }
        
        return tweets;
    }
}
