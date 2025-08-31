package org.example.service;

import org.example.model.Tweet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HadoopDataGeneratorServiceTest {

    @Test
    void testGenerateTweets() {
        var service = new HadoopDataGeneratorService();
        List<Tweet> tweets = service.generateTweets(3);
        assertEquals(3, tweets.size());
    }
}
