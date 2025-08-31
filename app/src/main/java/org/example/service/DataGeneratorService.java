package org.example.service;

import lombok.NonNull;
import org.example.model.Tweet;

import java.util.List;

public interface DataGeneratorService {
    List<Tweet> generateTweets(int count);
}


