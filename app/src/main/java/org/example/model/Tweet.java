package org.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.*;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.List;
import java.util.Set;

@Data
@NoArgsConstructor
public class Tweet {
    // Core tweet properties
    @NonNull
    @NotBlank(message = "Tweet ID cannot be blank")
    @JsonProperty("tweet_id")
    private String tweetId;

    @NonNull
    @NotBlank(message = "User ID cannot be blank")
    @JsonProperty("user_id")
    private String userId;

    @NonNull
    @NotBlank(message = "Username cannot be blank")
    @Size(min = 1, max = 50, message = "Username must be between 1 and 50 characters")
    @JsonProperty("username")
    private String username;

    @NonNull
    @NotBlank(message = "Content cannot be blank")
    @Size(max = 280, message = "Tweet content cannot exceed 280 characters")
    @JsonProperty("content")
    private String content;

    @NonNull
    @NotNull(message = "Timestamp cannot be null")
    @Positive(message = "Timestamp must be positive")
    @JsonProperty("timestamp")
    private Long timestamp;

    // Social features
    @NonNull
    @NotNull(message = "Hashtags list cannot be null")
    @Size(max = 10, message = "Cannot have more than 10 hashtags")
    @JsonProperty("hashtags")
    private List<String> hashtags;

    @NonNull
    @NotNull(message = "Mentions list cannot be null")
    @Size(max = 10, message = "Cannot mention more than 10 users")
    @JsonProperty("mentions")
    private List<String> mentions;

    // Metrics - primitives can't be null, but good practice to document
    @Min(value = 0, message = "Retweet count cannot be negative")
    @JsonProperty("retweet_count")
    private int retweetCount;

    @Min(value = 0, message = "Like count cannot be negative")
    @JsonProperty("like_count")
    private int likeCount;

    @Min(value = 0, message = "Reply count cannot be negative")
    @JsonProperty("reply_count")
    private int replyCount;

    // Celebrity detection fields
    @JsonProperty("is_celebrity")
    private boolean isCelebrity;

    @NonNull
    @NotBlank(message = "Celebrity category cannot be blank")
    @Pattern(regexp = "^(sports|entertainment|politics|tech|business|other)$", 
             message = "Celebrity category must be one of: sports, entertainment, politics, tech, business, other")
    @JsonProperty("celebrity_category")
    private String celebrityCategory;

    // Custom validation methods
    @JsonIgnore
    public boolean isValid() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<Tweet>> violations = validator.validate(this);
        return violations.isEmpty();
    }

    // Get validation errors
    @JsonIgnore
    public Set<ConstraintViolation<Tweet>> getValidationErrors() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        return validator.validate(this);
    }

    // Throw exception if invalid
    @JsonIgnore
    public void validateOrThrow() throws IllegalArgumentException {
        Set<ConstraintViolation<Tweet>> violations = getValidationErrors();
        if (!violations.isEmpty()) {
            StringBuilder sb = new StringBuilder("Tweet validation failed: ");
            for (ConstraintViolation<Tweet> violation : violations) {
                sb.append(violation.getMessage()).append("; ");
            }
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
