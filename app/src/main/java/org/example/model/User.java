package org.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.*;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.Set;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class User {
    private static final long CELEBRITY_THRESHOLD = 100_000; // 100K followers = celebrity
    
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
    @NotBlank(message = "Display name cannot be blank")
    @Size(min = 1, max = 100, message = "Display name must be between 1 and 100 characters")
    @JsonProperty("display_name")
    private String displayName;

    @NonNull
    @NotNull(message = "Follower count cannot be null")
    @Min(value = 0, message = "Follower count cannot be negative")
    @JsonProperty("follower_count")
    private Long followerCount;

    @NonNull
    @NotNull(message = "Following count cannot be null")
    @Min(value = 0, message = "Following count cannot be negative")
    @JsonProperty("following_count")
    private Long followingCount;

    @Min(value = 0, message = "Tweet count cannot be negative")
    @JsonProperty("tweet_count")
    private int tweetCount;

    @JsonProperty("verified")
    private boolean verified;

    @JsonProperty("bio")
    @Size(max = 500, message = "Bio cannot exceed 500 characters")
    private String bio;

    @JsonProperty("category")
    @Pattern(regexp = "^(sports|entertainment|politics|tech|business|other)$", 
             message = "Category must be one of: sports, entertainment, politics, tech, business, other")
    private String category; // Only relevant if celebrity

    // Computed field - celebrity if follower count >= threshold
    @JsonProperty("is_celebrity")
    public boolean isCelebrity() {
        return followerCount != null && followerCount >= CELEBRITY_THRESHOLD;
    }

    // Get celebrity category - only meaningful if is celebrity
    @JsonIgnore
    public String getCelebrityCategory() {
        return isCelebrity() ? (category != null ? category : "other") : "other";
    }

    @JsonIgnore
    public boolean isValid() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<User>> violations = validator.validate(this);
        return violations.isEmpty();
    }

    @JsonIgnore
    public Set<ConstraintViolation<User>> getValidationErrors() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        return validator.validate(this);
    }

    @JsonIgnore
    public void validateOrThrow() throws IllegalArgumentException {
        Set<ConstraintViolation<User>> violations = getValidationErrors();
        if (!violations.isEmpty()) {
            StringBuilder sb = new StringBuilder("User validation failed: ");
            for (ConstraintViolation<User> violation : violations) {
                sb.append(violation.getMessage()).append("; ");
            }
            throw new IllegalArgumentException(sb.toString());
        }
    }
}