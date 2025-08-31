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

import java.util.Set;

@Data
@NoArgsConstructor
public class Celebrity {
    @NonNull
    @NotBlank(message = "Celebrity ID cannot be blank")
    @JsonProperty("celebrity_id")
    private String celebrityId;

    @NonNull
    @NotBlank(message = "Name cannot be blank")
    @Size(min = 1, max = 100, message = "Name must be between 1 and 100 characters")
    @JsonProperty("name")
    private String name;

    @NonNull
    @NotBlank(message = "Username cannot be blank")
    @Size(min = 1, max = 50, message = "Username must be between 1 and 50 characters")
    @JsonProperty("username")
    private String username;

    @NonNull
    @NotBlank(message = "Category cannot be blank")
    @Pattern(regexp = "^(sports|entertainment|politics|tech|business|other)$", 
             message = "Category must be one of: sports, entertainment, politics, tech, business, other")
    @JsonProperty("category")
    private String category;

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

    @JsonIgnore
    public boolean isValid() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<Celebrity>> violations = validator.validate(this);
        return violations.isEmpty();
    }

    @JsonIgnore
    public Set<ConstraintViolation<Celebrity>> getValidationErrors() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        return validator.validate(this);
    }

    @JsonIgnore
    public void validateOrThrow() throws IllegalArgumentException {
        Set<ConstraintViolation<Celebrity>> violations = getValidationErrors();
        if (!violations.isEmpty()) {
            StringBuilder sb = new StringBuilder("Celebrity validation failed: ");
            for (ConstraintViolation<Celebrity> violation : violations) {
                sb.append(violation.getMessage()).append("; ");
            }
            throw new IllegalArgumentException(sb.toString());
        }
    }
}