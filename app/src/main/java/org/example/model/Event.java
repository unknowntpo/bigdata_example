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
public class Event {
    @NonNull
    @NotBlank(message = "Event ID cannot be blank")
    @JsonProperty("event_id")
    private String eventId;

    @NonNull
    @NotBlank(message = "Event type cannot be blank")
    @Pattern(regexp = "^(like|retweet|reply|mention|follow|unfollow|tweet)$", 
             message = "Event type must be one of: like, retweet, reply, mention, follow, unfollow, tweet")
    @JsonProperty("event_type")
    private String eventType;

    @NonNull
    @NotBlank(message = "User ID cannot be blank")
    @JsonProperty("user_id")
    private String userId;

    @NonNull
    @NotBlank(message = "Target ID cannot be blank")
    @JsonProperty("target_id")
    private String targetId; // tweet_id for tweet events, user_id for follow events

    @NonNull
    @NotNull(message = "Timestamp cannot be null")
    @Positive(message = "Timestamp must be positive")
    @JsonProperty("timestamp")
    private Long timestamp;

    @JsonProperty("metadata")
    @Size(max = 1000, message = "Metadata cannot exceed 1000 characters")
    private String metadata; // JSON string for additional event data

    @JsonProperty("is_celebrity_involved")
    private boolean isCelebrityInvolved;

    @JsonProperty("celebrity_id")
    private String celebrityId; // null if no celebrity involved

    @JsonIgnore
    public boolean isValid() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<Event>> violations = validator.validate(this);
        return violations.isEmpty();
    }

    @JsonIgnore
    public Set<ConstraintViolation<Event>> getValidationErrors() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        return validator.validate(this);
    }

    @JsonIgnore
    public void validateOrThrow() throws IllegalArgumentException {
        Set<ConstraintViolation<Event>> violations = getValidationErrors();
        if (!violations.isEmpty()) {
            StringBuilder sb = new StringBuilder("Event validation failed: ");
            for (ConstraintViolation<Event> violation : violations) {
                sb.append(violation.getMessage()).append("; ");
            }
            throw new IllegalArgumentException(sb.toString());
        }
    }
}