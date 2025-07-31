package store;

import java.time.Duration;
import java.time.Instant;

public class ValueWithExpiry {
    private final String value;
    private final long expiryMillis;
    private final Instant createdAt;

    public ValueWithExpiry(String value, Integer time, Instant setTime) {
        this.value = value;
        this.expiryMillis = time;
        this.createdAt = setTime;
    }

    public boolean isExpiryPresent(){
        return expiryMillis != -1;
    }

    public boolean isExpired(){
        Instant now = Instant.now();
        long elapsed = Duration.between(createdAt, now).toMillis();

        return elapsed > expiryMillis;
    }

    public String getValue(){
        return this.value;
    }
}
