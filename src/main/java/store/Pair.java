package store;

import java.time.Instant;

public class Pair {
    public final String value;
    public final Integer expiry;
    public final Instant setTime;

    public Pair(String value, Integer time, Instant setTime) {
        this.value = value;
        this.expiry = time;
        this.setTime = setTime;
    }
}
