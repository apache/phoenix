package org.apache.phoenix.util;

import java.util.concurrent.atomic.AtomicLong;


public class TestClock extends EnvironmentEdge {

    private final AtomicLong currentTime;
    private final long initialTime;
    private final long delta;
    private boolean advance;

    public TestClock(long initialTime, long delta, boolean advance) {
        this.currentTime = new AtomicLong(initialTime);
        this.initialTime = initialTime;
        this.delta = delta;
        this.advance = advance;
    }

    public TestClock(long initialTime) {
        this(initialTime, 0, false);
    }

    @Override
    public long currentTime() {
        long actualTime = currentTime.get();
        if (advance) {
            currentTime.getAndSet(currentTime.get() + delta);
        }
        return actualTime;
    }

    public long initialTime() {
        return initialTime;
    }

    public void shouldAdvance(boolean advance) {
        this.advance = advance;
    }

    public void setCurrentTime(long currentTime) {
        this.currentTime.set(currentTime);
    }

    public void advanceTime(long delta) {
        this.currentTime.set(this.currentTime.get() + delta);
    }
}
