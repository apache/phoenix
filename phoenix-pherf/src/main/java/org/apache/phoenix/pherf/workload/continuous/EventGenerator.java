package org.apache.phoenix.pherf.workload.continuous;

/**
 * An interface that implementers can use to generate events that can be consumed by
 * @see {@link com.lmax.disruptor.WorkHandler} which provide event handling functionality for
 * a given event.
 *
 * @param <T>
 */
public interface EventGenerator<T> {
    T next();
}
