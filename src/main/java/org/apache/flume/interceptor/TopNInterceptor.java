package org.apache.flume.interceptor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.tools.Counter;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

/**
 * A Flume Interceptor that ranks objects according to their count.
 * <p>
 * It is assumed that the topN list is collected by another source, which can emit the list
 * periodically.
 */
public abstract class TopNInterceptor implements AnalyticInterceptor {
  private static final Logger LOG = Logger.getLogger(TopNInterceptor.class);
  private final List<Counter> rankings = Lists.newArrayList();
  private final int topN;

  public TopNInterceptor(int topN) throws IOException {
    this.topN = topN;
    LOG.info(String.format("Initializing TopNInterceptor: topN=%d", topN));
  }

  /**
   * Gets the {@link Counter} objects from the given {@link Event}
   * @param event
   * @return List of {@link Counter} objects
   */
  public abstract List<Counter> getCounters(Event event);

  /** {@inheritDoc} */
  @Override
  public void initialize() {
    // no-op
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    // no-op
  }

  /**
   * Gets the current index for the given {@link Counter}
   * @param c
   * @return The current index, or -1 if the given {@link Counter} does not appear in the current
   *         rankings list
   */
  private int getCurrentIndex(Counter c) {
    for (int i = 0; i < rankings.size(); i++) {
      Counter existing = rankings.get(i);
      if (existing.getItem().equals(c.getItem())) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Updates the given {@link Counter} in the rankings list, or appends it to the end of the list if
   * it doesn't exist
   * @param c
   */
  private void addToRankings(Counter c) {
    int currentIndex = getCurrentIndex(c);
    if (currentIndex == -1) {
      rankings.add(c);
    } else {
      rankings.set(currentIndex, c);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Counter: %s, current index: %d, added to rankings", c, currentIndex));
    }
  }

  /** {@inheritDoc} */
  @Override
  public Event intercept(Event event) {
    for (Counter c : getCounters(event)) {
      addToRankings(c);
    }
    return event;
  }

  /** {@inheritDoc} */
  @Override
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      for (Counter r : getCounters(event)) {
        addToRankings(r);
      }
    }
    return events;
  }

  /**
   * Computes and returns the topN
   * @return the topN
   */
  public List<Counter> getTopN() {
    Collections.sort(rankings);
    Collections.reverse(rankings);
    if (rankings.size() > topN) {
      return rankings.subList(0, topN);
    }
    return rankings;
  }
}
