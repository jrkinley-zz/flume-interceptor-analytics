package org.apache.flume.interceptor;

import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.tools.RollingCounters;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

/**
 * A Flume Interceptor that counts objects within an event in a sliding window. For example,
 * counting how many times a hashtag has appeared in twitter status updates in the past 30 minutes.
 * <p>
 * In this case, the hashtags are extracted from the tweet, their counts incremented, and their new
 * totals added to the event's header. See {@link RollingCounters}.
 * <p>
 * For example, if the sliding window length is set to 30 minutes and the following tweet was seen
 * 100 times in the past 60 minutes, but only 50 times in the past 30 minutes then the emitted event
 * would contain the following:
 * 
 * <pre>
 * {@code
 * Event header:
 * Hadoop, 50
 * BigData, 50
 * 
 * Event body:
 * "text":"Follow @ClouderaEng for technical posts, updates, and resources. Check it out: http://j.mp/122iEeW #Hadoop #BigData"
 * }
 * </pre>
 * @param <T>
 */
public abstract class RollingCountInterceptor<T> implements Interceptor {
  private static final Logger LOG = Logger.getLogger(RollingCountInterceptor.class);
  public static final String NUM_BUCKETS = "numBuckets";
  public static final String WINDOW_LEN_SEC = "windowLenSec";
  private final RollingCounters<T> counters;

  public RollingCountInterceptor(int numBuckets, int windowLenSec) {
    this.counters = new RollingCounters<T>(numBuckets, windowLenSec);
    LOG.info(String.format("Initialising RollingCountInterceptor: buckets=%d, "
        + "window length=%d sec", numBuckets, windowLenSec));
  }

  /** {@inheritDoc} */
  public void close() {
    // no-op
  }

  /** {@inheritDoc} */
  public void initialize() {
    // no-op
  }

  /** {@inheritDoc} */
  public Event intercept(Event event) {
    List<T> objects = getObjectsToCount(event);
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Identified %d objects to count from event: %s", objects.size(),
        new String(event.getBody())));
    }

    if (!objects.isEmpty()) {
      Map<String, String> headers = event.getHeaders();
      for (T obj : objects) {
        long total = counters.incrementCount(obj);
        headers.put(obj.toString(), Long.toString(total));
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Added counter to event header: %s=%d", obj.toString(), total));
        }
      }
    }
    return event;
  }

  /** {@inheritDoc} */
  public List<Event> intercept(List<Event> events) {
    List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
    for (Event e : events) {
      intercepted.add(intercept(e));
    }
    return intercepted;
  }

  /**
   * Extracts the objects to count from the given event
   * @param event
   * @return The objects to count
   */
  public abstract List<T> getObjectsToCount(Event event);
}
