package org.apache.flume.analytics.twitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.RollingCountInterceptor;
import org.apache.flume.interceptor.TimestampInterceptor;
import org.apache.flume.tools.InterceptorRegistry;
import org.apache.flume.tools.RollingCounters;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A Flume Interceptor that counts how many times a hashtag has appered in twitter status updates in
 * a sliding window.
 * <p>
 * See {@link RollingCountInterceptor} and {@link RollingCounters}.
 */
public class HashtagRollingCountInterceptor extends RollingCountInterceptor<String> {
  private static final Logger LOG = Logger.getLogger(HashtagRollingCountInterceptor.class);
  private static final String STATUS_UPDATE_FIELDNAME = "text";
  private static final String WHITESPACE = " ";
  private static final String HASHTAG = "#";

  private final JsonFactory jsonFactory = new JsonFactory();

  public HashtagRollingCountInterceptor(int numBuckets, int windowLenSec) {
    super(numBuckets, windowLenSec);
  }

  /** {@inheritDoc} */
  public void initialize() {
    InterceptorRegistry.register(HashtagRollingCountInterceptor.class, this);
  }

  /** {@inheritDoc} */
  public void close() {
    InterceptorRegistry.deregister(this);
  }

  /**
   * Uses a JSON parser to extract the status update from a Tweet. The status update fieldname is
   * "text".
   * <p>
   * For more information see {@link https://dev.twitter.com/docs/platform-objects/tweets}
   * @param event
   * @return
   */
  private String getTweetFromEvent(Event event) {
    JsonParser jp = null;
    try {
      jp = jsonFactory.createJsonParser(event.getBody());
      while (jp.nextToken() != JsonToken.END_OBJECT) {
        String fieldname = jp.getCurrentName();
        if (STATUS_UPDATE_FIELDNAME.equals(fieldname)) {
          return jp.nextTextValue();
        }
      }
    } catch (IOException e) {
      LOG.error("Error parsing JSON", e);
    } finally {
      try {
        jp.close();
      } catch (IOException e) {
        LOG.error("Unable to close JSON parser", e);
      }
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getObjectsToCount(Event event) {
    List<String> hashtags = new ArrayList<String>();
    String tweet = getTweetFromEvent(event);
    if (tweet != null) {
      String[] words = tweet.split(WHITESPACE);
      for (String w : words) {
        if (w.startsWith(HASHTAG)) {
          hashtags.add(w);
        }
      }
    }
    return hashtags;
  }

  /** {@inheritDoc} */
  @Override
  public List<Event> getStatsEvents() {
    List<Event> events = Lists.newArrayList();
    Map<String, Long> counters = getCounters();

    for (String obj : counters.keySet()) {
      Map<String, String> headers = Maps.newHashMap();
      headers.put(obj, String.valueOf(counters.get(obj)));
      headers.put(TimestampInterceptor.Constants.TIMESTAMP,
        Long.toString(System.currentTimeMillis()));
      events.add(EventBuilder.withBody(new byte[0], headers));
    }

    return events;
  }

  /**
   * Builder which builds new instance of HashtagRollingCountInterceptor.
   */
  public static class Builder implements Interceptor.Builder {
    private int numBuckets;
    private int windowLenSec;

    @Override
    public void configure(Context context) {
      this.numBuckets = context.getInteger(NUM_BUCKETS);
      this.windowLenSec = context.getInteger(WINDOW_LEN_SEC);
    }

    @Override
    public Interceptor build() {
      return new HashtagRollingCountInterceptor(numBuckets, windowLenSec);
    }
  }
}
