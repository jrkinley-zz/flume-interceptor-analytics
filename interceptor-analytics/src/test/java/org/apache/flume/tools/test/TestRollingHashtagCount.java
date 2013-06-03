package org.apache.flume.tools.test;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.analytics.twitter.RollingHashtagCount;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.RollingCountInterceptor;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TestRollingHashtagCount {
  private static final Logger LOG = Logger.getLogger(TestRollingHashtagCount.class);
  private static final String TWEET =
      "{\"text\":\"Follow @ClouderaEng for technical posts, updates, and resources. "
          + "Check it out: http://j.mp/122iEeW #Hadoop #BigData\""
          + ",\"retweeted\":false,\"favorited\":false,\"retweet_count\":0,\"favorite_count\":0}";

  @Test
  public void testRollingHashtagCount() throws ClassNotFoundException, InstantiationException,
      IllegalAccessException, InterruptedException {

    Event intercepted;
    Interceptor.Builder builder = RollingHashtagCount.Builder.class.newInstance();

    Context ctx = new Context();
    ctx.put(RollingCountInterceptor.NUM_BUCKETS, "5");
    ctx.put(RollingCountInterceptor.WINDOW_LEN_SEC, "10");

    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    Event event = EventBuilder.withBody(TWEET, Charsets.UTF_8);

    // Load up the first bucket (first 2 seconds)
    for (int i = 0; i < 50; i++) {
      intercepted = interceptor.intercept(event);
      checkEventCounters(intercepted, i + 1);
    }

    // Wait for the window to pass (11 seconds) and the reaper to wipe the expired buckets
    Thread.sleep(11000);

    // Check the counter has been reset
    intercepted = interceptor.intercept(event);
    checkEventCounters(intercepted, 1);
  }

  private void checkEventCounters(Event event, long expected) {
    Map<String, String> headers = event.getHeaders();
    for (Entry<String, String> e : headers.entrySet()) {
      Assert.assertEquals(expected, Long.parseLong(e.getValue()));
      LOG.debug("Event header: " + e.toString());
    }
  }
}
