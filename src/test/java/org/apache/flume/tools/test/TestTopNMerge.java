/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.tools.test;

import java.io.IOException;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.analytics.twitter.TopNMergeInterceptor;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.TopNInterceptor;
import org.apache.flume.tools.Counter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test {@link TopNMergeInterceptor}
 */
public class TestTopNMerge {
  private Interceptor interceptor = null;
  private Event event1 = null;
  private Event event2 = null;

  private static final String TOPN_1 = "{\"topN_TS\":1375715289307,\"topN\":[{\"#one\":20},"
      + "{\"#two\":18},{\"#three\":16},{\"#four\":14},{\"#five\":12},{\"#six\":10},{\"#seven\":8},"
      + "{\"#eight\":6},{\"#nine\":4},{\"#ten\":2}]}";

  private static final String TOPN_2 = "{\"topN_TS\":1375715289307,\"topN\":[{\"#aaa\":19},"
      + "{\"#bbb\":17},{\"#ccc\":15},{\"#ddd\":13},{\"#eee\":11},{\"#fff\":9},{\"#ggg\":7},"
      + "{\"#hhh\":5},{\"#iii\":3},{\"#jjj\":1}]}";

  @Before
  public void setup() throws InstantiationException, IllegalAccessException {
    Context ctx = new Context();
    ctx.put(TopNInterceptor.TOP_N, "10");

    Interceptor.Builder builder = TopNMergeInterceptor.Builder.class.newInstance();
    builder.configure(ctx);

    interceptor = builder.build();

    event1 = EventBuilder.withBody(TOPN_1.getBytes());
    event2 = EventBuilder.withBody(TOPN_2.getBytes());
  }

  @Test
  public void testTopNMerge() {
    interceptor.intercept(event1);
    interceptor.intercept(event2);

    List<Event> result = ((TopNMergeInterceptor) interceptor).getStatsEvents();
    List<Counter> topN = ((TopNMergeInterceptor) interceptor).getCounters(result.get(0));

    Assert.assertEquals("#one", topN.get(0).getItem());
    Assert.assertEquals(20, topN.get(0).getCount());

    Assert.assertEquals("#aaa", topN.get(1).getItem());
    Assert.assertEquals(19, topN.get(1).getCount());

    Assert.assertEquals("#eee", topN.get(9).getItem());
    Assert.assertEquals(11, topN.get(9).getCount());
  }

  @Test
  public void testTopNParser() throws IOException {
    List<Counter> topN = ((TopNMergeInterceptor) interceptor).getCounters(event1);

    Assert.assertEquals("#one", topN.get(0).getItem());
    Assert.assertEquals(20, topN.get(0).getCount());

    Assert.assertEquals("#ten", topN.get(9).getItem());
    Assert.assertEquals(2, topN.get(9).getCount());
  }
}
