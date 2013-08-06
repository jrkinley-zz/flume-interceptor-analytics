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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.analytics.twitter.HashtagTopNInterceptor;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.TopNInterceptor;
import org.apache.flume.tools.Counter;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TestHashtagTopN {
  private static final Logger LOG = Logger.getLogger(TestHashtagTopN.class);
  private static final String TAG_HADOOP = "#Hadoop";
  private static final String TAG_BIGDATA = "#BigData";
  private static final String TAG_CLOUDERA = "#Cloudera";
  private Map<String, String> headers = new HashMap<String, String>();

  /**
   * Test {@link HashtagTopNInterceptor}
   * @throws InterruptedException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  @Test
  public void testHashtagTopN() throws InterruptedException, InstantiationException,
      IllegalAccessException {

    Event event;
    Interceptor.Builder builder = HashtagTopNInterceptor.Builder.class.newInstance();

    Context ctx = new Context();
    ctx.put(TopNInterceptor.TOP_N, "5");

    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    for (int i = 0; i < 20; i++) {
      headers.put(TAG_HADOOP, Integer.valueOf(i * 10).toString());
      headers.put(TAG_BIGDATA, Integer.valueOf(i * 100).toString());
      headers.put(TAG_CLOUDERA, Integer.valueOf(i * 1000).toString());
      event = EventBuilder.withBody(new byte[0], headers);
      interceptor.intercept(event);
      Thread.sleep(1000);
    }

    List<Counter> topN = ((HashtagTopNInterceptor) interceptor).getTopN();
    Assert.assertEquals(TAG_CLOUDERA, topN.get(0).getItem());
    Assert.assertEquals(19000, topN.get(0).getCount());
    Assert.assertEquals(TAG_HADOOP, topN.get(2).getItem());
    Assert.assertEquals(190, topN.get(2).getCount());

    if (LOG.isDebugEnabled()) {
      Event e = ((HashtagTopNInterceptor) interceptor).getStatsEvents().get(0);
      LOG.debug(new String(e.getBody()));
    }
  }
}
