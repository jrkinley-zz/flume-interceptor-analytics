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
package org.apache.flume.analytics.twitter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map.Entry;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.TimestampInterceptor;
import org.apache.flume.interceptor.TopNInterceptor;
import org.apache.flume.tools.Counter;
import org.apache.flume.tools.InterceptorRegistry;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import com.google.common.collect.Lists;

/**
 * A Flume Interceptor that ranks hashtags by the number of times they have appeared in twitter
 * status updates.
 * <p>
 * See {@link TopNInterceptor}.
 */
public class HashtagTopNInterceptor extends TopNInterceptor {
  private static final Logger LOG = Logger.getLogger(HashtagTopNInterceptor.class);
  private static final String HASHTAG = "#";
  public static final String TOP_N = "topN";
  private static final String TS_HEADER = "topN_TS";

  private final JsonFactory jsonFactory = new JsonFactory();

  public HashtagTopNInterceptor(int topN) throws IOException {
    super(topN);
  }

  /** {@inheritDoc} */
  @Override
  public void initialize() {
    InterceptorRegistry.register(HashtagTopNInterceptor.class, this);
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    InterceptorRegistry.deregister(this);
  }

  /** {@inheritDoc} */
  @Override
  public List<Counter> getCounters(Event event) {
    List<Counter> c = Lists.newArrayList();
    for (Entry<String, String> e : event.getHeaders().entrySet()) {
      if (e.getKey().startsWith(HASHTAG)) {
        try {
          c.add(new Counter(e.getKey(), Long.parseLong(e.getValue())));
        } catch (NumberFormatException ex) {
          LOG.error(ex);
        }
      }
    }
    return c;
  }

  /** {@inheritDoc} */
  @Override
  public List<Event> getStatsEvents() {
    List<Counter> topN = getTopN();
    StringWriter sw = new StringWriter();
    JsonGenerator gen = null;

    try {
      gen = jsonFactory.createJsonGenerator(sw);
      gen.writeStartObject();
      gen.writeNumberField(TS_HEADER, System.currentTimeMillis());
      gen.writeArrayFieldStart(TOP_N);
      for (Counter counter : topN) {
        gen.writeStartObject();
        gen.writeNumberField(counter.getItem(), counter.getCount());
        gen.writeEndObject();
      }
      gen.writeEndArray();
      gen.writeEndObject();
    } catch (IOException e) {
      LOG.error("Error writing JSON", e);
    } finally {
      try {
        gen.close();
      } catch (IOException e) {
        LOG.error("Unable to close JsonGenerator", e);
      }
    }

    Event e = EventBuilder.withBody(sw.toString().getBytes());
    e.getHeaders().put(TimestampInterceptor.Constants.TIMESTAMP,
      Long.toString(System.currentTimeMillis()));
    return Lists.newArrayList(e);
  }

  /**
   * Builder which builds new instance of HashtagTopNInterceptor.
   */
  public static class Builder implements Interceptor.Builder {
    private int topN;

    @Override
    public void configure(Context context) {
      this.topN = context.getInteger(TOP_N);
    }

    @Override
    public Interceptor build() {
      try {
        return new HashtagTopNInterceptor(topN);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
