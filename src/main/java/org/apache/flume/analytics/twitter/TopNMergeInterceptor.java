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
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.TopNInterceptor;
import org.apache.flume.tools.Counter;
import org.apache.flume.tools.InterceptorRegistry;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import com.google.common.collect.Lists;

/**
 * A Flume interceptor that merges the topN lists emitted by upstream {@link HashtagTopNInterceptor}
 * interceptors into a single, global topN list.
 * <p>
 * It assumes that the counters emitted by the upstream {@link HashtagTopNInterceptor} interceptors
 * are grouped, i.e. a specific counter is always routed to a single {@link HashtagTopNInterceptor}
 * and will therefore only appear in a single topN list.
 * <p>
 * See {@link TopNInterceptor}.
 */
public class TopNMergeInterceptor extends TopNInterceptor {
  private static final Logger LOG = Logger.getLogger(TopNMergeInterceptor.class);
  private final JsonFactory jsonFactory = new JsonFactory();

  /** {@inheritDoc} */
  public TopNMergeInterceptor(int topN) throws IOException {
    super(topN);
  }

  /** {@inheritDoc} */
  @Override
  public void initialize() {
    InterceptorRegistry.register(TopNMergeInterceptor.class, this);
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    InterceptorRegistry.deregister(this);
  }

  /** {@inheritDoc} */
  @Override
  public List<Counter> getCounters(Event event) {
    List<Counter> counters = Lists.newArrayList();
    JsonParser jp = null;

    try {
      jp = jsonFactory.createJsonParser(event.getBody());

      while (jp.nextToken() != null) {
        if (jp.getCurrentToken().equals(JsonToken.START_ARRAY) && jp.getCurrentName().equals(TOP_N)) {
          while (jp.nextToken() != null) {
            if (jp.getCurrentToken() == JsonToken.START_OBJECT) {
              jp.nextToken();
              String item = jp.getCurrentName();
              long count = jp.nextLongValue(0);
              counters.add(new Counter(item, count));
            }
          }
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

    return counters;
  }

  /**
   * Builder which builds new instance of TopNMergeInterceptor.
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
        return new TopNMergeInterceptor(topN);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
