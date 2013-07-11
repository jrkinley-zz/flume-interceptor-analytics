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

import java.util.Map;

import org.apache.flume.tools.RollingCounters;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TestRollingCounters {
  private static final Logger LOG = Logger.getLogger(TestRollingCounters.class);
  private static final int NUM_BUCKETS = 6;
  private static final int WINDOW_LEN_SEC = 30;
  private static final int INC = 10;
  private static final String WORD_ONE = "ONE";
  private static final String WORD_TWO = "TWO";

  private RollingCounters<String> counters;
  private long t1 = 0, t2 = 0;

  /**
   * Test {@link RollingCounters} by loading up some counters and ensuring that they decrease over
   * time (i.e. as the "reaper" thread advances the window):
   * 
   * <pre>
   * {@code
   * numBuckets=6
   * windowLenSec=30
   * millisPerBucket=5000 (5 sec)
   * 
   * Time (sec):
   * 0    5    10   15   20   25   30   35   40
   * 
   * Head bucket:
   * 0    1    2    3    4    5    0    1    2
   * 
   * Observed counts:
   * 10   10   0    0    0    0    0    0    0
   * 
   * Total returned by {@link RollingCounters}:
   * 10   20   20   20   20   20   10   0    0
   * }
   * </pre>
   * @throws InterruptedException
   */
  @Test
  public void testRollingCounters() throws InterruptedException {
    counters = new RollingCounters<String>(NUM_BUCKETS, WINDOW_LEN_SEC);
    long wait = (WINDOW_LEN_SEC * 1000) / NUM_BUCKETS;

    // Wait a second to offset against the reaper
    Thread.sleep(1000);

    // Load up for 10 seconds (first 2 buckets)
    for (int i = 0; i < 2; i++) {
      loadCounters(INC, (i + 1) * INC);
      Thread.sleep(wait);
    }

    Map<String, Long> currentTotals;

    // Check counts at 5 second intervals for next 20 seconds. The counts should stay the same as
    // we're still within the window length of 30 seconds (6 buckets)
    for (int i = 0; i < 4; i++) {
      currentTotals = counters.getCounters();
      t1 = currentTotals.get(WORD_ONE);
      t2 = currentTotals.get(WORD_TWO);
      LOG.info(String.format("Counters: %s:%d\t%s:%d", WORD_ONE, t1, WORD_TWO, t2));
      Assert.assertEquals(20, t1);
      Assert.assertEquals(10, t2);
      Thread.sleep(wait);
    }

    // Check counts at 5 second intervals for next 10 seconds. The counts should start to decrease
    // as the window advances
    for (int i = 2; i > 0; i--) {
      currentTotals = counters.getCounters();
      t1 = currentTotals.get(WORD_ONE);
      t2 = currentTotals.get(WORD_TWO);
      LOG.info(String.format("Counters: %s:%d\t%s:%d", WORD_ONE, t1, WORD_TWO, t2));
      Assert.assertEquals((i - 1) * 10, t1);
      Assert.assertEquals((i - 1) * 5, t2);
      Thread.sleep(wait);
    }
  }

  private void loadCounters(int increment, long expected) {
    for (int i = 0; i < increment; i++) {
      t1 = counters.incrementCount(WORD_ONE);
      if (i % 2 == 0) {
        t2 = counters.incrementCount(WORD_TWO);
      }
    }

    LOG.info(String.format("Counters: %s:%d\t%s:%d", WORD_ONE, t1, WORD_TWO, t2));
    Assert.assertEquals(expected, t1);
    Assert.assertEquals((expected / 2), t2);
  }
}
