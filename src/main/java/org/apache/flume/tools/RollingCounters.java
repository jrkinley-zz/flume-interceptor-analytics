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
package org.apache.flume.tools;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Counts objects in a sliding window.
 * <p>
 * An objects counter is split into buckets, with each bucket representing an equal fraction of time
 * of the overall window length. An objects total count is the sum of its buckets.
 * <p>
 * Counter increments are added to the head bucket, whilst a separate "reaper" thread advances the
 * window by shifting the head bucket and wiping the tail bucket every (windowLenSec / numBuckets)
 * seconds. Therefore as an objects frequency decreases, its total count also decreases over time.
 * Stale objects (those with a total count of zero) are removed from the list to free up memory.
 * <p>
 * Increasing the number of buckets increases the granularity of the counters (i.e. a higher number
 * of buckets causes the "reaper" thread to run more frequently, which in turn will decrement an
 * objects total count faster as its frequency decreases).
 * <p>
 * @param <T> The type of objects to count
 */
public class RollingCounters<T> {
  private static final Logger LOG = Logger.getLogger(RollingCounters.class);
  private static final String REAPER_NAME = "Reaper";
  private final Map<T, long[]> objCounts = Maps.newHashMap();

  private final int numBuckets;
  private final long millisPerBucket;
  private Thread reaper;
  private int head;
  private int tail;

  /**
   * @param numBuckets The number of buckets that compose the sliding window
   * @param windowLenSec The length of the sliding window in seconds
   */
  public RollingCounters(int numBuckets, int windowLenSec) {
    this.numBuckets = numBuckets;
    this.millisPerBucket = (windowLenSec * 1000) / numBuckets;
    this.head = 0;
    this.tail = setTail();
    startReaper();

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Head pos set to [%d], tail pos set to [%d]", head, tail));
    }
  }

  /**
   * Starts the reaper thread which takes care of advancing the sliding window and removing stale
   * counters (objects with a count of zero)
   */
  private void startReaper() {
    reaper = new Thread(new Runnable() {
      public void run() {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Reaper thread started");
        }
        while (true) {
          try {
            Thread.sleep(millisPerBucket);
          } catch (InterruptedException e) {
            throw new RuntimeException(
                "Reaper thread has been interrupted, sliding window will no longer advance", e);
          }

          synchronized (objCounts) {
            Set<T> objToRemove = Sets.newHashSet();
            for (T obj : objCounts.keySet()) {
              if (getTotalCount(obj) == 0) {
                objToRemove.add(obj);
              }
              long[] buckets = objCounts.get(obj);
              buckets[tail] = 0; // Wipe tail bucket
              if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Wiped tail bucket [%d] for: %s", tail, obj));
              }
            }
            for (T obj : objToRemove) {
              objCounts.remove(obj); // Remove stale objects
              if (LOG.isDebugEnabled()) {
                LOG.debug("Removed stale object: " + obj);
              }
            }
            // Advance window
            head = tail;
            tail = setTail();
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Head pos set to [%d], tail pos set to [%d]", head, tail));
          }
        }
      }
    });
    reaper.setName(REAPER_NAME);
    reaper.start();
  }

  /**
   * @return The tail bucket
   */
  private int setTail() {
    return (head + 1) % numBuckets;
  }

  /**
   * Increment the count for the given object
   * @param obj
   * @return The new total
   */
  public long incrementCount(T obj) {
    long total = 0;
    synchronized (objCounts) {
      long[] buckets = objCounts.get(obj);
      if (buckets == null) {
        buckets = new long[numBuckets];
        objCounts.put(obj, buckets);
      }
      buckets[head]++;
      for (long b : buckets) {
        total += b;
      }
    }
    return total;
  }

  /**
   * Gets the current objects and their totals
   * @return Map of objects to totals
   */
  public Map<T, Long> getCounters() {
    Map<T, Long> totals = Maps.newHashMap();
    for (T obj : objCounts.keySet()) {
      totals.put(obj, getTotalCount(obj));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Returned %d counters", totals.size()));
    }
    return totals;
  }

  /**
   * @param obj The object to return the count for
   * @return The count for the given object. The count is the sum of all buckets
   */
  private long getTotalCount(T obj) {
    long[] buckets = objCounts.get(obj);
    long total = 0;
    for (long b : buckets) {
      total += b;
    }
    return total;
  }
}
