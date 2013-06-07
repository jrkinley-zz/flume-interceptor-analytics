package org.apache.flume.interceptor;

import java.util.List;

import org.apache.flume.Event;

public interface AnalyticInterceptor extends Interceptor {
  /**
   * Get a list of {@link Event events} from the analytic for sending downstream.
   * @return List of {@link Event events}
   */
  public List<Event> getStatsEvents();
}
