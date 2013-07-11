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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorBuilderFactory;

/**
 * Used to register and retrieve instances of {@link Interceptor Interceptors}.
 * <p>
 * This capability could be added to {@link InterceptorBuilderFactory}
 * @param <T>
 */
public class InterceptorRegistry {
  private static Map<Class<?>, Set<Interceptor>> interceptors = Collections
      .synchronizedMap(new HashMap<Class<?>, Set<Interceptor>>());

  /**
   * Register instance of {@link Interceptor}
   * @param type
   * @param instance
   */
  public static void register(Class<?> type, Interceptor instance) {
    if (type == null) {
      throw new IllegalArgumentException("Type may not be null");
    }
    if (instance == null) {
      throw new IllegalArgumentException("Instance may not be null");
    }
    if (!interceptors.containsKey(type)) {
      interceptors.put(type, Collections.synchronizedSet(new HashSet<Interceptor>()));
    }
    interceptors.get(type).add(instance);
  }

  /**
   * Get set of {@link Interceptor} instances for given class
   * @param type
   * @return Set of {@link Interceptor} instances
   */
  public static Set<Interceptor> getInstances(Class<?> type) {
    if (interceptors.containsKey(type)) {
      return interceptors.get(type);
    } else {
      return Collections.emptySet();
    }
  }

  /**
   * Unregister {@link Interceptor} instance
   * @param instance
   */
  public static void deregister(Interceptor instance) {
    interceptors.get(instance.getClass()).remove(instance);
  }

  /**
   * Clear registry
   */
  public static void clear() {
    interceptors.clear();
  }
}
