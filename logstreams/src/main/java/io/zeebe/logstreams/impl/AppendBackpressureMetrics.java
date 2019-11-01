/*
 * Copyright © 2019  camunda services GmbH (info@camunda.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package io.zeebe.logstreams.impl;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

public class AppendBackpressureMetrics {

  private static final Counter DROPPED_REQUEST_COUNT =
      Counter.build()
          .namespace("zeebe")
          .name("dropped_request_count_total")
          .help("Number of requests dropped due to backpressure")
          .labelNames("partition")
          .register();

  private static final Counter TOTAL_REQUEST_COUNT =
      Counter.build()
          .namespace("zeebe")
          .name("received_request_count_total")
          .help("Number of requests received")
          .labelNames("partition")
          .register();

  private static final Gauge CURRENT_INFLIGHT =
      Gauge.build()
          .namespace("zeebe")
          .name("backpressure_inflight_requests_count")
          .help("Current number of request inflight")
          .labelNames("partition")
          .register();

  private static final Gauge CURRENT_LIMIT =
      Gauge.build()
          .namespace("zeebe")
          .name("backpressure_requests_limit")
          .help("Current limit for number of inflight requests")
          .labelNames("partition")
          .register();

  public void dropped(int partitionId) {
    DROPPED_REQUEST_COUNT.labels(String.valueOf(partitionId)).inc();
  }

  public void receivedRequest(int partitionId) {
    TOTAL_REQUEST_COUNT.labels(String.valueOf(partitionId)).inc();
  }

  public void incInflight(int partitionId) {
    CURRENT_INFLIGHT.labels(String.valueOf(partitionId)).inc();
  }

  public void decInflight(int partitionId) {
    CURRENT_INFLIGHT.labels(String.valueOf(partitionId)).dec();
  }

  public void setNewLimit(int partitionId, int newLimit) {
    CURRENT_LIMIT.labels(String.valueOf(partitionId)).set(newLimit);
  }

  public void setInflight(int partitionId, int count) {
    CURRENT_INFLIGHT.labels(String.valueOf(partitionId)).set(0);
  }
}
