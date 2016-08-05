/*
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
package org.apache.cassandra.metrics;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.metrics.ColumnFamilyMetrics.ColumnFamilyMetricNameFactory;

import com.codahale.metrics.Counter;

public class CompactionFilterMetrics
{
    public final MetricNameFactory factory;

    /** Total cells filtered in this CF */
    public final Counter compactionFilterCellsFiltered;
    /** Total cell sanity check failures in this CF */
    public final Counter compactionFilterFailedSanityCheck;

    public CompactionFilterMetrics(final ColumnFamilyStore cfs)
    {
        factory = new ColumnFamilyMetricNameFactory(cfs);
        compactionFilterCellsFiltered = Metrics.counter(factory.createMetricName("CompactionFilterCellsFiltered"));
        compactionFilterFailedSanityCheck = Metrics
                .counter(factory.createMetricName("CompactionFilterFailedSanityCheck"));
    }

}
