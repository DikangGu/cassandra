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
package org.apache.cassandra.db.compaction;

import java.util.Map;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CounterCell;
import org.apache.cassandra.db.DecoratedKey;

public class CounterExpirationCompactionFilter extends AbstractCompactionFilter
{
    public static final long DEFAULT_EXPIRATION_SECONDS = 86400l; // 24hrs
    private static final long SANITY_THRESHOLD_SECONDS = 31536000l; // 1yr

    private static final String EXPIRATION_OPTION = "expiration_seconds";

    private long expirationSeconds;

    public CounterExpirationCompactionFilter(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        String optionValue = options.get(EXPIRATION_OPTION);
        this.expirationSeconds = optionValue == null ? DEFAULT_EXPIRATION_SECONDS : Long.parseLong(optionValue);
    }

    @Override
    public boolean filter(ColumnFamily cf, DecoratedKey key, Cell cell)
    {
        boolean ret = (cell instanceof CounterCell) && isExpired(cell);
        if (ret)
            cfs.compactionFilterMetric.compactionFilterCellsFiltered.inc();
        return ret;
    }

    private boolean isExpired(Cell cell)
    {
        return isSane(cell) && millisBeforeExpiration(cell, this.expirationSeconds) < 0;
    }

    public boolean isSane(Cell cell)
    {
        // We assume cell timestamp is microseconds, so this will return false if:
        // 1. timestamp is milliseconds
        // 2. timestamp is in the future
        // 3. elapsed time > SANITY_THRESHOLD_SECONDS
        long now = System.currentTimeMillis();
        boolean sane = (cell.timestamp() / 1000) < now
                && Math.abs(now - (cell.timestamp() / 1000)) < (SANITY_THRESHOLD_SECONDS * 1000);
        if (!sane)
            cfs.compactionFilterMetric.compactionFilterFailedSanityCheck.inc();
        return sane;
    }

    public static long millisBeforeExpiration(Cell cell, long expirationSeconds)
    {
        return ((cell.timestamp() / 1000) + (expirationSeconds * 1000)) - System.currentTimeMillis();
    }

    public long getExpirationSeconds()
    {
        return expirationSeconds;
    }

    @Override
    public String getName()
    {
        return getClass().getSimpleName();
    }
}
