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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public abstract class AbstractCompactionFilter
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractCompactionFilter.class);

    protected static final float DEFAULT_LOG_SAMPLING_PCT = 0f;

    protected static final String LOG_SAMPLING_PCT_OPTION = "log_sampling_pct";

    public Map<String, String> options;

    private final Random rand = new Random();
    protected float logSamplingPct;

    protected final ColumnFamilyStore cfs;

    public AbstractCompactionFilter(ColumnFamilyStore cfs, Map<String, String> options)
    {
        assert cfs != null;
        this.cfs = cfs;
        this.options = ImmutableMap.copyOf(options);

        try
        {
            validateOptions(options);
            String optionValue = options.get(LOG_SAMPLING_PCT_OPTION);
            this.logSamplingPct = optionValue == null ? DEFAULT_LOG_SAMPLING_PCT : Float.parseFloat(optionValue);
        }
        catch (ConfigurationException e)
        {
            logger.warn("Error setting compaction filter options ({}), defaults will be used", e.getMessage());
            this.logSamplingPct = DEFAULT_LOG_SAMPLING_PCT;
        }
    }

    /**
     * @return True if cell should be filtered out.
     *
     * Is responsible for determining if a cell should be purged (skip tombstoning).
     */
    public abstract boolean filter(ColumnFamily cf, DecoratedKey key, Cell cell);

    protected boolean shouldLog()
    {
        return (rand.nextFloat() * 100) < this.logSamplingPct;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        String logPct = options.get(LOG_SAMPLING_PCT_OPTION);
        if (logPct != null)
        {
            try
            {
                float logPctValue = Float.parseFloat(logPct);
                if (logPctValue > 100f || logPctValue < 0f)
                {
                    throw new ConfigurationException(String.format("%s must be >= 0 and <= 100, but was %f",
                            LOG_SAMPLING_PCT_OPTION, logPctValue));
                }
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(
                        String.format("%s is not a parsable float for %s", logPct, LOG_SAMPLING_PCT_OPTION), e);
            }
        }
        Map<String, String> uncheckedOptions = new HashMap<String, String>(options);
        uncheckedOptions.remove(LOG_SAMPLING_PCT_OPTION);
        return uncheckedOptions;
    }

    public String getName()
    {
        return getClass().getSimpleName();
    }
}
