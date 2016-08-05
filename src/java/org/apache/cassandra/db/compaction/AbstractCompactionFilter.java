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
import org.apache.cassandra.db.DecoratedKey;

import com.google.common.collect.ImmutableMap;

public abstract class AbstractCompactionFilter
{
    public Map<String, String> options;

    protected final ColumnFamilyStore cfs;

    public AbstractCompactionFilter(ColumnFamilyStore cfs, Map<String, String> options)
    {
        assert cfs != null;
        this.cfs = cfs;
        this.options = ImmutableMap.copyOf(options);
    }

    /**
     * @return True if cell should be filtered out.
     *
     * Is responsible for determining if a cell should be purged (skip tombstoning).
     */
    public abstract boolean filter(ColumnFamily cf, DecoratedKey key, Cell cell);

    public String getName()
    {
        return getClass().getSimpleName();
    }
}
