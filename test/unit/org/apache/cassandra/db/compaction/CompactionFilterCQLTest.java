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

import static org.junit.Assert.assertEquals;

import org.apache.cassandra.cql3.CQLTester;
import org.junit.Test;

public class CompactionFilterCQLTest extends CQLTester
{
    @Test
    public void testSetCompactionFilter() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        AbstractCompactionFilter compactionFilter = getCurrentColumnFamilyStore().getCompactionFilter();
        // assert default compaction filter
        assertEquals(compactionFilter.getClass(), NoOpCompactionFilter.class);
        // altering something non-compaction filter related
        execute("ALTER TABLE %s WITH gc_grace_seconds = 1000");
        // should keep the compaction filter
        compactionFilter = getCurrentColumnFamilyStore().getCompactionFilter();
        assertEquals(compactionFilter.getClass(), NoOpCompactionFilter.class);
        // altering compaction filter
        execute("ALTER TABLE %s WITH compaction_filter = {'class':'CounterExpirationCompactionFilter'}");
        // will use the new filter
        compactionFilter = getCurrentColumnFamilyStore().getCompactionFilter();
        assertEquals(compactionFilter.getClass(), CounterExpirationCompactionFilter.class);
    }

    @Test
    public void testSetCompactionFilterOptions() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        AbstractCompactionFilter compactionFilter = getCurrentColumnFamilyStore().getCompactionFilter();
        // assert default compaction filter
        assertEquals(compactionFilter.getClass(), NoOpCompactionFilter.class);
        execute("ALTER TABLE %s WITH compaction_filter = {'class':'CounterExpirationCompactionFilter'}");
        // assert filter and default expiration_seconds
        compactionFilter = getCurrentColumnFamilyStore().getCompactionFilter();
        assertEquals(compactionFilter.getClass(), CounterExpirationCompactionFilter.class);
        assertEquals(((CounterExpirationCompactionFilter) compactionFilter).getExpirationSeconds(),
                CounterExpirationCompactionFilter.DEFAULT_EXPIRATION_SECONDS);
        execute("ALTER TABLE %s WITH compaction_filter = {'class':'CounterExpirationCompactionFilter', 'expiration_seconds':30}");
        // assert new expiration_seconds
        compactionFilter = getCurrentColumnFamilyStore().getCompactionFilter();
        assertEquals(((CounterExpirationCompactionFilter) compactionFilter).getExpirationSeconds(), 30l);
    }

}
