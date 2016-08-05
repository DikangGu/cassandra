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

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(OrderedJUnit4ClassRunner.class)
public class NoOpCompactionFilterTest
{
    private static final String KEYSPACE1 = "CompactionFilterTest";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1)
                        .compactionFilterClass(NoOpCompactionFilter.class));
    }

    @Test
    public void testFilter() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        long timestamp = System.currentTimeMillis();
        DecoratedKey dk = Util.dk("key1");
        Mutation rm = new Mutation(KEYSPACE1, dk.getKey());
        rm.add("Standard1", Util.cellname("col0"),
                ByteBufferUtil.EMPTY_BYTE_BUFFER,
                timestamp,
                1);
        rm.add("Standard1", Util.cellname("col1"),
                ByteBufferUtil.EMPTY_BYTE_BUFFER,
                timestamp,
                1);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        rm = new Mutation(KEYSPACE1, dk.getKey());
        rm.add("Standard1", Util.cellname("col2"),
                ByteBufferUtil.EMPTY_BYTE_BUFFER,
                timestamp,
                1);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();
        rm = new Mutation(KEYSPACE1, dk.getKey());
        rm.add("Standard1", Util.cellname("col3"),
                ByteBufferUtil.EMPTY_BYTE_BUFFER,
                timestamp,
                1);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();
        assertEquals(3, cfs.getSSTables().size());
        // Default compaction filter should not filter any cells
        cfs.forceMajorCompaction();
        assertEquals(1, cfs.getSSTables().size());
        ColumnFamily retrieved = cfs
                .getColumnFamily(QueryFilter.getIdentityFilter(dk, CF_STANDARD1, System.currentTimeMillis()));
        assertEquals(4, retrieved.getColumnCount());
    }
}
