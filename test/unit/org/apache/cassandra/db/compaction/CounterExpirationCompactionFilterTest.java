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

import static org.apache.cassandra.Util.cellname;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.BufferCounterUpdateCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ClockAndCount;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(OrderedJUnit4ClassRunner.class)
public class CounterExpirationCompactionFilterTest
{
    private static final String KEYSPACE1 = "CounterExpirationCompactionFilterTest";
    private static final String CF1 = "Counter1";
    private static final long EXPIRATION_SECONDS = 2l;
    private static final long EXPIRATION_MICROSECONDS = 2000000l;
    private long expiredTimestampMicros;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        Map<String, String> counterExpirationFilterOptions = new HashMap<>();
        counterExpirationFilterOptions.put("expiration_seconds", String.valueOf(EXPIRATION_SECONDS));
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE1, CF1)
                        .defaultValidator(CounterColumnType.instance)
                        .compactionFilterClass(CounterExpirationCompactionFilter.class)
                        .compactionFilterOptions(counterExpirationFilterOptions));
    }

    @Before
    public void init()
    {
        expiredTimestampMicros = FBUtilities.timestampMicros() - EXPIRATION_MICROSECONDS;
    }

    @Test
    public void testCellSanity() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        Cell cell = new BufferCounterUpdateCell(cellname(1), 1L, expiredTimestampMicros);
        assertTrue(((CounterExpirationCompactionFilter) cfs.getCompactionFilter()).isSane(cell));
        cell = new BufferCounterUpdateCell(cellname(1), 1L, (expiredTimestampMicros / 1000));
        assertFalse(((CounterExpirationCompactionFilter) cfs.getCompactionFilter()).isSane(cell));
    }

    @Test
    public void testCounterSingleCell() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        DecoratedKey dk = Util.dk("key1");

        // Do the initial update (+1)
        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterUpdateCell(cellname(1), 1L, expiredTimestampMicros));
        new CounterMutation(new Mutation(KEYSPACE1, dk.getKey(), cells), ConsistencyLevel.ONE).apply();
        ColumnFamily current = cfs
                .getColumnFamily(QueryFilter.getIdentityFilter(dk, CF1, System.currentTimeMillis()));
        assertEquals(1L, CounterContext.instance().total(current.getColumn(cellname(1)).value()));

        // Make another increment (+2)
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterUpdateCell(cellname(1), 2L, expiredTimestampMicros));
        new CounterMutation(new Mutation(KEYSPACE1, dk.getKey(), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk, CF1, System.currentTimeMillis()));
        assertEquals(3L, CounterContext.instance().total(current.getColumn(cellname(1)).value()));

        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getSSTables().size());

        // compaction filter will purge the expired counter cell
        // and purge the now empty sstable altogether
        cfs.forceMajorCompaction();
        assertEquals(0, cfs.getSSTables().size());
    }

    @Test
    public void testCounterTwoCells() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        DecoratedKey dk = Util.dk("key1");

        // Do the initial update (+1, -1)
        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterUpdateCell(cellname(1), 1L, expiredTimestampMicros));
        cells.addColumn(new BufferCounterUpdateCell(cellname(2), -1L, expiredTimestampMicros));
        new CounterMutation(new Mutation(KEYSPACE1, dk.getKey(), cells), ConsistencyLevel.ONE).apply();
        ColumnFamily current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk, CF1, System.currentTimeMillis()));
        assertEquals(1L, CounterContext.instance().total(current.getColumn(cellname(1)).value()));
        assertEquals(-1L, CounterContext.instance().total(current.getColumn(cellname(2)).value()));

        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getSSTables().size());

        // Increment only cell1, leave cell2 expired
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addCounter(cellname(1), 1L); // will update cell timestamp to now
        new CounterMutation(new Mutation(KEYSPACE1, dk.getKey(), cells), ConsistencyLevel.ONE).apply();
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk, CF1, System.currentTimeMillis()));
        assertEquals(2L, CounterContext.instance().total(current.getColumn(cellname(1)).value()));
        assertEquals(-1L, CounterContext.instance().total(current.getColumn(cellname(2)).value()));
        cfs.forceBlockingFlush();
        assertEquals(2, cfs.getSSTables().size());

        // compaction filter will only purge expired cell2, cell1 remains
        cfs.forceMajorCompaction();
        assertEquals(1, cfs.getSSTables().size());
        current = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk, CF1, System.currentTimeMillis()));
        assertEquals(1, current.getColumnCount());
        Cell cell1 = current.getColumn(cellname(1));
        assert (CounterExpirationCompactionFilter.millisBeforeExpiration(cell1, EXPIRATION_SECONDS) > 0);
        assertEquals(2L, CounterContext.instance().total(cell1.value()));

        Thread.sleep((EXPIRATION_SECONDS + 1) * 1000); // ensure expiration_seconds is reached

        // compaction filter will purge expired cell1
        cfs.forceMajorCompaction();
        assertEquals(0, cfs.getSSTables().size());
    }

    @Test
    public void testCacheInvalidation() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        CacheService.instance.invalidateCounterCache();
        DecoratedKey dk = Util.dk("key1");

        // validate empty cache
        assertEquals(0, CacheService.instance.counterCache.size());
        assertNull(cfs.getCachedCounter(dk.getKey(), cellname(1)));

        // Do the initial update (+1)
        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterUpdateCell(cellname(1), 1L, expiredTimestampMicros));
        new CounterMutation(new Mutation(KEYSPACE1, dk.getKey(), cells), ConsistencyLevel.ONE).apply();
        ColumnFamily current = cfs
                .getColumnFamily(QueryFilter.getIdentityFilter(dk, CF1, System.currentTimeMillis()));
        assertEquals(1L, CounterContext.instance().total(current.getColumn(cellname(1)).value()));

        // check cache
        assertEquals(1, CacheService.instance.counterCache.size());
        assertEquals(ClockAndCount.create(1L, 1L), cfs.getCachedCounter(dk.getKey(), cellname(1)));

        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getSSTables().size());

        // compaction filter
        cfs.forceMajorCompaction();

        // validate empty cache
        assertEquals(0, CacheService.instance.counterCache.size());
        assertNull(cfs.getCachedCounter(dk.getKey(), cellname(1)));
    }

    @Test
    public void testAnticompaction() throws Exception
    {
        // Anticompaction is run at the end of each repair job.
        // Verify that compaction filter is executed in the anticompaction code path,
        // which will ensure expired cells [are|remain] purged during repair.

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        DecoratedKey dk = Util.dk("key1");
        DecoratedKey dk2 = Util.dk("key2");

        // Do the initial update (+1)
        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterUpdateCell(cellname(1), 1L, expiredTimestampMicros));
        new CounterMutation(new Mutation(KEYSPACE1, dk.getKey(), cells), ConsistencyLevel.ONE).apply();
        ColumnFamily current = cfs
                .getColumnFamily(QueryFilter.getIdentityFilter(dk, CF1, System.currentTimeMillis()));
        assertEquals(1L, CounterContext.instance().total(current.getColumn(cellname(1)).value()));

        // flush to sstable
        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getSSTables().size());

        // anticompaction, compaction filter
        Range<Token> range = new Range<Token>(new BytesToken(dk.getKey()), new BytesToken(dk2.getKey()));
        Collection<SSTableReader> sstables = cfs.getSSTables();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
                Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(cfs, Arrays.asList(range), refs, txn, 12345);
        }
        assertEquals(0, cfs.getSSTables().size());
    }
}
