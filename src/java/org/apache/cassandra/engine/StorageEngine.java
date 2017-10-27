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

package org.apache.cassandra.engine;

import java.util.Collection;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.engine.streaming.AbstractStreamReceiveTask;
import org.apache.cassandra.engine.streaming.AbstractStreamTransferTask;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.utils.SearchIterator;

public interface StorageEngine
{
    /**
     * This method should create all necessary files but does not need to open the table.
     */
    void create(String tableName);

    /**
     * Before any read or write operations are performaned on a table, the cassandra server
     * will call the open method to open the table data.
     */
    void open(String tableName);

    /**
     * Apply a mutation to storage engine
     */
    void applyMutate(String tableName,
                     PartitionUpdate partitionUpdate,
                     boolean writeCommitLog);

    /**
     * In-memory representation of a Partition.
     *
     * Storage engine needs to implement iterators (UnfilteredRowIterator) to
     * avoid "materializing" a full partition/query response in memory as much as possible.
     */
    public interface Partition
    {
        public TableMetadata metadata();
        public DecoratedKey partitionKey();
        public DeletionTime partitionLevelDeletion();

        public RegularAndStaticColumns columns();

        public EncodingStats stats();

        /**
         * Whether the partition object has no informations at all, including any deletion informations.
         */
        public boolean isEmpty();

        /**
         * Returns the row corresponding to the provided clustering, or null if there is not such row.
         */
        public Row getRow(Clustering clustering);

        /**
         * Returns an iterator that allows to search specific rows efficiently.
         */
        public SearchIterator<Clustering, Row> searchIterator(ColumnFilter columns, boolean reversed);

        /**
         * Returns an UnfilteredRowIterator over all the rows/RT contained by this partition.
         */
        public UnfilteredRowIterator unfilteredIterator();

        /**
         * Returns an UnfilteredRowIterator over the rows/RT contained by this partition
         * selected by the provided slices.
         */
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, Slices slices, boolean reversed);
    }

    void forceFlush(String tableName);

    void truncate(String tableName);

    void close(String tableName);

    void setCompactionThroughputMbPerSec(int throughputMbPerSec);

    void forceMajorCompaction(String tableName);

    /**
     *  Streaming APIs
     */
    AbstractStreamTransferTask getStreamTransferTask(StreamSession session,
                                                     String tableName,
                                                     Collection<Range<Token>> ranges);

    AbstractStreamReceiveTask getStreamReceiveTask(StreamSession session,
                                                   StreamSummary summary);

    boolean cleanUpRanges(final String tableName);
}
