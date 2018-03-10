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
package org.apache.cassandra.db;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.PartitionRangeQueryPager;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * A read command that selects a (part of a) range of partitions.
 */
public class PartitionRangeReadCommand extends ReadCommand
{
    protected static final SelectionDeserializer selectionDeserializer = new Deserializer();

    private final DataRange dataRange;
    private int oldestUnrepairedTombstone = Integer.MAX_VALUE;

    private PartitionRangeReadCommand(boolean isDigest,
                                     int digestVersion,
                                     TableMetadata metadata,
                                     int nowInSec,
                                     ColumnFilter columnFilter,
                                     RowFilter rowFilter,
                                     DataLimits limits,
                                     DataRange dataRange,
                                     IndexMetadata index)
    {
        super(Kind.PARTITION_RANGE, isDigest, digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, index);
        this.dataRange = dataRange;
    }

    public static PartitionRangeReadCommand create(TableMetadata metadata,
                                                   int nowInSec,
                                                   ColumnFilter columnFilter,
                                                   RowFilter rowFilter,
                                                   DataLimits limits,
                                                   DataRange dataRange)
    {
        return new PartitionRangeReadCommand(false,
                                             0,
                                             metadata,
                                             nowInSec,
                                             columnFilter,
                                             rowFilter,
                                             limits,
                                             dataRange,
                                             findIndex(metadata, rowFilter));
    }

    /**
     * Creates a new read command that query all the data in the table.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     *
     * @return a newly created read command that queries everything in the table.
     */
    public static PartitionRangeReadCommand allDataRead(TableMetadata metadata, int nowInSec)
    {
        return new PartitionRangeReadCommand(false,
                                             0,
                                             metadata,
                                             nowInSec,
                                             ColumnFilter.all(metadata),
                                             RowFilter.NONE,
                                             DataLimits.NONE,
                                             DataRange.allData(metadata.partitioner),
                                             null);
    }

    public DataRange dataRange()
    {
        return dataRange;
    }

    public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key)
    {
        return dataRange.clusteringIndexFilter(key);
    }

    public boolean isNamesQuery()
    {
        return dataRange.isNamesQuery();
    }

    /**
     * Returns an equivalent command but that only queries data within the provided range.
     *
     * @param range the sub-range to restrict the command to. This method <b>assumes</b> that this is a proper sub-range
     * of the command this is applied to.
     * @param isRangeContinuation whether {@code range} is a direct continuation of whatever previous range we have
     * queried. This matters for the {@code DataLimits} that may contain states when we do paging and in the context of
     * parallel queries: that state only make sense if the range queried is indeed the follow-up of whatever range we've
     * previously query (that yield said state). In practice this means that ranges for which {@code isRangeContinuation}
     * is false may have to be slightly pessimistic when counting data and may include a little bit than necessary, and
     * this should be dealt with post-query (in the case of {@code StorageProxy.getRangeSlice()}, which uses this method
     * for replica queries, this is dealt with by re-counting results on the coordinator). Note that if this is the
     * first range we queried, then the {@code DataLimits} will have not state and the value of this parameter doesn't
     * matter.
     */
    public PartitionRangeReadCommand forSubRange(AbstractBounds<PartitionPosition> range, boolean isRangeContinuation)
    {
        // If we're not a continuation of whatever range we've previously queried, we should ignore the states of the
        // DataLimits as it's either useless, or misleading. This is particularly important for GROUP BY queries, where
        // DataLimits.CQLGroupByLimits.GroupByAwareCounter assumes that if GroupingState.hasClustering(), then we're in
        // the middle of a group, but we can't make that assumption if we query and range "in advance" of where we are
        // on the ring.
        return new PartitionRangeReadCommand(isDigestQuery(),
                                             digestVersion(),
                                             metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             isRangeContinuation ? limits() : limits().withoutState(),
                                             dataRange().forSubRange(range),
                                             indexMetadata());
    }

    public PartitionRangeReadCommand copy()
    {
        return new PartitionRangeReadCommand(isDigestQuery(),
                                             digestVersion(),
                                             metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             limits(),
                                             dataRange(),
                                             indexMetadata());
    }

    public PartitionRangeReadCommand copyAsDigestQuery()
    {
        return new PartitionRangeReadCommand(true,
                                             digestVersion(),
                                             metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             limits(),
                                             dataRange(),
                                             indexMetadata());
    }

    public ReadCommand withUpdatedLimit(DataLimits newLimits)
    {
        return new PartitionRangeReadCommand(isDigestQuery(),
                                             digestVersion(),
                                             metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             newLimits,
                                             dataRange(),
                                             indexMetadata());
    }

    public PartitionRangeReadCommand withUpdatedLimitsAndDataRange(DataLimits newLimits, DataRange newDataRange)
    {
        return new PartitionRangeReadCommand(isDigestQuery(),
                                             digestVersion(),
                                             metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             newLimits,
                                             newDataRange,
                                             indexMetadata());
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getRangeRpcTimeout();
    }

    public boolean selectsKey(DecoratedKey key)
    {
        if (!dataRange().contains(key))
            return false;

        return rowFilter().partitionKeyRestrictionsAreSatisfiedBy(key, metadata().partitionKeyType);
    }

    public boolean selectsClustering(DecoratedKey key, Clustering clustering)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
            return !columnFilter().fetchedColumns().statics.isEmpty();

        if (!dataRange().clusteringIndexFilter(key).selects(clustering))
            return false;
        return rowFilter().clusteringKeyRestrictionsAreSatisfiedBy(clustering);
    }

    public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestExecutionException
    {
        return StorageProxy.getRangeSlice(this, consistency, queryStartNanoTime);
    }

    public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion)
    {
            return new PartitionRangeQueryPager(this, pagingState, protocolVersion);
    }

    protected void recordLatency(TableMetrics metric, long latencyNanos)
    {
        metric.rangeLatency.addNano(latencyNanos);
    }

    @VisibleForTesting
    public UnfilteredPartitionIterator queryStorage(final ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        return cfs.storageHandler.queryPartitionRange(this);
    }

    @Override
    protected int oldestUnrepairedTombstone()
    {
        return oldestUnrepairedTombstone;
    }

    public void updateUnrepairedTombstone(int unrepairedTombstone)
    {
        oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, unrepairedTombstone);
    }

    public MessageOut<ReadCommand> createMessage()
    {
        return new MessageOut<>(MessagingService.Verb.RANGE_SLICE, this, serializer);
    }

    protected void appendCQLWhereClause(StringBuilder sb)
    {
        if (dataRange.isUnrestricted() && rowFilter().isEmpty())
            return;

        sb.append(" WHERE ");
        // We put the row filter first because the data range can end by "ORDER BY"
        if (!rowFilter().isEmpty())
        {
            sb.append(rowFilter());
            if (!dataRange.isUnrestricted())
                sb.append(" AND ");
        }
        if (!dataRange.isUnrestricted())
            sb.append(dataRange.toCQLString(metadata()));
    }

    /**
     * Allow to post-process the result of the query after it has been reconciled on the coordinator
     * but before it is passed to the CQL layer to return the ResultSet.
     *
     * See CASSANDRA-8717 for why this exists.
     */
    public PartitionIterator postReconciliationProcessing(PartitionIterator result)
    {
        ColumnFamilyStore cfs = Keyspace.open(metadata().keyspace).getColumnFamilyStore(metadata().name);
        Index index = getIndex(cfs);
        return index == null ? result : index.postProcessorFor(this).apply(result, this);
    }

    @Override
    public boolean selectsFullPartition()
    {
        return metadata().isStaticCompactTable() ||
               (dataRange.selectsAllPartition() && !rowFilter().hasExpressionOnClusteringOrRegularColumns());
    }

    @Override
    public String toString()
    {
        return String.format("Read(%s columns=%s rowfilter=%s limits=%s %s)",
                             metadata().toString(),
                             columnFilter(),
                             rowFilter(),
                             limits(),
                             dataRange().toString(metadata()));
    }

    protected void serializeSelection(DataOutputPlus out, int version) throws IOException
    {
        DataRange.serializer.serialize(dataRange(), out, version, metadata());
    }

    protected long selectionSerializedSize(int version)
    {
        return DataRange.serializer.serializedSize(dataRange(), version, metadata());
    }

    /*
     * We are currently using PartitionRangeReadCommand for most index queries, even if they are explicitly restricted
     * to a single partition key. Return true if that is the case.
     *
     * See CASSANDRA-11617 and CASSANDRA-11872 for details.
     */
    public boolean isLimitedToOnePartition()
    {
        return dataRange.keyRange instanceof Bounds
            && dataRange.startKey().kind() == PartitionPosition.Kind.ROW_KEY
            && dataRange.startKey().equals(dataRange.stopKey());
    }

    private static class Deserializer extends SelectionDeserializer
    {
        public ReadCommand deserialize(DataInputPlus in,
                                       int version,
                                       boolean isDigest,
                                       int digestVersion,
                                       TableMetadata metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       IndexMetadata index)
        throws IOException
        {
            DataRange range = DataRange.serializer.deserialize(in, version, metadata);
            return new PartitionRangeReadCommand(isDigest, digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, range, index);
        }
    }
}
