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

package org.apache.cassandra.db.engine;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadataRef;

public interface IKeyspace
{
    /**
     * Return the name of the keyspace
     */
    String getName();

    /**
     * Before any read or write operations are performaned on a keyspace, the cassandra server
     * will call the open method to open the keyspace data.
     */
    void open();

    /**
     * Create a new column family
     */
    void createColumnFamily(TableMetadataRef metadata);

    /**
     * Drop a column family
     */
    void dropColumnFamily(TableId tableId);

    Collection<IColumnFamilyStore> getColumnFamilyStores();

    /**
     * Apply a mutation into the keyspace, future version.
     */
    Future<?> applyFuture(IMutation mutation,
                                     boolean writeCommitLog);

    /**
     * Apply a mutation into the keyspace.
     */
    void apply(Mutation mutation, boolean writeCommitLog);

    /**
     * Trigger flush of the keyspace.
     */
    List<Future<?>> flush();

    /**
     * Take a snapshot of the table, if supported.
     */
    void snapshot(String snapshotName, String columnFamilyName);

    boolean snapshotExists(String snapshotName);

    void clearSnapshot(String snapshotName);

    AbstractReplicationStrategy getReplicationStrategy();
}
