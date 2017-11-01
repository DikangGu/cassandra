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

import java.util.concurrent.Future;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableMetadata;

public interface IColumnFamilyStore
{
    /**
     * Return the name of the table
     */
    String getName();

    TableMetadata getMetadata();

    /**
     * Before any read or write operations are performaned on a table, the cassandra server
     * will call the open method to open the table data.
     */
    void open();

    /**
     * Apply a mutation (partition update) to the table.
     */
    void apply(PartitionUpdate partitionUpdate);

    /**
     * Trigger flush of the table
     */
    Future<?> flush();

    /**
     * Take a snapshot of the table, if supported.
     */
    void snapshot(String snapshotName);

    boolean snapshotExists(String snapshotName);

    void clearSnapshot(String snapshotName);

    /**
     * Invalidate caches, if supported
     */
    void invalidateCaches();

    /**
     * Deletes the entier column family's data
     */
    void truncate();
}
