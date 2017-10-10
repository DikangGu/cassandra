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

package org.apache.cassandra.engine.streaming;

import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamTask;
import org.apache.cassandra.streaming.messages.IncomingFileMessage;

public abstract class AbstractStreamReceiveTask extends StreamTask
{
    // number of files to receive
    protected final int totalFiles;
    // total size of files to receive
    protected final long totalSize;

    // true if task is done (either completed or aborted)
    protected volatile boolean done = false;

    protected AbstractStreamReceiveTask(StreamSession session, TableId tableId, int totalFiles, long totalSize)
    {
        super(session, tableId);
        this.totalFiles = totalFiles;
        this.totalSize = totalSize;
    }

    public int getTotalNumberOfFiles()
    {
        return totalFiles;
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    public void abort()
    {
        done = true;
    }

    public abstract void receive(IncomingFileMessage message);
}
