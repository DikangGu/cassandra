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

package org.apache.cassandra.service.paxos;

import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.ReadCallback;

public class PrepareAndReadCallback implements IAsyncCallback<PrepareAndReadResponse>
{
    public final PrepareCallback prepareCallback;
    public final ReadCallback readCallback;
    public PartitionIterator iterator = null;

    public PrepareAndReadCallback(PrepareCallback prepareCallback, ReadCallback readCallback)
    {
        this.prepareCallback = prepareCallback;
        this.readCallback = readCallback;
    }

    @Override
    public void response(MessageIn<PrepareAndReadResponse> msg)
    {
        PrepareAndReadResponse response = msg.payload;
        prepareCallback.response(response.prepareResponse, msg.from);
        readCallback.response(response.readResponse);
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }

    public void await()
    {
        readCallback.awaitResults();
        iterator = ((DataResolver)readCallback.resolver).getData();
    }
}
