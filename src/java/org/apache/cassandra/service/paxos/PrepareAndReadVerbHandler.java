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

import java.io.IOException;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class PrepareAndReadVerbHandler implements IVerbHandler<PrepareAndReadCommand>
{
    @Override
    public void doVerb(MessageIn<PrepareAndReadCommand> message, int id) throws IOException
    {
        PrepareAndReadCommand prepareCommand = message.payload;
        PrepareResponse paxosResponse = PaxosState.prepare(prepareCommand.commit);
        ReadCommand command = prepareCommand.readCommand;

        ReadResponse readResponse;

        try (ReadExecutionController executionController = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(executionController))
        {
            readResponse = command.createResponse(iterator);
        }

        PrepareAndReadResponse response = new PrepareAndReadResponse(paxosResponse, readResponse);

        MessageOut<PrepareAndReadResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, response, PrepareAndReadResponse.serializer);
        MessagingService.instance().sendReply(reply, id, message.from);
    }
}
