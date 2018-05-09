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

import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class PrepareAndReadResponse
{
    public static final PrepareResponseSerializer serializer = new PrepareResponseSerializer();

    public final PrepareResponse prepareResponse;
    public final ReadResponse readResponse;

    public PrepareAndReadResponse(PrepareResponse prepareResponse, ReadResponse readResponse)
    {
        this.prepareResponse = prepareResponse;
        this.readResponse = readResponse;
    }

    @Override
    public String toString()
    {
        return String.format("PrepareAndReadResponse(%s, %s)", prepareResponse, readResponse);
    }

    public static class PrepareResponseSerializer implements IVersionedSerializer<PrepareAndReadResponse>
    {
        public void serialize(PrepareAndReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            ReadResponse.serializer.serialize(response.readResponse, out, version);
            PrepareResponse.serializer.serialize(response.prepareResponse, out, version);
        }

        public PrepareAndReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            ReadResponse readResponse = ReadResponse.serializer.deserialize(in, version);
            PrepareResponse prepareResponse = PrepareResponse.serializer.deserialize(in, version);
            return new PrepareAndReadResponse(prepareResponse, readResponse);
        }

        public long serializedSize(PrepareAndReadResponse response, int version)
        {
            return PrepareResponse.serializer.serializedSize(response.prepareResponse, version) +
                   ReadResponse.serializer.serializedSize(response.readResponse, version);
        }
    }
}
