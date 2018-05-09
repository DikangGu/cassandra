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

package org.apache.cassandra.cql3;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.junit.Assert.assertTrue;

public class CASOperationTest extends CQLTester
{
    ProtocolVersion protocolVersion = ProtocolVersion.V4;

    @BeforeClass()
    public static void setup()
    {
        System.setProperty("cassandra.use_fastpaxos", "true");
    }

    @Test
    public void testNotExists() throws Throwable
    {
        assertTrue(PaxosState.USE_FASTPAXOS);
        
        createTable("CREATE TABLE %s (k text, v1 int, v2 text, PRIMARY KEY (k, v1))");

        executeNet(protocolVersion, "INSERT INTO %s (k, v1, v2) values (?, ?, ?) IF NOT EXISTS", "first", 1, "value1");
        executeNet(protocolVersion, "INSERT INTO %s (k, v1, v2) values (?, ?, ?) IF NOT EXISTS", "second", 2, "value2");

        assertRows(execute("SELECT * FROM %s WHERE k = ?", "first"),
                   row("first", 1, "value1")
        );

        assertRows(execute("SELECT v2 FROM %s WHERE k = ?", "second"),
                   row("value2")
        );

        executeNet(protocolVersion, "INSERT INTO %s (k, v1, v2) values (?, ?, ?) IF NOT EXISTS", "second", 2, "value3");

        assertRows(execute("SELECT v2 FROM %s WHERE k = ?", "second"),
                   row("value2")
        );
    }

    @Test
    public void testCompareAndSet() throws Throwable
    {
        assertTrue(PaxosState.USE_FASTPAXOS);

        createTable("CREATE TABLE %s (k text, v1 int, v2 int, PRIMARY KEY (k, v1))");

        executeNet(protocolVersion, "INSERT INTO %s (k, v1, v2) values (?, ?, ?) IF NOT EXISTS", "first", 1, 1);
        executeNet(protocolVersion, "INSERT INTO %s (k, v1, v2) values (?, ?, ?) IF NOT EXISTS", "second", 2, 2);

        assertRows(execute("SELECT * FROM %s WHERE k = ?", "first"),
                   row("first", 1, 1));
        assertRows(execute("SELECT v2 FROM %s WHERE k = ?", "second"),
                   row(2));

        executeNet(protocolVersion, "UPDATE %s SET v2 = ? WHERE k = ? AND v1 = ? if v2 = ? ", 3, "second", 2, 2);
        assertRows(execute("SELECT v2 FROM %s WHERE k = ?", "second"),
                   row(3));

        executeNet(protocolVersion, "UPDATE %s SET v2 = ? WHERE k = ? AND v1 = ? if v2 < ? ", 4, "second", 2, 3);
        assertRows(execute("SELECT v2 FROM %s WHERE k = ?", "second"),
                   row(3));

        executeNet(protocolVersion, "UPDATE %s SET v2 = ? WHERE k = ? AND v1 = ? if v2 < ? ", 4, "second", 2, 4);
        assertRows(execute("SELECT v2 FROM %s WHERE k = ?", "second"),
                   row(4));
    }
}