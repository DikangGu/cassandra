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

package org.apache.cassandra.cql3.validation.operations;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * Test SELECT queries with IN restrictions
 */
public class SelectInTest extends CQLTester
{
    @Test
    public void testInWithAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d blob, PRIMARY KEY (a, b, c))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 10, bytes(1));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 11, bytes(2));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 2, 12, bytes(3));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 3, 13, bytes(4));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 4, 14, bytes(5));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 5, 15, bytes(6));

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 10, bytes(1));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 11, bytes(2));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 12, bytes(3));
        
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 3, 13, bytes(4));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 4, 14, bytes(5));
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 5, 15, bytes(6));

        assertRows(execute("SELECT * FROM %s WHERE c in (12, 14, 10) allow filtering"),
                   row(1, 0, 10, bytes(1)),
                   row(1, 2, 12, bytes(3)),
                   row(0, 0, 10, bytes(1)),
                   row(0, 2, 12, bytes(3)),
                   row(0, 4, 14, bytes(5)),
                   row(2, 4, 14, bytes(5)));

        assertRows(execute("SELECT * FROM %s WHERE a = 0 AND c in (12, 14, 10) allow filtering"),
                   row(0, 0, 10, bytes(1)),
                   row(0, 2, 12, bytes(3)),
                   row(0, 4, 14, bytes(5)));

        assertRows(execute("SELECT * FROM %s WHERE a = 1 AND c in (12, 14, 10) allow filtering"),
                   row(1, 0, 10, bytes(1)),
                   row(1, 2, 12, bytes(3)));

        assertRows(execute("SELECT * FROM %s WHERE a = 2 AND c in (12, 14, 10) allow filtering"),
                   row(2, 4, 14, bytes(5)));
    }
}
