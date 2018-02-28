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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.datastax.driver.core.Row;
import junit.framework.Assert;

public class ViewTTLTest extends ViewTestBase
{
    @Test
    public void ttlTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TTL 3", 1, 1, 1, 1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 2);

        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        List<Row> results = executeNet(protocolVersion, "SELECT d FROM mv WHERE c = 2 AND a = 1 AND b = 1").all();
        Assert.assertEquals(1, results.size());
        Assert.assertTrue("There should be a null result given back due to ttl expiry", results.get(0).isNull(0));
    }

    @Test
    public void ttlExpirationTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TTL 3", 1, 1, 1, 1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(4));
        Assert.assertEquals(0, executeNet(protocolVersion, "SELECT * FROM mv WHERE c = 1 AND a = 1 AND b = 1").all().size());
    }

    @Test
    public void testCreateMvWithTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int) WITH default_time_to_live = 60");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        // Must NOT include "default_time_to_live" for Materialized View creation
        try
        {
            createView("mv_ttl1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c) WITH default_time_to_live = 30");
            Assert.fail("Should fail if TTL is provided for materialized view");
        }
        catch (Exception e)
        {
        }
    }

    @Test
    public void testAlterMvWithTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int) WITH default_time_to_live = 60");

        createView("mv_ttl2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)");

        // Must NOT include "default_time_to_live" on alter Materialized View
        try
        {
            executeNet(protocolVersion, "ALTER MATERIALIZED VIEW %s WITH default_time_to_live = 30");
            Assert.fail("Should fail if TTL is provided while altering materialized view");
        }
        catch (Exception e)
        {
        }
    }
}
