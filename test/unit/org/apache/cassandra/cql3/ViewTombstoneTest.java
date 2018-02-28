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

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import junit.framework.Assert;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;

import static org.junit.Assert.assertTrue;

public class ViewTombstoneTest extends ViewTestBase
{
    @Test
    public void testExistingRangeTombstoneWithFlush() throws Throwable
    {
        testExistingRangeTombstone(true);
    }

    @Test
    public void testExistingRangeTombstoneWithoutFlush() throws Throwable
    {
        testExistingRangeTombstone(false);
    }

    public void testExistingRangeTombstone(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int, c2 int, v1 int, v2 int, PRIMARY KEY (k1, c1, c2))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1",
                   "CREATE MATERIALIZED VIEW view1 AS SELECT * FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL PRIMARY KEY (k1, c2, c1)");

        updateView("DELETE FROM %s USING TIMESTAMP 10 WHERE k1 = 1 and c1=1");

        if (flush)
            Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush();

        String table = KEYSPACE + "." + currentTable();
        updateView("BEGIN BATCH " +
                   "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 0, 0, 0, 0) USING TIMESTAMP 5; " +
                   "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 0, 1, 0, 1) USING TIMESTAMP 5; " +
                   "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 1, 0, 1, 0) USING TIMESTAMP 5; " +
                   "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 1, 1, 1, 1) USING TIMESTAMP 5; " +
                   "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 1, 2, 1, 2) USING TIMESTAMP 5; " +
                   "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 1, 3, 1, 3) USING TIMESTAMP 5; " +
                   "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 2, 0, 2, 0) USING TIMESTAMP 5; " +
                   "APPLY BATCH");

        assertRowsIgnoringOrder(execute("select * from %s"),
                                row(1, 0, 0, 0, 0),
                                row(1, 0, 1, 0, 1),
                                row(1, 2, 0, 2, 0));
        assertRowsIgnoringOrder(execute("select k1,c1,c2,v1,v2 from view1"),
                                row(1, 0, 0, 0, 0),
                                row(1, 0, 1, 0, 1),
                                row(1, 2, 0, 2, 0));
    }

    @Test
    public void testPartitionTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT k1 FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)");
        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)");

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from view1").size());

        updateView("DELETE FROM %s WHERE k1 = 1");

        Assert.assertEquals(0, execute("select * from %s").size());
        Assert.assertEquals(0, execute("select * from view1").size());
    }

    @Test
    public void testRangeTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "textval1 text, " +
                    "textval2 text, " +
                    "PRIMARY KEY((k, asciival), bigintval, textval1)" +
                    ")");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv_test1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL PRIMARY KEY ((textval2, k), asciival, bigintval, textval1)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,asciival,bigintval,textval1,textval2)VALUES(?,?,?,?,?)", 0, "foo", (long) i % 2, "bar" + i, "baz");

        Assert.assertEquals(50, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(50, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());

        Assert.assertEquals(100, execute("select * from mv_test1").size());

        //Check the builder works
        createView("mv_test2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL PRIMARY KEY ((textval2, k), asciival, bigintval, textval1)");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test2"))
            Thread.sleep(10);

        Assert.assertEquals(100, execute("select * from mv_test2").size());

        createView("mv_test3", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL PRIMARY KEY ((textval2, k), bigintval, textval1, asciival)");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test3"))
            Thread.sleep(10);

        Assert.assertEquals(100, execute("select * from mv_test3").size());
        Assert.assertEquals(100, execute("select asciival from mv_test3 where textval2 = ? and k = ?", "baz", 0).size());

        //Write a RT and verify the data is removed from index
        updateView("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval = ?", 0, "foo", 0L);

        Assert.assertEquals(50, execute("select asciival from mv_test3 where textval2 = ? and k = ?", "baz", 0).size());
    }

    @Test
    public void testRangeTombstone2() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "textval1 text, " +
                    "PRIMARY KEY((k, asciival), bigintval)" +
                    ")");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval1 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL PRIMARY KEY ((textval1, k), asciival, bigintval)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,asciival,bigintval,textval1)VALUES(?,?,?,?)", 0, "foo", (long) i % 2, "bar" + i);

        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());


        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from mv").size());

        //Write a RT and verify the data is removed from index
        updateView("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval = ?", 0, "foo", 0L);

        Assert.assertEquals(1, execute("select * from %s").size());
        Assert.assertEquals(1, execute("select * from mv").size());
    }

    @Test
    public void testRangeTombstone3() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "textval1 text, " +
                    "PRIMARY KEY((k, asciival), bigintval)" +
                    ")");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval1 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL PRIMARY KEY ((textval1, k), asciival, bigintval)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,asciival,bigintval,textval1)VALUES(?,?,?,?)", 0, "foo", (long) i % 2, "bar" + i);

        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());


        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from mv").size());

        //Write a RT and verify the data is removed from index
        updateView("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval >= ?", 0, "foo", 0L);

        Assert.assertEquals(0, execute("select * from %s").size());
        Assert.assertEquals(0, execute("select * from mv").size());
    }

    @Test
    public void rowDeletionTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        String table = keyspace() + "." + currentTable();
        updateView("DELETE FROM " + table + " USING TIMESTAMP 6 WHERE a = 1 AND b = 1;");
        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TIMESTAMP 3", 1, 1, 1, 1);
        Assert.assertEquals(0, executeNet(protocolVersion, "SELECT * FROM mv WHERE c = 1 AND a = 1 AND b = 1").all().size());
    }

    @Test
    public void testMultipleDeletes() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);
        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 2);
        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 3);

        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, b FROM mv1");
        assertRowsNet(protocolVersion, mvRows,
                      row(1, 1),
                      row(1, 2),
                      row(1, 3));

        updateView(String.format("BEGIN UNLOGGED BATCH " +
                                 "DELETE FROM %s WHERE a = 1 AND b > 1 AND b < 3;" +
                                 "DELETE FROM %s WHERE a = 1;" +
                                 "APPLY BATCH", currentTable(), currentTable()));

        mvRows = executeNet(protocolVersion, "SELECT a, b FROM mv1");
        assertRowsNet(protocolVersion, mvRows);
    }

    @Test
    public void testDeleteSingleColumnInViewClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());
        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND d IS NOT NULL PRIMARY KEY (a, d, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, d, b, c FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(0, 0, 0, 0));

        updateView("DELETE c FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeNet(protocolVersion, "SELECT a, d, b, c FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(0, 0, 0, null));

        updateView("DELETE d FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeNet(protocolVersion, "SELECT a, d, b FROM mv1");
        assertTrue(mvRows.isExhausted());
    }

    @Test
    public void testDeleteSingleColumnInViewPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());
        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND d IS NOT NULL PRIMARY KEY (d, a, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, d, b, c FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(0, 0, 0, 0));

        updateView("DELETE c FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeNet(protocolVersion, "SELECT a, d, b, c FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(0, 0, 0, null));

        updateView("DELETE d FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeNet(protocolVersion, "SELECT a, d, b FROM mv1");
        assertTrue(mvRows.isExhausted());
    }
}
