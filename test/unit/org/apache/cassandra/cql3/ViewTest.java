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

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import junit.framework.Assert;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;


public class ViewTest extends ViewTestBase
{

    @Test
    public void testNonExistingOnes() throws Throwable
    {
        assertInvalidMessage("Cannot drop non existing materialized view", "DROP MATERIALIZED VIEW " + KEYSPACE + ".view_does_not_exist");
        assertInvalidMessage("Cannot drop non existing materialized view", "DROP MATERIALIZED VIEW keyspace_does_not_exist.view_does_not_exist");

        execute("DROP MATERIALIZED VIEW IF EXISTS " + KEYSPACE + ".view_does_not_exist");
        execute("DROP MATERIALIZED VIEW IF EXISTS keyspace_does_not_exist.view_does_not_exist");
    }

    @Test
    public void createMvWithUnrestrictedPKParts() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT k1 FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

    }

    @Test
    public void testClusteringKeyTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT k1 FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)");
        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)");

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from view1").size());

        updateView("DELETE FROM %s WHERE k1 = 1 and c1 = 3");

        Assert.assertEquals(1, execute("select * from %s").size());
        Assert.assertEquals(1, execute("select * from view1").size());
    }

    @Test
    public void testPrimaryKeyIsNotNull() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY((k, asciival)))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        // Must include "IS NOT NULL" for primary keys
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s");
            Assert.fail("Should fail if no primary key is filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        // Must include both when the partition key is composite
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE bigintval IS NOT NULL AND asciival IS NOT NULL PRIMARY KEY (bigintval, k, asciival)");
            Assert.fail("Should fail if compound primary is not completely filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        dropTable("DROP TABLE %s");

        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY(k, asciival))");
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s");
            Assert.fail("Should fail if no primary key is filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        // Can omit "k IS NOT NULL" because we have a sinlge partition key
        createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE bigintval IS NOT NULL AND asciival IS NOT NULL PRIMARY KEY (bigintval, k, asciival)");
    }

    @Test
    public void testStaticTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "sval text static, " +
                    "val text, " +
                    "PRIMARY KEY(k,c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        try
        {
            createView("mv_static", "CREATE MATERIALIZED VIEW %%s AS SELECT * FROM %s WHERE sval IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (sval,k,c)");
            Assert.fail("Use of static column in a MV primary key should fail");
        }
        catch (InvalidQueryException e)
        {
        }

        try
        {
            createView("mv_static", "CREATE MATERIALIZED VIEW %%s AS SELECT val, sval FROM %s WHERE val IS NOT NULL AND  k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val, k, c)");
            Assert.fail("Explicit select of static column in MV should fail");
        }
        catch (InvalidQueryException e)
        {
        }

        try
        {
            createView("mv_static", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");
            Assert.fail("Implicit select of static column in MV should fail");
        }
        catch (InvalidQueryException e)
        {
        }

        createView("mv_static", "CREATE MATERIALIZED VIEW %s AS SELECT val,k,c FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,c,sval,val)VALUES(?,?,?,?)", 0, i % 2, "bar" + i, "baz");

        Assert.assertEquals(2, execute("select * from %s").size());

        assertRows(execute("SELECT sval from %s"), row("bar99"), row("bar99"));

        Assert.assertEquals(2, execute("select * from mv_static").size());

        assertInvalid("SELECT sval from mv_static");
    }

    @Test
    public void testCountersTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "count counter)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        try
        {
            createView("mv_counter", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE count IS NOT NULL AND k IS NOT NULL PRIMARY KEY (count,k)");
            Assert.fail("MV on counter should fail");
        }
        catch (InvalidQueryException e)
        {
        }
    }

    @Test
    public void testDurationsTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "result duration)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        try
        {
            createView("mv_duration", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE result IS NOT NULL AND k IS NOT NULL PRIMARY KEY (result,k)");
            Assert.fail("MV on duration should fail");
        }
        catch (InvalidQueryException e)
        {
            Assert.assertEquals("Cannot use Duration column 'result' in PRIMARY KEY of materialized view", e.getMessage());
        }
    }

    @Test
    public void testBuilderWidePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "intval int, " +
                    "PRIMARY KEY (k, c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());


        for(int i = 0; i < 1024; i++)
            execute("INSERT INTO %s (k, c, intval) VALUES (?, ?, ?)", 0, i, 0);

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, c, k)");


        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv"))
            Thread.sleep(1000);

        assertRows(execute("SELECT count(*) from %s WHERE k = ?", 0), row(1024L));
        assertRows(execute("SELECT count(*) from mv WHERE intval = ?", 0), row(1024L));
    }

    @Test
    public void testCompoundPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY((k, asciival)))");

        TableMetadata metadata = currentTableMetadata();

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        for (ColumnMetadata def : new HashSet<>(metadata.columns()))
        {
            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ("
                               + def.name + ", k" + (def.name.toString().equals("asciival") ? "" : ", asciival") + ")";
                createView("mv1_" + def.name, query);

                if (def.type.isMultiCell())
                    Assert.fail("MV on a multicell should fail " + def);
            }
            catch (InvalidQueryException e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }


            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + " PRIMARY KEY ("
                               + def.name + ", asciival" + (def.name.toString().equals("k") ? "" : ", k") + ")";
                createView("mv2_" + def.name, query);

                if (def.type.isMultiCell())
                    Assert.fail("MV on a multicell should fail " + def);
            }
            catch (InvalidQueryException e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }

            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ((" + def.name + ", k), asciival)";
                createView("mv3_" + def.name, query);

                if (def.type.isMultiCell())
                    Assert.fail("MV on a multicell should fail " + def);
            }
            catch (InvalidQueryException e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }


            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ((" + def.name + ", k), asciival)";
                createView("mv3_" + def.name, query);

                Assert.fail("Should fail on duplicate name");
            }
            catch (Exception e)
            {
            }

            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ((" + def.name + ", k), nonexistentcolumn)";
                createView("mv3_" + def.name, query);
                Assert.fail("Should fail with unknown base column");
            }
            catch (InvalidQueryException e)
            {
            }
        }

        updateView("INSERT INTO %s (k, asciival, bigintval) VALUES (?, ?, fromJson(?))", 0, "ascii text", "123123123123");
        updateView("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"), row(123123123123L));

        //Check the MV
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"), row(0, 123123123123L));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0), row(0, 123123123123L));
        assertRows(execute("SELECT k from mv1_bigintval WHERE bigintval = ?", 123123123123L), row(0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 123123123123L, 0), row("ascii text"));


        //UPDATE BASE
        updateView("INSERT INTO %s (k, asciival, bigintval) VALUES (?, ?, fromJson(?))", 0, "ascii text", "1");
        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"), row(1L));

        //Check the MV
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"), row(0, 1L));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0), row(0, 1L));
        assertRows(execute("SELECT k from mv1_bigintval WHERE bigintval = ?", 123123123123L));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 123123123123L, 0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 1L, 0), row("ascii text"));


        //test truncate also truncates all MV
        execute("TRUNCATE %s");

        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"));
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 1L, 0));
    }

    @Test
    public void testCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "intval int, " +
                    "listval list<int>, " +
                    "PRIMARY KEY (k))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)");

        updateView("INSERT INTO %s (k, intval, listval) VALUES (?, ?, fromJson(?))", 0, 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, listval from mv WHERE intval = ?", 0), row(0, list(1, 2, 3)));

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 1, 1);
        updateView("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 1, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 1), row(1, list(1, 2, 3)));
        assertRows(execute("SELECT k, listval from mv WHERE intval = ?", 1), row(1, list(1, 2, 3)));
    }


    @Test
    public void testFrozenCollectionsWithComplicatedInnerType() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, intval int,  listval frozen<list<tuple<text,text>>>, PRIMARY KEY (k))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND listval IS NOT NULL PRIMARY KEY (k, listval)");

        updateView("INSERT INTO %s (k, intval, listval) VALUES (?, ?, fromJson(?))",
                   0,
                   0,
                   "[[\"a\",\"1\"], [\"b\",\"2\"], [\"c\",\"3\"]]");

        // verify input
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
        assertRows(execute("SELECT k, listval from mv"),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));

        // update listval with the same value and it will be compared in view generator
        updateView("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))",
                   0,
                   "[[\"a\",\"1\"], [\"b\",\"2\"], [\"c\",\"3\"]]");
        // verify result
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
        assertRows(execute("SELECT k, listval from mv"),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
    }

    @Test
    public void testUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "intval int, " +
                    "PRIMARY KEY (k))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)");

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 0);
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 0));
        assertRows(execute("SELECT k, intval from mv WHERE intval = ?", 0), row(0, 0));

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 1);
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 1));
        assertRows(execute("SELECT k, intval from mv WHERE intval = ?", 1), row(0, 1));
    }

    @Test
    public void testIgnoreUpdate() throws Throwable
    {
        // regression test for CASSANDRA-10614

        createTable("CREATE TABLE %s (" +
                    "a int, " +
                    "b int, " +
                    "c int, " +
                    "d int, " +
                    "PRIMARY KEY (a, b))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT a, b, c FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        assertRows(execute("SELECT a, b, c from mv WHERE b = ?", 0), row(0, 0, 0));

        updateView("UPDATE %s SET d = ? WHERE a = ? AND b = ?", 0, 0, 0);
        assertRows(execute("SELECT a, b, c from mv WHERE b = ?", 0), row(0, 0, 0));

        // Note: errors here may result in the test hanging when the memtables are flushed as part of the table drop,
        // because empty rows in the memtable will cause the flush to fail.  This will result in a test timeout that
        // should not be ignored.
        String table = KEYSPACE + "." + currentTable();
        updateView("BEGIN BATCH " +
                   "INSERT INTO " + table + " (a, b, c, d) VALUES (?, ?, ?, ?); " + // should be accepted
                   "UPDATE " + table + " SET d = ? WHERE a = ? AND b = ?; " +  // should be accepted
                   "APPLY BATCH",
                   0, 0, 0, 0,
                   1, 0, 1);
        assertRows(execute("SELECT a, b, c from mv WHERE b = ?", 0), row(0, 0, 0));
        assertRows(execute("SELECT a, b, c from mv WHERE b = ?", 1), row(0, 1, null));

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore("mv");
        cfs.forceBlockingFlush();
        Assert.assertEquals(1, cfs.getLiveSSTables().size());
    }

    @Test
    public void testClusteringOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b, c))" +
                    "WITH CLUSTERING ORDER BY (b ASC, c DESC)");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, b, c) WITH CLUSTERING ORDER BY (b DESC)");
        createView("mv2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, c, b) WITH CLUSTERING ORDER BY (c ASC)");
        createView("mv3", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, b, c)");
        createView("mv4", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, c, b) WITH CLUSTERING ORDER BY (c DESC)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 1);
        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 2, 2);

        com.datastax.driver.core.ResultSet mvRows = executeNet(protocolVersion, "SELECT b FROM mv1");
        assertRowsNet(protocolVersion, mvRows,
                      row(2),
                      row(1));

        mvRows = executeNet(protocolVersion, "SELECT c FROM mv2");
        assertRowsNet(protocolVersion, mvRows,
                      row(1),
                      row(2));

        mvRows = executeNet(protocolVersion, "SELECT b FROM mv3");
        assertRowsNet(protocolVersion, mvRows,
                      row(1),
                      row(2));

        mvRows = executeNet(protocolVersion, "SELECT c FROM mv4");
        assertRowsNet(protocolVersion, mvRows,
                      row(2),
                      row(1));
    }

    @Test
    public void testPrimaryKeyOnlyTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        // Cannot use SELECT *, as those are always handled by the includeAll shortcut in View.updateAffectsView
        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %%s WHERE b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);

        com.datastax.driver.core.ResultSet mvRows = executeNet(protocolVersion, "SELECT a, b FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(1, 1));
    }

    @Test
    public void testPartitionKeyOnlyTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "PRIMARY KEY ((a, b)))");

        executeNet(protocolVersion, "USE " + keyspace());

        // Cannot use SELECT *, as those are always handled by the includeAll shortcut in View.updateAffectsView
        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);

        com.datastax.driver.core.ResultSet mvRows = executeNet(protocolVersion, "SELECT a, b FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(1, 1));
    }


    @Test
    public void testCollectionInView() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c map<int, text>," +
                    "PRIMARY KEY (a))");

        executeNet(protocolVersion, "USE " + keyspace());
        createView("mvmap", "CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %%s WHERE b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 0, 0);
        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, b FROM mvmap WHERE b = ?", 0);
        assertRowsNet(protocolVersion, mvRows, row(0, 0));

        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, map(1, "1"));
        mvRows = executeNet(protocolVersion, "SELECT a, b FROM mvmap WHERE b = ?", 1);
        assertRowsNet(protocolVersion, mvRows, row(1, 1));

        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, map(0, "0"));
        mvRows = executeNet(protocolVersion, "SELECT a, b FROM mvmap WHERE b = ?", 0);
        assertRowsNet(protocolVersion, mvRows, row(0, 0));
    }

    @Test
    public void testMultipleNonPrimaryKeysInView() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "e int," +
                    "PRIMARY KEY ((a, b), c))");

        try
        {
            createView("mv_de", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND d IS NOT NULL AND e IS NOT NULL PRIMARY KEY ((d, a), b, e, c)");
            Assert.fail("Should have rejected a query including multiple non-primary key base columns");
        }
        catch (Exception e)
        {
        }

        try
        {
            createView("mv_de", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND d IS NOT NULL AND e IS NOT NULL PRIMARY KEY ((a, b), c, d, e)");
            Assert.fail("Should have rejected a query including multiple non-primary key base columns");
        }
        catch (Exception e)
        {
        }

    }

    @Test
    public void testNullInClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (id1 int, id2 int, v1 text, v2 text, PRIMARY KEY (id1, id2))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS" +
                   "  SELECT id1, v1, id2, v2" +
                   "  FROM %%s" +
                   "  WHERE id1 IS NOT NULL AND v1 IS NOT NULL AND id2 IS NOT NULL" +
                   "  PRIMARY KEY (id1, v1, id2)" +
                   "  WITH CLUSTERING ORDER BY (v1 DESC, id2 ASC)");

        execute("INSERT INTO %s (id1, id2, v1, v2) VALUES (?, ?, ?, ?)", 0, 1, "foo", "bar");

        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM %s"), row(0, 1, "foo", "bar"));
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM mv"), row(0, "foo", 1, "bar"));

        executeNet(protocolVersion, "UPDATE %s SET v1=? WHERE id1=? AND id2=?", null, 0, 1);
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM %s"), row(0, 1, null, "bar"));
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM mv"));

        executeNet(protocolVersion, "UPDATE %s SET v2=? WHERE id1=? AND id2=?", "rab", 0, 1);
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM %s"), row(0, 1, null, "rab"));
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM mv"));
    }

    @Test
    public void testReservedKeywordsInMV() throws Throwable
    {
        createTable("CREATE TABLE %s (\"token\" int PRIMARY KEY, \"keyspace\" int)");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS" +
                   "  SELECT \"keyspace\", \"token\"" +
                   "  FROM %%s" +
                   "  WHERE \"keyspace\" IS NOT NULL AND \"token\" IS NOT NULL" +
                   "  PRIMARY KEY (\"keyspace\", \"token\")");

        execute("INSERT INTO %s (\"token\", \"keyspace\") VALUES (?, ?)", 0, 1);

        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM %s"), row(0, 1));
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM mv"), row(1, 0));
    }

    private void testViewBuilderResume(int concurrentViewBuilders) throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "val text, " +
                    "PRIMARY KEY(k,c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        CompactionManager.instance.setConcurrentViewBuilders(concurrentViewBuilders);
        CompactionManager.instance.setCoreCompactorThreads(1);
        CompactionManager.instance.setMaximumCompactorThreads(1);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, ""+i);

        cfs.forceBlockingFlush();

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, ""+i);

        cfs.forceBlockingFlush();

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, ""+i);

        cfs.forceBlockingFlush();

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, ""+i);

        cfs.forceBlockingFlush();

        String viewName1 = "mv_test_" + concurrentViewBuilders;
        createView(viewName1, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        cfs.enableAutoCompaction();
        List<Future<?>> futures = CompactionManager.instance.submitBackground(cfs);

        String viewName2 = viewName1 + "_2";
        //Force a second MV on the same base table, which will restart the first MV builder...
        createView(viewName2, "CREATE MATERIALIZED VIEW %s AS SELECT val, k, c FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");


        //Compact the base table
        FBUtilities.waitOnFutures(futures);

        while (!SystemKeyspace.isViewBuilt(keyspace(), viewName1))
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        assertRows(execute("SELECT count(*) FROM " + viewName1), row(1024L));
    }

    @Test
    public void testViewBuilderResume() throws Throwable
    {
        for (int i = 1; i <= 8; i *= 2)
        {
            testViewBuilderResume(i);
        }
    }
}