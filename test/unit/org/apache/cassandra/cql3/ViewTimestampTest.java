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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import junit.framework.Assert;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.FBUtilities;

public class ViewTimestampTest extends ViewTestBase
{
    @Test
    public void testOldTimestamps() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "val text, " +
                    "PRIMARY KEY(k,c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv_tstest", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,c,val)VALUES(?,?,?)", 0, i % 2, "baz");

        Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush();

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from mv_tstest").size());

        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("baz"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(0), row(1));

        //Make sure an old TS does nothing
        updateView("UPDATE %s USING TIMESTAMP 100 SET val = ? where k = ? AND c = ?", "bar", 0, 0);
        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("baz"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(0), row(1));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "bar"));

        //Latest TS
        updateView("UPDATE %s SET val = ? where k = ? AND c = ?", "bar", 0, 0);
        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("bar"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "bar"), row(0));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(1));
    }

    @Test
    public void testRegularColumnTimestampUpdates() throws Throwable
    {
        // Regression test for CASSANDRA-10910

        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv_rctstest", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)");

        updateView("UPDATE %s SET c = ?, val = ? WHERE k = ?", 0, 0, 0);
        updateView("UPDATE %s SET val = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s SET c = ? WHERE k = ?", 1, 0);
        assertRows(execute("SELECT c, k, val FROM mv_rctstest"), row(1, 0, 1));

        execute("TRUNCATE %s");

        updateView("UPDATE %s USING TIMESTAMP 1 SET c = ?, val = ? WHERE k = ?", 0, 0, 0);
        updateView("UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s USING TIMESTAMP 2 SET val = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE k = ?", 2, 0);
        updateView("UPDATE %s USING TIMESTAMP 3 SET val = ? WHERE k = ?", 2, 0);

        assertRows(execute("SELECT c, k, val FROM mv_rctstest"), row(2, 0, 2));
        assertRows(execute("SELECT c, k, val FROM mv_rctstest limit 1"), row(2, 0, 2));
    }

    @Test
    public void complexTimestampUpdateTestWithFlush() throws Throwable
    {
        complexTimestampUpdateTest(true);
    }

    @Test
    public void complexTimestampUpdateTestWithoutFlush() throws Throwable
    {
        complexTimestampUpdateTest(false);
    }

    public void complexTimestampUpdateTest(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY (a, b))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, a, b)");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        //Set initial values TS=0, leaving e null and verify view
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c, d) VALUES (0, 0, 1, 0) USING TIMESTAMP 0");
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0));

        //update c's timestamp TS=2
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        // change c's value and TS=3, tombstones c=1 and adds c=0 record
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? and b = ? ", 0, 0, 0);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0));

        if(flush)
        {
            ks.getColumnFamilyStore("mv").forceMajorCompaction();
            FBUtilities.waitOnFutures(ks.flush());
        }


        //change c's value back to 1 with TS=4, check we can see d
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0);
        if (flush)
        {
            ks.getColumnFamilyStore("mv").forceMajorCompaction();
            FBUtilities.waitOnFutures(ks.flush());
        }

        assertRows(execute("SELECT d,e from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, null));


        //Add e value @ TS=1
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 1 SET e = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d,e from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, 1));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());


        //Change d value @ TS=2
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 2 SET d = ? WHERE a = ? and b = ? ", 2, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(2));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());


        //Change d value @ TS=3
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(1));


        //Tombstone c
        executeNet(protocolVersion, "DELETE FROM %s WHERE a = ? and b = ?", 0, 0);
        assertRows(execute("SELECT d from mv"));

        //Add back without D
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c) VALUES (0, 0, 1)");

        //Make sure D doesn't pop back in.
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row((Object) null));


        //New partition
        // insert a row with timestamp 0
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP 0", 1, 0, 0, 0, 0);

        // overwrite pk and e with timestamp 1, but don't overwrite d
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c, e) VALUES (?, ?, ?, ?) USING TIMESTAMP 1", 1, 0, 0, 0);

        // delete with timestamp 0 (which should only delete d)
        executeNet(protocolVersion, "DELETE FROM %s USING TIMESTAMP 0 WHERE a = ? AND b = ?", 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, null, 0)
        );

        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? AND b = ?", 1, 1, 0);
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? AND b = ?", 0, 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, null, 0)
        );

        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? AND b = ?", 0, 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, 0, 0)
        );
    }

    @Test
    public void conflictingTimestampTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        for (int i = 0; i < 50; i++)
        {
            updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP 1", 1, 1, i);
        }

        ResultSet mvRows = executeNet(protocolVersion, "SELECT c FROM mv");
        List<Row> rows = executeNet(protocolVersion, "SELECT c FROM %s").all();
        Assert.assertEquals("There should be exactly one row in base", 1, rows.size());
        int expected = rows.get(0).getInt("c");
        assertRowsNet(protocolVersion, mvRows, row(expected));
    }
}
