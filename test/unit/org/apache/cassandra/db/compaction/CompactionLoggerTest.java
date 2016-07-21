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
package org.apache.cassandra.db.compaction;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.List;
import java.util.Arrays;
import java.nio.charset.Charset;
import java.io.IOException;
import java.io.PrintWriter;

import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonNode;

public class CompactionLoggerTest extends CQLTester
{
    private static String logDirectory;
    private static Path compactionLog;

    @BeforeClass
    public static void beforeLogging() throws Throwable
    {
        System.setProperty("cassandra.logdir", "/tmp");
        logDirectory = System.getProperty("cassandra.logdir", ".");
        compactionLog = Paths.get(logDirectory, "compaction.log");
        deleteLogs();
    }

    @AfterClass
    public static void afterLogging() throws Throwable
    {
        deleteLogs();
    }

    private static void deleteLogs() throws IOException
    {
        Files.deleteIfExists(compactionLog);
        int count = 0;
        Path path;
        do {
            path = Paths.get(logDirectory, String.format("compaction-%d.log", count++));
        } while (Files.deleteIfExists(path));
    }

    private class Attr {
        public String key;
        public String value;

        public Attr(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    private static int countLogs(List<String> logs, List<Attr> attrs) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        int count = 0;
        for (String log : logs) {
            JsonNode node = mapper.readTree(log);
            Boolean qualifies = true;
            for (Attr attr : attrs) {
                if (!node.get(attr.key).asText().equals(attr.value)) {
                    qualifies = false;
                    break;
                }
            }
            if (qualifies) {
                count++;
            }
        }
        return count;
    }

    private void testCompactionLogger(String options) throws Throwable
    {
        String query = String.format("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = %s", "%s", options);
        createTable(query);
        AbstractCompactionStrategy acs = getCurrentColumnFamilyStore().getCompactionStrategy();
        assertTrue(acs.isEnabled() && acs.logAll);

        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        compact();

        assertTrue(Files.exists(compactionLog));
        List<String> logs = Files.readAllLines(compactionLog, Charset.defaultCharset());

        List<Attr> attrs1 = Arrays.asList(new Attr("type", "flush"), new Attr("table", currentTable()));
        assertTrue(countLogs(logs, attrs1) == 2);

        List<Attr> attrs2 = Arrays.asList(new Attr("type", "compaction"), new Attr("table", currentTable()));
        assertTrue(countLogs(logs, attrs2) == 1);
    }

    @Test
    public void testCompactionLoggerSTCS() throws Throwable
    {
        String options = "{'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'log_all':'true'};";
        testCompactionLogger(options);
    }

    @Test
    public void testCompactionLoggerLCS() throws Throwable
    {
        String options = "{'class':'LeveledCompactionStrategy', 'sstable_size_in_mb':1, 'log_all':'true'};";
        testCompactionLogger(options);
    }

    @Test
    public void testCompactionLoggerDTCS() throws Throwable
    {
        String options = "{'class':'DateTieredCompactionStrategy', 'min_threshold':2, 'log_all':'true'};";
        testCompactionLogger(options);
    }
}
