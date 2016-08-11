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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.NoSpamLogger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

public class CompactionLogger
{
    public interface Strategy
    {
        JsonNode sstable(SSTableReader sstable);

        JsonNode options();

        static Strategy none = new Strategy()
        {
            public JsonNode sstable(SSTableReader sstable)
            {
                return null;
            }

            public JsonNode options()
            {
                return null;
            }
        };
    }

    /**
     * This will produce the compaction strategy's starting information.
     */
    public interface StrategySummary
    {
        JsonNode getSummary();
    }

    /**
     * This is an interface to allow writing to a different interface.
     */
    public interface Writer
    {
        /**
         * This is used when we are already trying to write out the start of a
         *
         * @param statement This should be written out to the medium capturing the logs
         * @param tag       This is an identifier for a strategy; each strategy should have a distinct Object
         */
        void writeStart(JsonNode statement, Object tag);

        /**
         * @param statement This should be written out to the medium capturing the logs
         * @param summary   This can be used when a tag is not recognized by this writer; this can be because the file
         *                  has been rolled, or otherwise the writer had to start over
         * @param tag       This is an identifier for a strategy; each strategy should have a distinct Object
         */
        void write(JsonNode statement, StrategySummary summary, Object tag);
    }

    private interface CompactionStrategyAndTableFunction
    {
        JsonNode apply(AbstractCompactionStrategy strategy, SSTableReader sstable);
    }

    private static final JsonNodeFactory json = JsonNodeFactory.instance;
    private static final Logger logger = LoggerFactory.getLogger(CompactionLogger.class);
    private static final Writer serializer = new CompactionLogSerializer();
    private final ColumnFamilyStore cfs;
    private final WrappingCompactionStrategy csw;
    private final AtomicInteger identifier = new AtomicInteger(0);
    private final Map<AbstractCompactionStrategy, String> compactionStrategyMapping = new ConcurrentHashMap<>();
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private StrategySummary startStrategiesWrapper = new StrategySummary()
    {
        public JsonNode getSummary()
        {
            return startStrategies();
        }
    };

    public CompactionLogger(ColumnFamilyStore cfs, WrappingCompactionStrategy csw)
    {
        this.csw = csw;
        this.cfs = cfs;
    }

    private ArrayNode compactionStrategyMap(Function<AbstractCompactionStrategy, JsonNode> select)
    {
        ArrayNode node = json.arrayNode();
        List<AbstractCompactionStrategy> strategies = csw.getStrategies();
        for (AbstractCompactionStrategy strategy : strategies)
        {
            node.add(select.apply(strategy));
        }
        return node;
    }

    private ArrayNode sstableMap(Collection<SSTableReader> sstables, CompactionStrategyAndTableFunction csatf)
    {
        ArrayNode node = json.arrayNode();
        for (SSTableReader sstable : sstables)
        {
            node.add(csatf.apply(csw.getCompactionStrategyFor(sstable), sstable));
        }
        return node;
    }

    private String getId(AbstractCompactionStrategy strategy)
    {
        String value = compactionStrategyMapping.get(strategy);
        if (value == null)
        {
            value = String.valueOf(identifier.getAndIncrement());
            if (value != null)
            {
                compactionStrategyMapping.put(strategy, value);
            }
        }
        return value;
    }

    private JsonNode formatSSTables(AbstractCompactionStrategy strategy)
    {
        ArrayNode node = json.arrayNode();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (csw.getCompactionStrategyFor(sstable) == strategy)
                node.add(formatSSTable(strategy, sstable));
        }
        return node;
    }

    private JsonNode formatSSTable(AbstractCompactionStrategy strategy, SSTableReader sstable)
    {
        ObjectNode node = json.objectNode();
        node.put("generation", sstable.descriptor.generation);
        node.put("version", sstable.descriptor.version.getVersion());
        node.put("size", sstable.onDiskLength());
        node.put("filename", sstable.getFilename());
        JsonNode logResult = strategy.strategyLogger().sstable(sstable);
        if (logResult != null)
            node.put("details", logResult);
        return node;
    }

    private JsonNode startStrategy(AbstractCompactionStrategy strategy)
    {
        ObjectNode node = json.objectNode();
        node.put("strategyId", getId(strategy));
        node.put("type", strategy.getName());
        node.put("tables", formatSSTables(strategy));
        node.put("repaired", csw.isRepaired(strategy));
        List<String> folders = csw.getStrategyFolders(strategy);
        ArrayNode folderNode = json.arrayNode();
        for (String folder : folders)
        {
            folderNode.add(folder);
        }
        node.put("folders", folderNode);

        JsonNode logResult = strategy.strategyLogger().options();
        if (logResult != null)
            node.put("options", logResult);
        return node;
    }

    private JsonNode shutdownStrategy(AbstractCompactionStrategy strategy)
    {
        ObjectNode node = json.objectNode();
        node.put("strategyId", getId(strategy));
        return node;
    }

    private JsonNode describeSSTable(AbstractCompactionStrategy strategy, SSTableReader sstable)
    {
        ObjectNode node = json.objectNode();
        node.put("strategyId", getId(strategy));
        node.put("table", formatSSTable(strategy, sstable));
        return node;
    }

    private CompactionStrategyAndTableFunction describeSSTableWrapper = new CompactionStrategyAndTableFunction()
    {
        public JsonNode apply(AbstractCompactionStrategy strategy, SSTableReader sstable)
        {
            return describeSSTable(strategy, sstable);
        }
    };

    private void describeStrategy(ObjectNode node)
    {
        node.put("keyspace", cfs.keyspace.getName());
        node.put("table", cfs.getColumnFamilyName());
        node.put("time", System.currentTimeMillis());
    }

    private JsonNode startStrategies()
    {
        ObjectNode node = json.objectNode();
        node.put("type", "enable");
        describeStrategy(node);
        node.put("strategies", compactionStrategyMap(new Function<AbstractCompactionStrategy, JsonNode>()
        {
            public JsonNode apply(AbstractCompactionStrategy strategy)
            {
                return startStrategy(strategy);
            }
        }));
        return node;
    }

    public void enable()
    {
        if (enabled.compareAndSet(false, true))
        {
            serializer.writeStart(startStrategies(), this);
        }
    }

    public void disable()
    {
        if (enabled.compareAndSet(true, false))
        {
            ObjectNode node = json.objectNode();
            node.put("type", "disable");
            describeStrategy(node);
            node.put("strategies", compactionStrategyMap(new Function<AbstractCompactionStrategy, JsonNode>()
            {
                public JsonNode apply(AbstractCompactionStrategy strategy)
                {
                    return shutdownStrategy(strategy);
                }
            }));
            serializer.write(node, startStrategiesWrapper, this);
        }
    }

    public void flush(SSTableReader sstable)
    {
        if (enabled.get())
        {
            ObjectNode node = json.objectNode();
            node.put("type", "flush");
            describeStrategy(node);
            node.put("tables", describeSSTable(csw.getCompactionStrategyFor(sstable), sstable));
            serializer.write(node, startStrategiesWrapper, this);
        }
    }

    public void compaction(long startTime, Collection<SSTableReader> input, long endTime,
            Collection<SSTableReader> output)
    {
        if (enabled.get())
        {
            ObjectNode node = json.objectNode();
            node.put("type", "compaction");
            describeStrategy(node);
            node.put("start", String.valueOf(startTime));
            node.put("end", String.valueOf(endTime));
            node.put("input", sstableMap(input, describeSSTableWrapper));
            node.put("output", sstableMap(output, describeSSTableWrapper));
            serializer.write(node, startStrategiesWrapper, this);
        }
    }

    public void pending(AbstractCompactionStrategy strategy, int remaining)
    {
        if (remaining != 0 && enabled.get())
        {
            ObjectNode node = json.objectNode();
            node.put("type", "pending");
            describeStrategy(node);
            node.put("strategyId", getId(strategy));
            node.put("pending", remaining);
            serializer.write(node, startStrategiesWrapper, this);
        }
    }

    public void compactionFilter(AbstractCompactionFilter compactionFilter, DecoratedKey key, Cell cell, boolean isFiltered,
            Map<String, String> details)
    {
        if (enabled.get())
        {
            ObjectNode node = json.objectNode();
            node.put("type", "compaction_filter");
            describeStrategy(node);
            node.put("filterType", compactionFilter.getName());
            node.put("logSamplingPct", compactionFilter.logSamplingPct);
            node.put("isFiltered", isFiltered);
            node.put("key", key.toString());
            node.put("cell", cell.getString(cfs.metadata.comparator));
            for (String dKey : details.keySet())
            {
                node.put(dKey, details.get(dKey));
            }
            serializer.write(node, startStrategiesWrapper, this);
        }
    }

    private static class CompactionLogSerializer implements Writer
    {
        private static final String logDirectory = System.getProperty("cassandra.logdir", ".");
        private final ExecutorService loggerService = Executors.newFixedThreadPool(1);
        // This is only accessed on the logger service thread, so it does not need to be thread safe
        private final Set<Object> rolled = new HashSet<>();
        private OutputStreamWriter stream;

        private static OutputStreamWriter createStream() throws IOException
        {
            int count = 0;
            Path compactionLog = Paths.get(logDirectory, "compaction.log");
            if (Files.exists(compactionLog))
            {
                Path tryPath = compactionLog;
                while (Files.exists(tryPath))
                {
                    tryPath = Paths.get(logDirectory, String.format("compaction-%d.log", count++));
                }
                Files.move(compactionLog, tryPath);
            }

            return new OutputStreamWriter(
                    Files.newOutputStream(compactionLog, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
        }

        private void writeLocal(String toWrite)
        {
            try
            {
                if (stream == null)
                    stream = createStream();
                stream.write(toWrite);
                stream.flush();
            }
            catch (IOException ioe)
            {
                // We'll drop the change and log the error to the logger.
                NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 1, TimeUnit.MINUTES,
                                 "Could not write to the log file: {}", ioe);
            }

        }

        public void writeStart(JsonNode statement, Object tag)
        {
            final String toWrite = statement.toString() + System.lineSeparator();
            final Object c_tag = tag;
            loggerService.execute(new Runnable()
            {
                public void run()
                {
                    rolled.add(c_tag);
                    writeLocal(toWrite);
                }
            });
        }

        public void write(JsonNode statement, StrategySummary summary, Object tag)
        {
            final String toWrite = statement.toString() + System.lineSeparator();
            final StrategySummary c_summary = summary;
            final Object c_tag = tag;
            loggerService.execute(new Runnable()
            {
                public void run()
                {
                    if (!rolled.contains(c_tag))
                    {
                        writeLocal(c_summary.getSummary().toString() + System.lineSeparator());
                        rolled.add(c_tag);
                    }
                    writeLocal(toWrite);
                }
            });
        }
    }
}
