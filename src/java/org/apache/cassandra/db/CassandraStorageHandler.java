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

package org.apache.cassandra.db;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import static org.apache.cassandra.utils.Throwables.maybeFail;

public class CassandraStorageHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraStorageHandler.class);

    private final ColumnFamilyStore cfs;

    /**
     * Memtables and SSTables on disk for this column family.
     *
     * We synchronize on the Tracker to ensure isolation when we want to make sure
     * that the memtable we're acting on doesn't change out from under us.  I.e., flush
     * syncronizes on it to make sure it can submit on both executors atomically,
     * so anyone else who wants to make sure flush doesn't interfere should as well.
     */
    private final Tracker data;

    private final TableMetadataRef metadata;
    private final Directories directories;
    private final Keyspace keyspace;
    private final String name;

    /* This is used to generate the next index for a SSTable */
    private final AtomicInteger fileIndexGenerator = new AtomicInteger(0);

    /*
    We keep a pool of threads for each data directory, size of each pool is memtable_flush_writers.
    When flushing we start a Flush runnable in the flushExecutor. Flush calculates how to split the
    memtable ranges over the existing data directories and creates a FlushRunnable for each of the directories.
    The FlushRunnables are executed in the perDiskflushExecutors and the Flush will block until all FlushRunnables
    are finished. By having flushExecutor size the same size as each of the perDiskflushExecutors we make sure we can
    have that many flushes going at the same time.
    */
    private static final ExecutorService flushExecutor = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getFlushWriters(),
                                                                                          StageManager.KEEPALIVE,
                                                                                          TimeUnit.SECONDS,
                                                                                          new LinkedBlockingQueue<Runnable>(),
                                                                                          new NamedThreadFactory("MemtableFlushWriter"),
                                                                                          "internal");

    private static final ExecutorService [] perDiskflushExecutors = new ExecutorService[DatabaseDescriptor.getAllDataFileLocations().length];

    static
    {
        for (int i = 0; i < DatabaseDescriptor.getAllDataFileLocations().length; i++)
        {
            perDiskflushExecutors[i] = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getFlushWriters(),
                                                                        StageManager.KEEPALIVE,
                                                                        TimeUnit.SECONDS,
                                                                        new LinkedBlockingQueue<Runnable>(),
                                                                        new NamedThreadFactory("PerDiskMemtableFlushWriter_"+i),
                                                                        "internal");
        }
    }

    // post-flush executor is single threaded to provide guarantee that any flush Future on a CF will never return until prior flushes have completed
    private static final ExecutorService postFlushExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                                              StageManager.KEEPALIVE,
                                                                                              TimeUnit.SECONDS,
                                                                                              new LinkedBlockingQueue<Runnable>(),
                                                                                              new NamedThreadFactory("MemtablePostFlush"),
                                                                                              "internal");

    private static final ExecutorService reclaimExecutor = new JMXEnabledThreadPoolExecutor(1,
                                                                                            StageManager.KEEPALIVE,
                                                                                            TimeUnit.SECONDS,
                                                                                            new LinkedBlockingQueue<Runnable>(),
                                                                                            new NamedThreadFactory("MemtableReclaimMemory"),
                                                                                            "internal");

    public CassandraStorageHandler(ColumnFamilyStore cfs,
                                   int generation,
                                   boolean loadSSTables)
    {
        this.cfs = cfs;
        keyspace = cfs.keyspace;
        metadata = cfs.metadata;
        name = cfs.name;
        directories = cfs.getDirectories();
        fileIndexGenerator.set(generation);

        // Create Memtable only on online
        Memtable initialMemtable = null;
        if (DatabaseDescriptor.isDaemonInitialized())
            initialMemtable = new Memtable(new AtomicReference<>(CommitLog.instance.getCurrentPosition()), cfs);
        data = new Tracker(initialMemtable, loadSSTables);

        // scan for sstables corresponding to this cf and load them
        if (data.loadsstables)
        {
            Directories.SSTableLister sstableFiles = directories.sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true);
            Collection<SSTableReader> sstables = SSTableReader.openAll(sstableFiles.list().entrySet(), metadata);
            data.addInitialSSTables(sstables);
        }
    }

    public long apply(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, CommitLogPosition commitLogPosition)
    {
        Memtable mt = data.getMemtableFor(opGroup, commitLogPosition);
        return mt.put(update, indexer, opGroup);
    }

    public Directories getDirectories()
    {
        return directories;
    }

    public TableMetadata metadata()
    {
        return metadata.get();
    }
    
    public Tracker getTracker()
    {
        return data;
    }

    public void removeUnreadableDirectory(File directory)
    {
        data.removeUnreadableSSTables(directory);
    }

    public void invalidate()
    {
        data.dropSSTables();
    }

    public void reload()
    {
        scheduleFlush();

        // If the CF comparator has changed, we need to change the memtable,
        // because the old one still aliases the previous comparator.
        if (data.getView().getCurrentMemtable().initialComparator != metadata().comparator)
            switchMemtable();
    }

    public static void shutdownPostFlushExecutor() throws InterruptedException
    {
        postFlushExecutor.shutdown();
        postFlushExecutor.awaitTermination(60, TimeUnit.SECONDS);
    }

    void scheduleFlush()
    {
        int period = metadata().params.memtableFlushPeriodInMs;
        if (period > 0)
        {
            logger.trace("scheduling flush in {} ms", period);
            WrappedRunnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow()
                {
                    synchronized (data)
                    {
                        Memtable current = data.getView().getCurrentMemtable();
                        // if we're not expired, we've been hit by a scheduled flush for an already flushed memtable, so ignore
                        if (current.isExpired())
                        {
                            if (current.isClean())
                            {
                                // if we're still clean, instead of swapping just reschedule a flush for later
                                scheduleFlush();
                            }
                            else
                            {
                                // we'll be rescheduled by the constructor of the Memtable.
                                forceFlush();
                            }
                        }
                    }
                }
            };
            ScheduledExecutors.scheduledTasks.schedule(runnable, period, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Switches the memtable iff the live memtable is the one provided
     *
     * @param memtable
     */
    public ListenableFuture<CommitLogPosition> switchMemtableIfCurrent(Memtable memtable)
    {
        synchronized (data)
        {
            if (data.getView().getCurrentMemtable() == memtable)
                return switchMemtable();
        }
        return waitForFlushes();
    }

    /**
     * @return a Future yielding the commit log position that can be guaranteed to have been successfully written
     *         to sstables for this table once the future completes
     */
    private ListenableFuture<CommitLogPosition> waitForFlushes()
    {
        // we grab the current memtable; once any preceding memtables have flushed, we know its
        // commitLogLowerBound has been set (as this it is set with the upper bound of the preceding memtable)
        final Memtable current = data.getView().getCurrentMemtable();
        ListenableFutureTask<CommitLogPosition> task = ListenableFutureTask.create(() -> {
            logger.debug("forceFlush requested but everything is clean in {}", name);
            return current.getCommitLogLowerBound();
        });
        postFlushExecutor.execute(task);
        return task;
    }

    /*
     * switchMemtable puts Memtable.getSortedContents on the writer executor.  When the write is complete,
     * we turn the writer into an SSTableReader and add it to ssTables where it is available for reads.
     * This method does not block except for synchronizing on Tracker, but the Future it returns will
     * not complete until the Memtable (and all prior Memtables) have been successfully flushed, and the CL
     * marked clean up to the position owned by the Memtable.
     */
    public ListenableFuture<CommitLogPosition> switchMemtable()
    {
        synchronized (data)
        {
            logFlush();
            Flush flush = new Flush(false);
            flushExecutor.execute(flush);
            postFlushExecutor.execute(flush.postFlushTask);
            return flush.postFlushTask;
        }
    }

    public Descriptor newSSTableDescriptor(File directory)
    {
        return newSSTableDescriptor(directory, SSTableFormat.Type.current().info.getLatestVersion(), SSTableFormat.Type.current());
    }

    public Descriptor newSSTableDescriptor(File directory, SSTableFormat.Type format)
    {
        return newSSTableDescriptor(directory, format.info.getLatestVersion(), format);
    }

    private Descriptor newSSTableDescriptor(File directory, Version version, SSTableFormat.Type format)
    {
        return new Descriptor(version,
                              directory,
                              keyspace.getName(),
                              name,
                              fileIndexGenerator.incrementAndGet(),
                              format);
    }

    /**
     * @param sstables
     * @return sstables whose key range overlaps with that of the given sstables, not including itself.
     * (The given sstables may or may not overlap with each other.)
     */
    public Collection<SSTableReader> getOverlappingLiveSSTables(Iterable<SSTableReader> sstables)
    {
        logger.trace("Checking for sstables overlapping {}", sstables);

        // a normal compaction won't ever have an empty sstables list, but we create a skeleton
        // compaction controller for streaming, and that passes an empty list.
        if (!sstables.iterator().hasNext())
            return ImmutableSet.of();

        View view = data.getView();

        List<SSTableReader> sortedByFirst = Lists.newArrayList(sstables);
        Collections.sort(sortedByFirst, (o1, o2) -> o1.first.compareTo(o2.first));

        List<AbstractBounds<PartitionPosition>> bounds = new ArrayList<>();
        DecoratedKey first = null, last = null;
        /*
        normalize the intervals covered by the sstables
        assume we have sstables like this (brackets representing first/last key in the sstable);
        [   ] [   ]    [   ]   [  ]
           [   ]         [       ]
        then we can, instead of searching the interval tree 6 times, normalize the intervals and
        only query the tree 2 times, for these intervals;
        [         ]    [          ]
         */
        for (SSTableReader sstable : sortedByFirst)
        {
            if (first == null)
            {
                first = sstable.first;
                last = sstable.last;
            }
            else
            {
                if (sstable.first.compareTo(last) <= 0) // we do overlap
                {
                    if (sstable.last.compareTo(last) > 0)
                        last = sstable.last;
                }
                else
                {
                    bounds.add(AbstractBounds.bounds(first, true, last, true));
                    first = sstable.first;
                    last = sstable.last;
                }
            }
        }
        bounds.add(AbstractBounds.bounds(first, true, last, true));
        Set<SSTableReader> results = new HashSet<>();

        for (AbstractBounds<PartitionPosition> bound : bounds)
            Iterables.addAll(results, view.liveSSTablesInBounds(bound.left, bound.right));

        return Sets.difference(results, ImmutableSet.copyOf(sstables));
    }

    /**
     * like getOverlappingSSTables, but acquires references before returning
     */
    public Refs<SSTableReader> getAndReferenceOverlappingLiveSSTables(Iterable<SSTableReader> sstables)
    {
        while (true)
        {
            Iterable<SSTableReader> overlapped = getOverlappingLiveSSTables(sstables);
            Refs<SSTableReader> refs = Refs.tryRef(overlapped);
            if (refs != null)
                return refs;
        }
    }

    /*
     * Called after a BinaryMemtable flushes its in-memory data, or we add a file
     * via bootstrap. This information is cached in the ColumnFamilyStore.
     * This is useful for reads because the ColumnFamilyStore first looks in
     * the in-memory store and the into the disk to find the key. If invoked
     * during recoveryMode the onMemtableFlush() need not be invoked.
     *
     * param @ filename - filename just flushed to disk
     */
    public void addSSTable(SSTableReader sstable)
    {
        assert sstable.getColumnFamilyName().equals(name);
        addSSTables(Collections.singletonList(sstable));
    }

    public void addSSTables(Collection<SSTableReader> sstables)
    {
        data.addSSTables(sstables);
        CompactionManager.instance.submitBackground(cfs);
    }

    public Set<SSTableReader> getLiveSSTables()
    {
        return data.getView().liveSSTables();
    }

    public Iterable<SSTableReader> getSSTables(SSTableSet sstableSet)
    {
        return data.getView().select(sstableSet);
    }

    public Iterable<SSTableReader> getUncompactingSSTables()
    {
        return data.getUncompacting();
    }

    /**
     * Discard all SSTables that were created before given timestamp.
     *
     * Caller should first ensure that comapctions have quiesced.
     *
     * @param truncatedAt The timestamp of the truncation
     *                    (all SSTables before that timestamp are going be marked as compacted)
     */
    public void discardSSTables(long truncatedAt)
    {
        assert data.getCompacting().isEmpty() : data.getCompacting();

        List<SSTableReader> truncatedSSTables = new ArrayList<>();

        for (SSTableReader sstable : getSSTables(SSTableSet.LIVE))
        {
            if (!sstable.newSince(truncatedAt))
                truncatedSSTables.add(sstable);
        }

        if (!truncatedSSTables.isEmpty())
            markObsolete(truncatedSSTables, OperationType.UNKNOWN);
    }

    public void markObsolete(Collection<SSTableReader> sstables, OperationType compactionType)
    {
        assert !sstables.isEmpty();
        maybeFail(data.dropSSTables(Predicates.in(sstables), compactionType, null));
    }

    void replaceFlushed(Memtable memtable, Collection<SSTableReader> sstables)
    {
        data.replaceFlushed(memtable, sstables);
        if (sstables != null && !sstables.isEmpty())
            CompactionManager.instance.submitBackground(cfs);
    }

    /**
     * @param ephemeral If this flag is set to true, the snapshot will be cleaned during next startup
     */
    public Set<SSTableReader> snapshotWithoutFlush(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral)
    {
        Set<SSTableReader> snapshottedSSTables = new HashSet<>();
        for (ColumnFamilyStore cfs : cfs.concatWithIndexes())
        {
            final JSONArray filesJSONArr = new JSONArray();
            try (ColumnFamilyStore.RefViewFragment currentView = cfs.getStorageHandler().selectAndReference(View.select(SSTableSet.CANONICAL, (x) -> predicate == null || predicate.apply(x))))
            {
                for (SSTableReader ssTable : currentView.sstables)
                {
                    File snapshotDirectory = Directories.getSnapshotDirectory(ssTable.descriptor, snapshotName);
                    ssTable.createLinks(snapshotDirectory.getPath()); // hard links
                    filesJSONArr.add(ssTable.descriptor.relativeFilenameFor(Component.DATA));

                    if (logger.isTraceEnabled())
                        logger.trace("Snapshot for {} keyspace data file {} created in {}", keyspace, ssTable.getFilename(), snapshotDirectory);
                    snapshottedSSTables.add(ssTable);
                }

                writeSnapshotManifest(filesJSONArr, snapshotName);
                if (!SchemaConstants.isLocalSystemKeyspace(metadata.keyspace) && !SchemaConstants.isReplicatedSystemKeyspace(metadata.keyspace))
                    writeSnapshotSchema(snapshotName);
            }
        }
        if (ephemeral)
            createEphemeralSnapshotMarkerFile(snapshotName);
        return snapshottedSSTables;
    }

    private void writeSnapshotManifest(final JSONArray filesJSONArr, final String snapshotName)
    {
        final File manifestFile = getDirectories().getSnapshotManifestFile(snapshotName);

        try
        {
            if (!manifestFile.getParentFile().exists())
                manifestFile.getParentFile().mkdirs();

            try (PrintStream out = new PrintStream(manifestFile))
            {
                final JSONObject manifestJSON = new JSONObject();
                manifestJSON.put("files", filesJSONArr);
                out.println(manifestJSON.toJSONString());
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, manifestFile);
        }
    }

    private void writeSnapshotSchema(final String snapshotName)
    {
        final File schemaFile = getDirectories().getSnapshotSchemaFile(snapshotName);

        try
        {
            if (!schemaFile.getParentFile().exists())
                schemaFile.getParentFile().mkdirs();

            try (PrintStream out = new PrintStream(schemaFile))
            {
                for (String s: ColumnFamilyStoreCQLHelper.dumpReCreateStatements(metadata()))
                    out.println(s);
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, schemaFile);
        }
    }

    private void createEphemeralSnapshotMarkerFile(final String snapshot)
    {
        final File ephemeralSnapshotMarker = getDirectories().getNewEphemeralSnapshotMarkerFile(snapshot);

        try
        {
            if (!ephemeralSnapshotMarker.getParentFile().exists())
                ephemeralSnapshotMarker.getParentFile().mkdirs();

            Files.createFile(ephemeralSnapshotMarker.toPath());
            logger.trace("Created ephemeral snapshot marker file on {}.", ephemeralSnapshotMarker.getAbsolutePath());
        }
        catch (IOException e)
        {
            logger.warn(String.format("Could not create marker file %s for ephemeral snapshot %s. " +
                                      "In case there is a failure in the operation that created " +
                                      "this snapshot, you may need to clean it manually afterwards.",
                                      ephemeralSnapshotMarker.getAbsolutePath(), snapshot), e);
        }
    }

    public Refs<SSTableReader> getSnapshotSSTableReaders(String tag)
    {
        Map<Integer, SSTableReader> active = new HashMap<>();
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
            active.put(sstable.descriptor.generation, sstable);
        Map<Descriptor, Set<Component>> snapshots = getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots(tag).list();
        Refs<SSTableReader> refs = new Refs<>();
        try
        {
            for (Map.Entry<Descriptor, Set<Component>> entries : snapshots.entrySet())
            {
                // Try acquire reference to an active sstable instead of snapshot if it exists,
                // to avoid opening new sstables. If it fails, use the snapshot reference instead.
                SSTableReader sstable = active.get(entries.getKey().generation);
                if (sstable == null || !refs.tryRef(sstable))
                {
                    if (logger.isTraceEnabled())
                        logger.trace("using snapshot sstable {}", entries.getKey());
                    // open without tracking hotness
                    sstable = SSTableReader.open(entries.getKey(), entries.getValue(), metadata, true, false);
                    refs.tryRef(sstable);
                    // release the self ref as we never add the snapshot sstable to DataTracker where it is otherwise released
                    sstable.selfRef().release();
                }
                else if (logger.isTraceEnabled())
                {
                    logger.trace("using active sstable {}", entries.getKey());
                }
            }
        }
        catch (FSReadError | RuntimeException e)
        {
            // In case one of the snapshot sstables fails to open,
            // we must release the references to the ones we opened so far
            refs.release();
            throw e;
        }
        return refs;
    }

    public List<String> getSSTablesForKey(String key, boolean hexFormat)
    {
        ByteBuffer keyBuffer = hexFormat ? ByteBufferUtil.hexToBytes(key) : metadata().partitionKeyType.fromString(key);
        DecoratedKey dk = cfs.decorateKey(keyBuffer);
        try (OpOrder.Group op = cfs.readOrdering.start())
        {
            List<String> files = new ArrayList<>();
            for (SSTableReader sstr : select(View.select(SSTableSet.LIVE, dk)).sstables)
            {
                // check if the key actually exists in this sstable, without updating cache and stats
                if (sstr.getPosition(dk, SSTableReader.Operator.EQ, false) != null)
                    files.add(sstr.getFilename());
            }
            return files;
        }
    }

    @SuppressWarnings("resource")
    public ColumnFamilyStore.RefViewFragment selectAndReference(Function<View, Iterable<SSTableReader>> filter)
    {
        long failingSince = -1L;
        while (true)
        {
            ColumnFamilyStore.ViewFragment view = select(filter);
            Refs<SSTableReader> refs = Refs.tryRef(view.sstables);
            if (refs != null)
                return new ColumnFamilyStore.RefViewFragment(view.sstables, view.memtables, refs);
            if (failingSince <= 0)
            {
                failingSince = System.nanoTime();
            }
            else if (System.nanoTime() - failingSince > TimeUnit.MILLISECONDS.toNanos(100))
            {
                List<SSTableReader> released = new ArrayList<>();
                for (SSTableReader reader : view.sstables)
                    if (reader.selfRef().globalCount() == 0)
                        released.add(reader);
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS,
                                 "Spinning trying to capture readers {}, released: {}, ", view.sstables, released);
                failingSince = System.nanoTime();
            }
        }
    }

    public ColumnFamilyStore.ViewFragment select(Function<View, Iterable<SSTableReader>> filter)
    {
        View view = data.getView();
        List<SSTableReader> sstables = Lists.newArrayList(filter.apply(view));
        return new ColumnFamilyStore.ViewFragment(sstables, view.getAllMemtables());
    }

    /**
     * For testing.  No effort is made to clear historical or even the current memtables, nor for
     * thread safety.  All we do is wipe the sstable containers clean, while leaving the actual
     * data files present on disk.  (This allows tests to easily call refresh on them.)
     */
    @VisibleForTesting
    public void clearUnsafe()
    {
        cfs.runWithCompactionsDisabled(new Callable<Void>()
        {
            public Void call()
            {
                data.reset(new Memtable(new AtomicReference<>(CommitLogPosition.NONE), cfs));
                return null;
            }
        }, true, false);
    }

    public CommitLogPosition forceBlockingFlush()
    {
        return FBUtilities.waitOnFuture(forceFlush());
    }

    /**
     * Truncate deletes the entire column family's data with no expensive tombstone creation
     */
    public void truncateBlocking()
    {
        // We have two goals here:
        // - truncate should delete everything written before truncate was invoked
        // - but not delete anything that isn't part of the snapshot we create.
        // We accomplish this by first flushing manually, then snapshotting, and
        // recording the timestamp IN BETWEEN those actions. Any sstables created
        // with this timestamp or greater time, will not be marked for delete.
        //
        // Bonus complication: since we store commit log segment position in sstable metadata,
        // truncating those sstables means we will replay any CL segments from the
        // beginning if we restart before they [the CL segments] are discarded for
        // normal reasons post-truncate.  To prevent this, we store truncation
        // position in the System keyspace.
        logger.info("Truncating {}.{}", keyspace.getName(), name);

        final long truncatedAt;
        final CommitLogPosition replayAfter;

        if (keyspace.getMetadata().params.durableWrites || DatabaseDescriptor.isAutoSnapshot())
        {
            replayAfter = forceBlockingFlush();
            cfs.viewManager.forceBlockingFlush();
        }
        else
        {
            // just nuke the memtable data w/o writing to disk first
            cfs.viewManager.dumpMemtables();
            try
            {
                replayAfter = dumpMemtable().get();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        long now = System.currentTimeMillis();
        // make sure none of our sstables are somehow in the future (clock drift, perhaps)
        for (ColumnFamilyStore cfs : cfs.concatWithIndexes())
            for (SSTableReader sstable : cfs.getLiveSSTables())
                now = Math.max(now, sstable.maxDataAge);
        truncatedAt = now;

        Runnable truncateRunnable = new Runnable()
        {
            public void run()
            {
                logger.debug("Discarding sstable data for truncated CF + indexes");
                data.notifyTruncated(truncatedAt);

                if (DatabaseDescriptor.isAutoSnapshot())
                    cfs.snapshot(Keyspace.getTimestampedSnapshotNameWithPrefix(name, cfs.SNAPSHOT_TRUNCATE_PREFIX));

                discardSSTables(truncatedAt);

                cfs.indexManager.truncateAllIndexesBlocking(truncatedAt);
                cfs.viewManager.truncateBlocking(replayAfter, truncatedAt);

                SystemKeyspace.saveTruncationRecord(cfs, truncatedAt, replayAfter);
                logger.trace("cleaning out row cache");
                cfs.invalidateCaches();
            }
        };

        cfs.runWithCompactionsDisabled(Executors.callable(truncateRunnable), true, true);
    }

    /**
     * Flush if there is unflushed data in the memtables
     *
     * @return a Future yielding the commit log position that can be guaranteed to have been successfully written
     *         to sstables for this table once the future completes
     */
    public ListenableFuture<CommitLogPosition> forceFlush()
    {
        synchronized (data)
        {
            Memtable current = data.getView().getCurrentMemtable();
            for (ColumnFamilyStore cfs : cfs.concatWithIndexes())
                if (!cfs.getStorageHandler().getTracker().getView().getCurrentMemtable().isClean())
                    return switchMemtableIfCurrent(current);
            return waitForFlushes();
        }
    }

    /**
     * Flush if there is unflushed data that was written to the CommitLog before @param flushIfDirtyBefore
     * (inclusive).
     *
     * @return a Future yielding the commit log position that can be guaranteed to have been successfully written
     *         to sstables for this table once the future completes
     */
    public ListenableFuture<?> forceFlush(CommitLogPosition flushIfDirtyBefore)
    {
        // we don't loop through the remaining memtables since here we only care about commit log dirtiness
        // and this does not vary between a table and its table-backed indexes
        Memtable current = data.getView().getCurrentMemtable();
        if (current.mayContainDataBefore(flushIfDirtyBefore))
            return switchMemtableIfCurrent(current);
        return waitForFlushes();
    }

    // print out size of all memtables we're enqueuing
    private void logFlush()
    {
        // reclaiming includes that which we are GC-ing;
        float onHeapRatio = 0, offHeapRatio = 0;
        long onHeapTotal = 0, offHeapTotal = 0;
        Memtable memtable = getTracker().getView().getCurrentMemtable();
        onHeapRatio +=  memtable.getAllocator().onHeap().ownershipRatio();
        offHeapRatio += memtable.getAllocator().offHeap().ownershipRatio();
        onHeapTotal += memtable.getAllocator().onHeap().owns();
        offHeapTotal += memtable.getAllocator().offHeap().owns();

        for (ColumnFamilyStore indexCfs : cfs.indexManager.getAllIndexColumnFamilyStores())
        {
            MemtableAllocator allocator = indexCfs.getStorageHandler().getTracker().getView().getCurrentMemtable().getAllocator();
            onHeapRatio += allocator.onHeap().ownershipRatio();
            offHeapRatio += allocator.offHeap().ownershipRatio();
            onHeapTotal += allocator.onHeap().owns();
            offHeapTotal += allocator.offHeap().owns();
        }

        logger.debug("Enqueuing flush of {}: {}",
                     name,
                     String.format("%s (%.0f%%) on-heap, %s (%.0f%%) off-heap",
                                   FBUtilities.prettyPrintMemory(onHeapTotal),
                                   onHeapRatio * 100,
                                   FBUtilities.prettyPrintMemory(offHeapTotal),
                                   offHeapRatio * 100));
    }

    /**
     * Should only be constructed/used from switchMemtable() or truncate(), with ownership of the Tracker monitor.
     * In the constructor the current memtable(s) are swapped, and a barrier on outstanding writes is issued;
     * when run by the flushWriter the barrier is waited on to ensure all outstanding writes have completed
     * before all memtables are immediately written, and the CL is either immediately marked clean or, if
     * there are custom secondary indexes, the post flush clean up is left to update those indexes and mark
     * the CL clean
     */
    private final class Flush implements Runnable
    {
        final OpOrder.Barrier writeBarrier;
        final List<Memtable> memtables = new ArrayList<>();
        final ListenableFutureTask<CommitLogPosition> postFlushTask;
        final PostFlush postFlush;
        final boolean truncate;

        private Flush(boolean truncate)
        {
            // if true, we won't flush, we'll just wait for any outstanding writes, switch the memtable, and discard
            this.truncate = truncate;

            cfs.metric.pendingFlushes.inc();
            /**
             * To ensure correctness of switch without blocking writes, run() needs to wait for all write operations
             * started prior to the switch to complete. We do this by creating a Barrier on the writeOrdering
             * that all write operations register themselves with, and assigning this barrier to the memtables,
             * after which we *.issue()* the barrier. This barrier is used to direct write operations started prior
             * to the barrier.issue() into the memtable we have switched out, and any started after to its replacement.
             * In doing so it also tells the write operations to update the commitLogUpperBound of the memtable, so
             * that we know the CL position we are dirty to, which can be marked clean when we complete.
             */
            writeBarrier = keyspace.writeOrder.newBarrier();

            // submit flushes for the memtable for any indexed sub-cfses, and our own
            AtomicReference<CommitLogPosition> commitLogUpperBound = new AtomicReference<>();
            for (ColumnFamilyStore cfs : cfs.concatWithIndexes())
            {
                // switch all memtables, regardless of their dirty status, setting the barrier
                // so that we can reach a coordinated decision about cleanliness once they
                // are no longer possible to be modified
                Memtable newMemtable = new Memtable(commitLogUpperBound, cfs);
                Memtable oldMemtable = cfs.getStorageHandler().getTracker().switchMemtable(truncate, newMemtable);
                oldMemtable.setDiscarding(writeBarrier, commitLogUpperBound);
                memtables.add(oldMemtable);
            }

            // we then ensure an atomic decision is made about the upper bound of the continuous range of commit log
            // records owned by this memtable
            setCommitLogUpperBound(commitLogUpperBound);

            // we then issue the barrier; this lets us wait for all operations started prior to the barrier to complete;
            // since this happens after wiring up the commitLogUpperBound, we also know all operations with earlier
            // commit log segment position have also completed, i.e. the memtables are done and ready to flush
            writeBarrier.issue();
            postFlush = new PostFlush(memtables);
            postFlushTask = ListenableFutureTask.create(postFlush);
        }

        public void run()
        {
            // mark writes older than the barrier as blocking progress, permitting them to exceed our memory limit
            // if they are stuck waiting on it, then wait for them all to complete
            writeBarrier.markBlocking();
            writeBarrier.await();

            // mark all memtables as flushing, removing them from the live memtable list
            for (Memtable memtable : memtables)
                memtable.cfs.getStorageHandler().getTracker().markFlushing(memtable);

            cfs.metric.memtableSwitchCount.inc();

            try
            {
                // Flush "data" memtable with non-cf 2i first;
                flushMemtable(memtables.get(0), true);
                for (int i = 1; i < memtables.size(); i++)
                    flushMemtable(memtables.get(i), false);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                postFlush.flushFailure = t;
            }
            // signal the post-flush we've done our work
            postFlush.latch.countDown();
        }

        public Collection<SSTableReader> flushMemtable(Memtable memtable, boolean flushNonCf2i)
        {
            if (memtable.isClean() || truncate)
            {
                memtable.cfs.getStorageHandler().replaceFlushed(memtable, Collections.emptyList());
                reclaim(memtable);
                return Collections.emptyList();
            }

            List<Future<SSTableMultiWriter>> futures = new ArrayList<>();
            long totalBytesOnDisk = 0;
            long maxBytesOnDisk = 0;
            long minBytesOnDisk = Long.MAX_VALUE;
            List<SSTableReader> sstables = new ArrayList<>();
            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH))
            {
                List<Memtable.FlushRunnable> flushRunnables = null;
                List<SSTableMultiWriter> flushResults = null;

                try
                {
                    // flush the memtable
                    flushRunnables = memtable.flushRunnables(txn);

                    for (int i = 0; i < flushRunnables.size(); i++)
                        futures.add(perDiskflushExecutors[i].submit(flushRunnables.get(i)));

                    /**
                     * we can flush 2is as soon as the barrier completes, as they will be consistent with (or ahead of) the
                     * flushed memtables and CL position, which is as good as we can guarantee.
                     * TODO: SecondaryIndex should support setBarrier(), so custom implementations can co-ordinate exactly
                     * with CL as we do with memtables/CFS-backed SecondaryIndexes.
                     */
                    if (flushNonCf2i)
                        cfs.indexManager.flushAllNonCFSBackedIndexesBlocking();

                    flushResults = Lists.newArrayList(FBUtilities.waitOnFutures(futures));
                }
                catch (Throwable t)
                {
                    t = memtable.abortRunnables(flushRunnables, t);
                    t = txn.abort(t);
                    throw Throwables.propagate(t);
                }

                try
                {
                    Iterator<SSTableMultiWriter> writerIterator = flushResults.iterator();
                    while (writerIterator.hasNext())
                    {
                        @SuppressWarnings("resource")
                        SSTableMultiWriter writer = writerIterator.next();
                        if (writer.getFilePointer() > 0)
                        {
                            writer.setOpenResult(true).prepareToCommit();
                        }
                        else
                        {
                            maybeFail(writer.abort(null));
                            writerIterator.remove();
                        }
                    }
                }
                catch (Throwable t)
                {
                    for (SSTableMultiWriter writer : flushResults)
                        t = writer.abort(t);
                    t = txn.abort(t);
                    Throwables.propagate(t);
                }

                txn.prepareToCommit();

                Throwable accumulate = null;
                for (SSTableMultiWriter writer : flushResults)
                    accumulate = writer.commit(accumulate);

                maybeFail(txn.commit(accumulate));

                for (SSTableMultiWriter writer : flushResults)
                {
                    Collection<SSTableReader> flushedSSTables = writer.finished();
                    for (SSTableReader sstable : flushedSSTables)
                    {
                        if (sstable != null)
                        {
                            sstables.add(sstable);
                            long size = sstable.bytesOnDisk();
                            totalBytesOnDisk += size;
                            maxBytesOnDisk = Math.max(maxBytesOnDisk, size);
                            minBytesOnDisk = Math.min(minBytesOnDisk, size);
                        }
                    }
                }
            }
            memtable.cfs.getStorageHandler().replaceFlushed(memtable, sstables);
            reclaim(memtable);
            memtable.cfs.compactionStrategyManager.compactionLogger.flush(sstables);
            logger.debug("Flushed to {} ({} sstables, {}), biggest {}, smallest {}",
                         sstables,
                         sstables.size(),
                         FBUtilities.prettyPrintMemory(totalBytesOnDisk),
                         FBUtilities.prettyPrintMemory(maxBytesOnDisk),
                         FBUtilities.prettyPrintMemory(minBytesOnDisk));
            return sstables;
        }

        private void reclaim(final Memtable memtable)
        {
            // issue a read barrier for reclaiming the memory, and offload the wait to another thread
            final OpOrder.Barrier readBarrier = cfs.readOrdering.newBarrier();
            readBarrier.issue();
            postFlushTask.addListener(new WrappedRunnable()
            {
                public void runMayThrow()
                {
                    readBarrier.await();
                    memtable.setDiscarded();
                }
            }, reclaimExecutor);
        }
    }

    /**
     * Both synchronises custom secondary indexes and provides ordering guarantees for futures on switchMemtable/flush
     * etc, which expect to be able to wait until the flush (and all prior flushes) requested have completed.
     */
    private final class PostFlush implements Callable<CommitLogPosition>
    {
        final CountDownLatch latch = new CountDownLatch(1);
        final List<Memtable> memtables;
        volatile Throwable flushFailure = null;

        private PostFlush(List<Memtable> memtables)
        {
            this.memtables = memtables;
        }

        public CommitLogPosition call()
        {
            try
            {
                // we wait on the latch for the commitLogUpperBound to be set, and so that waiters
                // on this task can rely on all prior flushes being complete
                latch.await();
            }
            catch (InterruptedException e)
            {
                throw new IllegalStateException();
            }

            CommitLogPosition commitLogUpperBound = CommitLogPosition.NONE;
            // If a flush errored out but the error was ignored, make sure we don't discard the commit log.
            if (flushFailure == null && !memtables.isEmpty())
            {
                Memtable memtable = memtables.get(0);
                commitLogUpperBound = memtable.getCommitLogUpperBound();
                CommitLog.instance.discardCompletedSegments(metadata.id, memtable.getCommitLogLowerBound(), commitLogUpperBound);
            }

            cfs.metric.pendingFlushes.dec();

            if (flushFailure != null)
                throw Throwables.propagate(flushFailure);

            return commitLogUpperBound;
        }
    }

    // atomically set the upper bound for the commit log
    private static void setCommitLogUpperBound(AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        // we attempt to set the holder to the current commit log context. at the same time all writes to the memtables are
        // also maintaining this value, so if somebody sneaks ahead of us somehow (should be rare) we simply retry,
        // so that we know all operations prior to the position have not reached it yet
        CommitLogPosition lastReplayPosition;
        while (true)
        {
            lastReplayPosition = new Memtable.LastCommitLogPosition((CommitLog.instance.getCurrentPosition()));
            CommitLogPosition currentLast = commitLogUpperBound.get();
            if ((currentLast == null || currentLast.compareTo(lastReplayPosition) <= 0)
                && commitLogUpperBound.compareAndSet(currentLast, lastReplayPosition))
                break;
        }
    }

    /**
     * Finds the largest memtable, as a percentage of *either* on- or off-heap memory limits, and immediately
     * queues it for flushing. If the memtable selected is flushed before this completes, no work is done.
     */
    public static class FlushLargestColumnFamily implements Runnable
    {
        public void run()
        {
            float largestRatio = 0f;
            Memtable largest = null;
            float liveOnHeap = 0, liveOffHeap = 0;
            for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            {
                // we take a reference to the current main memtable for the CF prior to snapping its ownership ratios
                // to ensure we have some ordering guarantee for performing the switchMemtableIf(), i.e. we will only
                // swap if the memtables we are measuring here haven't already been swapped by the time we try to swap them
                Memtable current = cfs.getStorageHandler().getTracker().getView().getCurrentMemtable();

                // find the total ownership ratio for the memtable and all SecondaryIndexes owned by this CF,
                // both on- and off-heap, and select the largest of the two ratios to weight this CF
                float onHeap = 0f, offHeap = 0f;
                onHeap += current.getAllocator().onHeap().ownershipRatio();
                offHeap += current.getAllocator().offHeap().ownershipRatio();

                for (ColumnFamilyStore indexCfs : cfs.indexManager.getAllIndexColumnFamilyStores())
                {
                    MemtableAllocator allocator = indexCfs.getStorageHandler().getTracker().getView().getCurrentMemtable().getAllocator();
                    onHeap += allocator.onHeap().ownershipRatio();
                    offHeap += allocator.offHeap().ownershipRatio();
                }

                float ratio = Math.max(onHeap, offHeap);
                if (ratio > largestRatio)
                {
                    largest = current;
                    largestRatio = ratio;
                }

                liveOnHeap += onHeap;
                liveOffHeap += offHeap;
            }

            if (largest != null)
            {
                float usedOnHeap = Memtable.MEMORY_POOL.onHeap.usedRatio();
                float usedOffHeap = Memtable.MEMORY_POOL.offHeap.usedRatio();
                float flushingOnHeap = Memtable.MEMORY_POOL.onHeap.reclaimingRatio();
                float flushingOffHeap = Memtable.MEMORY_POOL.offHeap.reclaimingRatio();
                float thisOnHeap = largest.getAllocator().onHeap().ownershipRatio();
                float thisOffHeap = largest.getAllocator().offHeap().ownershipRatio();
                logger.debug("Flushing largest {} to free up room. Used total: {}, live: {}, flushing: {}, this: {}",
                             largest.cfs, ratio(usedOnHeap, usedOffHeap), ratio(liveOnHeap, liveOffHeap),
                             ratio(flushingOnHeap, flushingOffHeap), ratio(thisOnHeap, thisOffHeap));
                largest.cfs.getStorageHandler().switchMemtableIfCurrent(largest);
            }
        }
    }

    private static String ratio(float onHeap, float offHeap)
    {
        return String.format("%.2f/%.2f", onHeap, offHeap);
    }

    public void refresh()
    {
        logger.info("Loading new SSTables for {}/{}...", keyspace.getName(), name);

        Set<Descriptor> currentDescriptors = new HashSet<>();
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
            currentDescriptors.add(sstable.descriptor);
        Set<SSTableReader> newSSTables = new HashSet<>();

        Directories.SSTableLister lister = getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true);
        for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
        {
            Descriptor descriptor = entry.getKey();

            if (currentDescriptors.contains(descriptor))
                continue; // old (initialized) SSTable found, skipping

            if (!descriptor.isCompatible())
                throw new RuntimeException(String.format("Can't open incompatible SSTable! Current version %s, found file: %s",
                                                         descriptor.getFormat().getLatestVersion(),
                                                         descriptor));

            // force foreign sstables to level 0
            try
            {
                if (new File(descriptor.filenameFor(Component.STATS)).exists())
                    descriptor.getMetadataSerializer().mutateLevel(descriptor, 0);
            }
            catch (IOException e)
            {
                FileUtils.handleCorruptSSTable(new CorruptSSTableException(e, entry.getKey().filenameFor(Component.STATS)));
                logger.error("Cannot read sstable {}; other IO error, skipping table", entry, e);
                continue;
            }

            // Increment the generation until we find a filename that doesn't exist. This is needed because the new
            // SSTables that are being loaded might already use these generation numbers.
            Descriptor newDescriptor;
            do
            {
                newDescriptor = new Descriptor(descriptor.version,
                                               descriptor.directory,
                                               descriptor.ksname,
                                               descriptor.cfname,
                                               fileIndexGenerator.incrementAndGet(),
                                               descriptor.formatType);
            }
            while (new File(newDescriptor.filenameFor(Component.DATA)).exists());

            logger.info("Renaming new SSTable {} to {}", descriptor, newDescriptor);
            SSTableWriter.rename(descriptor, newDescriptor, entry.getValue());

            SSTableReader reader;
            try
            {
                reader = SSTableReader.open(newDescriptor, entry.getValue(), metadata);
            }
            catch (CorruptSSTableException ex)
            {
                FileUtils.handleCorruptSSTable(ex);
                logger.error("Corrupt sstable {}; skipping table", entry, ex);
                continue;
            }
            catch (FSError ex)
            {
                FileUtils.handleFSError(ex);
                logger.error("Cannot read sstable {}; file system error, skipping table", entry, ex);
                continue;
            }
            newSSTables.add(reader);
        }

        if (newSSTables.isEmpty())
        {
            logger.info("No new SSTables were found for {}/{}", keyspace.getName(), name);
            return;
        }

        logger.info("Loading new SSTables and building secondary indexes for {}/{}: {}", keyspace.getName(), name, newSSTables);

        try (Refs<SSTableReader> refs = Refs.ref(newSSTables))
        {
            data.addSSTables(newSSTables);
        }

        logger.info("Done loading load new SSTables for {}/{}", keyspace.getName(), name);
    }

    /**
     * Drops current memtable without flushing to disk. This should only be called when truncating a column family which is not durable.
     */
    public Future<CommitLogPosition> dumpMemtable()
    {
        synchronized (data)
        {
            final Flush flush = new Flush(true);
            flushExecutor.execute(flush);
            postFlushExecutor.execute(flush.postFlushTask);
            return flush.postFlushTask;
        }
    }

    public LifecycleTransaction markAllCompacting(final OperationType operationType)
    {
        Callable<LifecycleTransaction> callable = new Callable<LifecycleTransaction>()
        {
            public LifecycleTransaction call()
            {
                assert data.getCompacting().isEmpty() : data.getCompacting();
                Iterable<SSTableReader> sstables = getLiveSSTables();
                sstables = AbstractCompactionStrategy.filterSuspectSSTables(sstables);
                sstables = ImmutableList.copyOf(sstables);
                LifecycleTransaction modifier = data.tryModify(sstables, operationType);
                assert modifier != null: "something marked things compacting while compactions are disabled";
                return modifier;
            }
        };

        return cfs.runWithCompactionsDisabled(callable, false, false);
    }

    public int getMeanColumns()
    {
        long sum = 0;
        long count = 0;
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
        {
            long n = sstable.getEstimatedColumnCount().count();
            sum += sstable.getEstimatedColumnCount().mean() * n;
            count += n;
        }
        return count > 0 ? (int) (sum / count) : 0;
    }

    public double getMeanPartitionSize()
    {
        long sum = 0;
        long count = 0;
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
        {
            long n = sstable.getEstimatedPartitionSize().count();
            sum += sstable.getEstimatedPartitionSize().mean() * n;
            count += n;
        }
        return count > 0 ? sum * 1.0 / count : 0;
    }

    public long estimateKeys()
    {
        long n = 0;
        for (SSTableReader sstable : getSSTables(SSTableSet.CANONICAL))
            n += sstable.estimatedKeys();
        return n;
    }

    public double getDroppableTombstoneRatio()
    {
        double allDroppable = 0;
        long allColumns = 0;
        int localTime = (int)(System.currentTimeMillis()/1000);

        for (SSTableReader sstable : getSSTables(SSTableSet.LIVE))
        {
            allDroppable += sstable.getDroppableTombstonesBefore(localTime - metadata().params.gcGraceSeconds);
            allColumns += sstable.getEstimatedColumnCount().mean() * sstable.getEstimatedColumnCount().count();
        }
        return allColumns > 0 ? allDroppable / allColumns : 0;
    }

    public void setCrcCheckChance(double crcCheckChance)
    {
        try
        {
            TableParams.builder().crcCheckChance(crcCheckChance).build().validate();
            for (ColumnFamilyStore cfs : cfs.concatWithIndexes())
            {
                cfs.crcCheckChance.set(crcCheckChance);
                for (SSTableReader sstable : cfs.getStorageHandler().getSSTables(SSTableSet.LIVE))
                    sstable.setCrcCheckChance(crcCheckChance);
            }
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public boolean isEmpty()
    {
        return data.getView().isEmpty();
    }

    public Iterable<DecoratedKey> keySamples(Range<Token> range)
    {
        try (ColumnFamilyStore.RefViewFragment view = selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            Iterable<DecoratedKey>[] samples = new Iterable[view.sstables.size()];
            int i = 0;
            for (SSTableReader sstable: view.sstables)
            {
                samples[i++] = sstable.getKeySamples(range);
            }
            return Iterables.concat(samples);
        }
    }

    public long estimatedKeysForRange(Range<Token> range)
    {
        try (ColumnFamilyStore.RefViewFragment view = selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            long count = 0;
            for (SSTableReader sstable : view.sstables)
                count += sstable.estimatedKeysForRanges(Collections.singleton(range));
            return count;
        }
    }
}
