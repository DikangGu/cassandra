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
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.management.*;
import javax.management.openmbean.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.*;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.Counter;
import org.apache.cassandra.cache.*;
import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.view.TableViews;
import org.apache.cassandra.db.lifecycle.*;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.*;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.TableMetrics.Sampler;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.TopKSampler.SamplerResult;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import static org.apache.cassandra.utils.Throwables.maybeFail;

public class ColumnFamilyStore implements ColumnFamilyStoreMBean
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyStore.class);

    private static final String[] COUNTER_NAMES = new String[]{"raw", "count", "error", "string"};
    private static final String[] COUNTER_DESCS = new String[]
    { "partition key in raw hex bytes",
      "value of this partition for given sampler",
      "value is within the error bounds plus or minus of this",
      "the partition key turned into a human readable format" };
    private static final CompositeType COUNTER_COMPOSITE_TYPE;
    private static final TabularType COUNTER_TYPE;

    private static final String[] SAMPLER_NAMES = new String[]{"cardinality", "partitions"};
    private static final String[] SAMPLER_DESCS = new String[]
    { "cardinality of partitions",
      "list of counter results" };

    private static final String SAMPLING_RESULTS_NAME = "SAMPLING_RESULTS";
    private static final CompositeType SAMPLING_RESULT;

    public static final String SNAPSHOT_TRUNCATE_PREFIX = "truncated";
    public static final String SNAPSHOT_DROP_PREFIX = "dropped";

    static
    {
        try
        {
            OpenType<?>[] counterTypes = new OpenType[] { SimpleType.STRING, SimpleType.LONG, SimpleType.LONG, SimpleType.STRING };
            COUNTER_COMPOSITE_TYPE = new CompositeType(SAMPLING_RESULTS_NAME, SAMPLING_RESULTS_NAME, COUNTER_NAMES, COUNTER_DESCS, counterTypes);
            COUNTER_TYPE = new TabularType(SAMPLING_RESULTS_NAME, SAMPLING_RESULTS_NAME, COUNTER_COMPOSITE_TYPE, COUNTER_NAMES);

            OpenType<?>[] samplerTypes = new OpenType[] { SimpleType.LONG, COUNTER_TYPE };
            SAMPLING_RESULT = new CompositeType(SAMPLING_RESULTS_NAME, SAMPLING_RESULTS_NAME, SAMPLER_NAMES, SAMPLER_DESCS, samplerTypes);
        } catch (OpenDataException e)
        {
            throw Throwables.propagate(e);
        }
    }

    public final Keyspace keyspace;
    public final String name;
    public final TableMetadataRef metadata;
    private final String mbeanName;
    @Deprecated
    private final String oldMBeanName;
    private volatile boolean valid = true;

    /* The read order, used to track accesses to off-heap memtable storage */
    public final OpOrder readOrdering = new OpOrder();

    public final SecondaryIndexManager indexManager;
    public final TableViews viewManager;

    /* These are locally held copies to be changed from the config during runtime */
    private volatile DefaultValue<Integer> minCompactionThreshold;
    private volatile DefaultValue<Integer> maxCompactionThreshold;
    protected volatile DefaultValue<Double> crcCheckChance;

    protected final CompactionStrategyManager compactionStrategyManager;

    private final Directories directories;

    public final TableMetrics metric;
    public volatile long sampleLatencyNanos;
    private final ScheduledFuture<?> latencyCalculator;

    private volatile boolean compactionSpaceCheck = true;

    @VisibleForTesting
    final DiskBoundaryManager diskBoundaryManager = new DiskBoundaryManager();

    private volatile boolean neverPurgeTombstones = false;

    private final CassandraStorageHandler storageHandler;

    public static void drain() throws InterruptedException
    {
        CassandraStorageHandler.shutdownPostFlushExecutor();
    }

    public void reload()
    {
        // metadata object has been mutated directly. make all the members jibe with new settings.

        // only update these runtime-modifiable settings if they have not been modified.
        if (!minCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.minCompactionThreshold = new DefaultValue(metadata().params.compaction.minCompactionThreshold());
        if (!maxCompactionThreshold.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.maxCompactionThreshold = new DefaultValue(metadata().params.compaction.maxCompactionThreshold());
        if (!crcCheckChance.isModified())
            for (ColumnFamilyStore cfs : concatWithIndexes())
                cfs.crcCheckChance = new DefaultValue(metadata().params.crcCheckChance);

        compactionStrategyManager.maybeReload(metadata());

        indexManager.reload();
        
        storageHandler.reload();
    }

    public CassandraStorageHandler getStorageHandler()
    {
        return storageHandler;
    }

    public static Runnable getBackgroundCompactionTaskSubmitter()
    {
        return new Runnable()
        {
            public void run()
            {
                for (Keyspace keyspace : Keyspace.all())
                    for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                        CompactionManager.instance.submitBackground(cfs);
            }
        };
    }

    public Map<String, String> getCompactionParameters()
    {
        return compactionStrategyManager.getCompactionParams().asMap();
    }

    public String getCompactionParametersJson()
    {
        return FBUtilities.json(getCompactionParameters());
    }

    public void setCompactionParameters(Map<String, String> options)
    {
        try
        {
            CompactionParams compactionParams = CompactionParams.fromMap(options);
            compactionParams.validate();
            compactionStrategyManager.setNewLocalCompactionStrategy(compactionParams);
        }
        catch (Throwable t)
        {
            logger.error("Could not set new local compaction strategy", t);
            // dont propagate the ConfigurationException over jmx, user will only see a ClassNotFoundException
            throw new IllegalArgumentException("Could not set new local compaction strategy: "+t.getMessage());
        }
    }

    public void setCompactionParametersJson(String options)
    {
        setCompactionParameters(FBUtilities.fromJsonMap(options));
    }

    public Map<String,String> getCompressionParameters()
    {
        return metadata().params.compression.asMap();
    }

    public String getCompressionParametersJson()
    {
        return FBUtilities.json(getCompressionParameters());
    }

    public void setCompressionParameters(Map<String,String> opts)
    {
        try
        {
            CompressionParams params = CompressionParams.fromMap(opts);
            params.validate();
            throw new UnsupportedOperationException(); // TODO FIXME CASSANDRA-12949
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public void setCompressionParametersJson(String options)
    {
        setCompressionParameters(FBUtilities.fromJsonMap(options));
    }

    @VisibleForTesting
    public ColumnFamilyStore(Keyspace keyspace,
                             String columnFamilyName,
                             int generation,
                             TableMetadataRef metadata,
                             Directories directories,
                             boolean loadSSTables,
                             boolean registerBookeeping,
                             boolean offline)
    {
        assert directories != null;
        assert metadata != null : "null metadata for " + keyspace + ":" + columnFamilyName;

        this.keyspace = keyspace;
        this.metadata = metadata;
        this.directories = directories;
        name = columnFamilyName;
        minCompactionThreshold = new DefaultValue<>(metadata.get().params.compaction.minCompactionThreshold());
        maxCompactionThreshold = new DefaultValue<>(metadata.get().params.compaction.maxCompactionThreshold());
        crcCheckChance = new DefaultValue<>(metadata.get().params.crcCheckChance);
        viewManager = keyspace.viewManager.forTable(metadata.id);
        metric = new TableMetrics(this);
        sampleLatencyNanos = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getReadRpcTimeout() / 2);

        logger.info("Initializing {}.{}", keyspace.getName(), name);

        storageHandler = new CassandraStorageHandler(this, generation, loadSSTables);

        // compaction strategy should be created after the CFS has been prepared
        compactionStrategyManager = new CompactionStrategyManager(this);

        if (maxCompactionThreshold.value() <= 0 || minCompactionThreshold.value() <=0)
        {
            logger.warn("Disabling compaction strategy by setting compaction thresholds to 0 is deprecated, set the compaction option 'enabled' to 'false' instead.");
            this.compactionStrategyManager.disable();
        }

        // create the private ColumnFamilyStores for the secondary column indexes
        indexManager = new SecondaryIndexManager(this);
        for (IndexMetadata info : metadata.get().indexes)
            indexManager.addIndex(info, true);

        if (registerBookeeping)
        {
            // register the mbean
            mbeanName = String.format("org.apache.cassandra.db:type=%s,keyspace=%s,table=%s",
                                         isIndex() ? "IndexTables" : "Tables",
                                         keyspace.getName(), name);
            oldMBeanName = String.format("org.apache.cassandra.db:type=%s,keyspace=%s,columnfamily=%s",
                                         isIndex() ? "IndexColumnFamilies" : "ColumnFamilies",
                                         keyspace.getName(), name);
            try
            {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                ObjectName[] objectNames = {new ObjectName(mbeanName), new ObjectName(oldMBeanName)};
                for (ObjectName objectName : objectNames)
                {
                    mbs.registerMBean(this, objectName);
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            logger.trace("retryPolicy for {} is {}", name, this.metadata.get().params.speculativeRetry);
            latencyCalculator = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(new Runnable()
            {
                public void run()
                {
                    SpeculativeRetryParam retryPolicy = ColumnFamilyStore.this.metadata.get().params.speculativeRetry;
                    switch (retryPolicy.kind())
                    {
                        case PERCENTILE:
                            // get percentile in nanos
                            sampleLatencyNanos = (long) (metric.coordinatorReadLatency.getSnapshot().getValue(retryPolicy.threshold()));
                            break;
                        case CUSTOM:
                            sampleLatencyNanos = (long) retryPolicy.threshold();
                            break;
                        default:
                            sampleLatencyNanos = Long.MAX_VALUE;
                            break;
                    }
                }
            }, DatabaseDescriptor.getReadRpcTimeout(), DatabaseDescriptor.getReadRpcTimeout(), TimeUnit.MILLISECONDS);
        }
        else
        {
            latencyCalculator = ScheduledExecutors.optionalTasks.schedule(Runnables.doNothing(), 0, TimeUnit.NANOSECONDS);
            mbeanName = null;
            oldMBeanName= null;
        }
    }

    public TableMetadata metadata()
    {
        return metadata.get();
    }

    public Directories getDirectories()
    {
        return directories;
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, int sstableLevel, SerializationHeader header, LifecycleTransaction txn)
    {
        MetadataCollector collector = new MetadataCollector(metadata().comparator).sstableLevel(sstableLevel);
        return createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, collector, header, txn);
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, MetadataCollector metadataCollector, SerializationHeader header, LifecycleTransaction txn)
    {
        return getCompactionStrategyManager().createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, metadataCollector, header, indexManager.listIndexes(), txn);
    }

    public boolean supportsEarlyOpen()
    {
        return compactionStrategyManager.supportsEarlyOpen();
    }

    /** call when dropping or renaming a CF. Performs mbean housekeeping and invalidates CFS to other operations */
    public void invalidate()
    {
        invalidate(true);
    }

    public void invalidate(boolean expectMBean)
    {
        // disable and cancel in-progress compactions before invalidating
        valid = false;

        try
        {
            unregisterMBean();
        }
        catch (Exception e)
        {
            if (expectMBean)
            {
                JVMStabilityInspector.inspectThrowable(e);
                // this shouldn't block anything.
                logger.warn("Failed unregistering mbean: {}", mbeanName, e);
            }
        }

        latencyCalculator.cancel(false);
        compactionStrategyManager.shutdown();
        SystemKeyspace.removeTruncationRecord(metadata.id);

        storageHandler.invalidate();
        LifecycleTransaction.waitForDeletions();
        indexManager.dropAllIndexes();

        invalidateCaches();
    }

    /**
     * Removes every SSTable in the directory from the Tracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    void maybeRemoveUnreadableDirectory(File directory)
    {
        storageHandler.removeUnreadableDirectory(directory);
    }

    void unregisterMBean() throws MalformedObjectNameException, InstanceNotFoundException, MBeanRegistrationException
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName[] objectNames = {new ObjectName(mbeanName), new ObjectName(oldMBeanName)};
        for (ObjectName objectName : objectNames)
        {
            if (mbs.isRegistered(objectName))
                mbs.unregisterMBean(objectName);
        }

        // unregister metrics
        metric.release();
    }


    public static ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace, TableMetadataRef metadata, boolean loadSSTables)
    {
        return createColumnFamilyStore(keyspace, metadata.name, metadata, loadSSTables);
    }

    public static synchronized ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace,
                                                                         String columnFamily,
                                                                         TableMetadataRef metadata,
                                                                         boolean loadSSTables)
    {
        Directories directories = new Directories(metadata.get());
        return createColumnFamilyStore(keyspace, columnFamily, metadata, directories, loadSSTables, true, false);
    }

    /** This is only directly used by offline tools */
    public static synchronized ColumnFamilyStore createColumnFamilyStore(Keyspace keyspace,
                                                                         String columnFamily,
                                                                         TableMetadataRef metadata,
                                                                         Directories directories,
                                                                         boolean loadSSTables,
                                                                         boolean registerBookkeeping,
                                                                         boolean offline)
    {
        // get the max generation number, to prevent generation conflicts
        Directories.SSTableLister lister = directories.sstableLister(Directories.OnTxnErr.IGNORE).includeBackups(true);
        List<Integer> generations = new ArrayList<Integer>();
        for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
        {
            Descriptor desc = entry.getKey();
            generations.add(desc.generation);
            if (!desc.isCompatible())
                throw new RuntimeException(String.format("Incompatible SSTable found. Current version %s is unable to read file: %s. Please run upgradesstables.",
                                                         desc.getFormat().getLatestVersion(), desc));
        }
        Collections.sort(generations);
        int value = (generations.size() > 0) ? (generations.get(generations.size() - 1)) : 0;

        return new ColumnFamilyStore(keyspace, columnFamily, value, metadata, directories, loadSSTables, registerBookkeeping, offline);
    }

    /**
     * Removes unnecessary files from the cf directory at startup: these include temp files, orphans, zero-length files
     * and compacted sstables. Files that cannot be recognized will be ignored.
     */
    public static void  scrubDataDirectories(TableMetadata metadata) throws StartupException
    {
        Directories directories = new Directories(metadata);
        Set<File> cleanedDirectories = new HashSet<>();

         // clear ephemeral snapshots that were not properly cleared last session (CASSANDRA-7357)
        clearEphemeralSnapshots(directories);

        directories.removeTemporaryDirectories();

        logger.trace("Removing temporary or obsoleted files from unfinished operations for table {}", metadata.name);
        if (!LifecycleTransaction.removeUnfinishedLeftovers(metadata))
            throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                       String.format("Cannot remove temporary or obsoleted files for %s due to a problem with transaction " +
                                                     "log files. Please check records with problems in the log messages above and fix them. " +
                                                     "Refer to the 3.0 upgrading instructions in NEWS.txt " +
                                                     "for a description of transaction log files.", metadata.toString()));

        logger.trace("Further extra check for orphan sstable files for {}", metadata.name);
        for (Map.Entry<Descriptor,Set<Component>> sstableFiles : directories.sstableLister(Directories.OnTxnErr.IGNORE).list().entrySet())
        {
            Descriptor desc = sstableFiles.getKey();
            File directory = desc.directory;
            Set<Component> components = sstableFiles.getValue();

            if (!cleanedDirectories.contains(directory))
            {
                cleanedDirectories.add(directory);
                for (File tmpFile : desc.getTemporaryFiles())
                    tmpFile.delete();
            }

            File dataFile = new File(desc.filenameFor(Component.DATA));
            if (components.contains(Component.DATA) && dataFile.length() > 0)
                // everything appears to be in order... moving on.
                continue;

            // missing the DATA file! all components are orphaned
            logger.warn("Removing orphans for {}: {}", desc, components);
            for (Component component : components)
            {
                File file = new File(desc.filenameFor(component));
                if (file.exists())
                    FileUtils.deleteWithConfirm(desc.filenameFor(component));
            }
        }

        // cleanup incomplete saved caches
        Pattern tmpCacheFilePattern = Pattern.compile(metadata.keyspace + "-" + metadata.name + "-(Key|Row)Cache.*\\.tmp$");
        File dir = new File(DatabaseDescriptor.getSavedCachesLocation());

        if (dir.exists())
        {
            assert dir.isDirectory();
            for (File file : dir.listFiles())
                if (tmpCacheFilePattern.matcher(file.getName()).matches())
                    if (!file.delete())
                        logger.warn("could not delete {}", file.getAbsolutePath());
        }

        // also clean out any index leftovers.
        for (IndexMetadata index : metadata.indexes)
            if (!index.isCustom())
            {
                TableMetadata indexMetadata = CassandraIndex.indexCfsMetadata(metadata, index);
                scrubDataDirectories(indexMetadata);
            }
    }

    /**
     * See #{@code StorageService.refresh(String, String)} for more info
     *
     * @param ksName The keyspace name
     * @param cfName The columnFamily name
     */
    public static synchronized void refreshStorage(String ksName, String cfName)
    {
        /** ks/cf existence checks will be done by open and getCFS methods for us */
        Keyspace keyspace = Keyspace.open(ksName);
        keyspace.getColumnFamilyStore(cfName).refreshStorage();
    }

    /**
     * #{@inheritDoc}
     */
    public synchronized void refreshStorage()
    {
        storageHandler.refresh();
    }

    public void rebuildSecondaryIndex(String idxName)
    {
        rebuildSecondaryIndex(keyspace.getName(), metadata.name, idxName);
    }

    public static void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)
    {
        ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(cfName);

        logger.info("User Requested secondary index re-build for {}/{} indexes: {}", ksName, cfName, Joiner.on(',').join(idxNames));
        cfs.indexManager.rebuildIndexesBlocking(Sets.newHashSet(Arrays.asList(idxNames)));
    }

    public AbstractCompactionStrategy createCompactionStrategyInstance(CompactionParams compactionParams)
    {
        try
        {
            Constructor<? extends AbstractCompactionStrategy> constructor =
                compactionParams.klass().getConstructor(ColumnFamilyStore.class, Map.class);
            return constructor.newInstance(this, compactionParams.options());
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Deprecated
    public String getColumnFamilyName()
    {
        return getTableName();
    }

    public String getTableName()
    {
        return name;
    }

    /**
     * Flush if there is unflushed data in the memtables
     *
     * @return a Future yielding the commit log position that can be guaranteed to have been successfully written
     *         to sstables for this table once the future completes
     */
    public ListenableFuture<CommitLogPosition> forceFlush()
    {
        return storageHandler.forceFlush();
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
        return storageHandler.forceFlush(flushIfDirtyBefore);
    }

    public CommitLogPosition forceBlockingFlush()
    {
        return FBUtilities.waitOnFuture(forceFlush());
    }

    /**
     * Insert/Update the column family for this key.
     * Caller is responsible for acquiring Keyspace.switchLock
     * param @ lock - lock that needs to be used.
     * param @ key - key for update/insert
     * param @ columnFamily - columnFamily changes
     */
    public void apply(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, CommitLogPosition commitLogPosition)

    {
        long start = System.nanoTime();
        try
        {
            long timeDelta = storageHandler.apply(update, indexer, opGroup, commitLogPosition);
            DecoratedKey key = update.partitionKey();
            invalidateCachedPartition(key);
            metric.samplers.get(Sampler.WRITES).addSample(key.getKey(), key.hashCode(), 1);
            StorageHook.instance.reportWrite(metadata.id, update);
            metric.writeLatency.addNano(System.nanoTime() - start);
            // CASSANDRA-11117 - certain resolution paths on memtable put can result in very
            // large time deltas, either through a variety of sentinel timestamps (used for empty values, ensuring
            // a minimal write, etc). This limits the time delta to the max value the histogram
            // can bucket correctly. This also filters the Long.MAX_VALUE case where there was no previous value
            // to update.
            if(timeDelta < Long.MAX_VALUE)
                metric.colUpdateTimeDeltaHistogram.update(Math.min(18165375903306L, timeDelta));
        }
        catch (RuntimeException e)
        {
            throw new RuntimeException(e.getMessage()
                                       + " for ks: "
                                       + keyspace.getName() + ", table: " + name, e);
        }
    }

    /**
     * Calculate expected file size of SSTable after compaction.
     *
     * If operation type is {@code CLEANUP} and we're not dealing with an index sstable,
     * then we calculate expected file size with checking token range to be eliminated.
     *
     * Otherwise, we just add up all the files' size, which is the worst case file
     * size for compaction of all the list of files given.
     *
     * @param sstables SSTables to calculate expected compacted file size
     * @param operation Operation type
     * @return Expected file size of SSTable after compaction
     */
    public long getExpectedCompactedFileSize(Iterable<SSTableReader> sstables, OperationType operation)
    {
        if (operation != OperationType.CLEANUP || isIndex())
        {
            return SSTableReader.getTotalBytes(sstables);
        }

        // cleanup size estimation only counts bytes for keys local to this node
        long expectedFileSize = 0;
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());
        for (SSTableReader sstable : sstables)
        {
            List<Pair<Long, Long>> positions = sstable.getPositionsForRanges(ranges);
            for (Pair<Long, Long> position : positions)
                expectedFileSize += position.right - position.left;
        }

        double compressionRatio = metric.compressionRatio.getValue();
        if (compressionRatio > 0d)
            expectedFileSize *= compressionRatio;

        return expectedFileSize;
    }

    /*
     *  Find the maximum size file in the list .
     */
    public SSTableReader getMaxSizeFile(Iterable<SSTableReader> sstables)
    {
        long maxSize = 0L;
        SSTableReader maxFile = null;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.onDiskLength() > maxSize)
            {
                maxSize = sstable.onDiskLength();
                maxFile = sstable;
            }
        }
        return maxFile;
    }

    public CompactionManager.AllSSTableOpStatus forceCleanup(int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performCleanup(ColumnFamilyStore.this, jobs);
    }

    public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, int jobs) throws ExecutionException, InterruptedException
    {
        return scrub(disableSnapshot, skipCorrupted, reinsertOverflowedTTL, false, checkData, jobs);
    }

    @VisibleForTesting
    public CompactionManager.AllSSTableOpStatus scrub(boolean disableSnapshot, boolean skipCorrupted, boolean reinsertOverflowedTTL, boolean alwaysFail, boolean checkData, int jobs) throws ExecutionException, InterruptedException
    {
        // skip snapshot creation during scrub, SEE JIRA 5891
        if(!disableSnapshot)
            snapshotWithoutFlush("pre-scrub-" + System.currentTimeMillis());

        try
        {
            return CompactionManager.instance.performScrub(ColumnFamilyStore.this, skipCorrupted, checkData, reinsertOverflowedTTL, jobs);
        }
        catch(Throwable t)
        {
            if (!rebuildOnFailedScrub(t))
                throw t;

            return alwaysFail ? CompactionManager.AllSSTableOpStatus.ABORTED : CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        }
    }

    /**
     * CASSANDRA-5174 : For an index cfs we may be able to discard everything and just rebuild
     * the index when a scrub fails.
     *
     * @return true if we are an index cfs and we successfully rebuilt the index
     */
    public boolean rebuildOnFailedScrub(Throwable failure)
    {
        if (!isIndex() || !SecondaryIndexManager.isIndexColumnFamilyStore(this))
            return false;

        truncateBlocking();

        logger.warn("Rebuilding index for {} because of <{}>", name, failure.getMessage());

        ColumnFamilyStore parentCfs = SecondaryIndexManager.getParentCfs(this);
        assert parentCfs.indexManager.getAllIndexColumnFamilyStores().contains(this);

        String indexName = SecondaryIndexManager.getIndexName(this);

        parentCfs.rebuildSecondaryIndex(indexName);
        return true;
    }

    public CompactionManager.AllSSTableOpStatus verify(Verifier.Options options) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performVerify(ColumnFamilyStore.this, options);
    }

    public CompactionManager.AllSSTableOpStatus sstablesRewrite(boolean excludeCurrentVersion, int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performSSTableRewrite(ColumnFamilyStore.this, excludeCurrentVersion, jobs);
    }

    public CompactionManager.AllSSTableOpStatus relocateSSTables(int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.relocateSSTables(this, jobs);
    }

    public CompactionManager.AllSSTableOpStatus garbageCollect(TombstoneOption tombstoneOption, int jobs) throws ExecutionException, InterruptedException
    {
        return CompactionManager.instance.performGarbageCollection(this, tombstoneOption, jobs);
    }

    public boolean isValid()
    {
        return valid;
    }
    
    // TODO (Dikang): remove this method from ColumnFamilyStore
    public Set<SSTableReader> getLiveSSTables()
    {
        return storageHandler.getLiveSSTables();
    }

    public boolean isFilterFullyCoveredBy(ClusteringIndexFilter filter,
                                          DataLimits limits,
                                          CachedPartition cached,
                                          int nowInSec,
                                          boolean enforceStrictLiveness)
    {
        // We can use the cached value only if we know that no data it doesn't contain could be covered
        // by the query filter, that is if:
        //   1) either the whole partition is cached
        //   2) or we can ensure than any data the filter selects is in the cached partition

        // We can guarantee that a partition is fully cached if the number of rows it contains is less than
        // what we're caching. Wen doing that, we should be careful about expiring cells: we should count
        // something expired that wasn't when the partition was cached, or we could decide that the whole
        // partition is cached when it's not. This is why we use CachedPartition#cachedLiveRows.
        if (cached.cachedLiveRows() < metadata().params.caching.rowsPerPartitionToCache())
            return true;

        // If the whole partition isn't cached, then we must guarantee that the filter cannot select data that
        // is not in the cache. We can guarantee that if either the filter is a "head filter" and the cached
        // partition has more live rows that queried (where live rows refers to the rows that are live now),
        // or if we can prove that everything the filter selects is in the cached partition based on its content.
        return (filter.isHeadFilter() && limits.hasEnoughLiveData(cached,
                                                                  nowInSec,
                                                                  filter.selectsAllPartition(),
                                                                  enforceStrictLiveness))
                || filter.isFullyCoveredBy(cached);
    }

    public int gcBefore(int nowInSec)
    {
        return nowInSec - metadata().params.gcGraceSeconds;
    }

    // WARNING: this returns the set of LIVE sstables only, which may be only partially written
    public List<String> getSSTablesForKey(String key)
    {
        return getSSTablesForKey(key, false);
    }

    public List<String> getSSTablesForKey(String key, boolean hexFormat)
    {
        return storageHandler.getSSTablesForKey(key, hexFormat);
    }


    public void beginLocalSampling(String sampler, int capacity)
    {
        metric.samplers.get(Sampler.valueOf(sampler)).beginSampling(capacity);
    }

    public CompositeData finishLocalSampling(String sampler, int count) throws OpenDataException
    {
        SamplerResult<ByteBuffer> samplerResults = metric.samplers.get(Sampler.valueOf(sampler))
                .finishSampling(count);
        TabularDataSupport result = new TabularDataSupport(COUNTER_TYPE);
        for (Counter<ByteBuffer> counter : samplerResults.topK)
        {
            //Not duplicating the buffer for safety because AbstractSerializer and ByteBufferUtil.bytesToHex
            //don't modify position or limit
            ByteBuffer key = counter.getItem();
            result.put(new CompositeDataSupport(COUNTER_COMPOSITE_TYPE, COUNTER_NAMES, new Object[] {
                    ByteBufferUtil.bytesToHex(key), // raw
                    counter.getCount(),  // count
                    counter.getError(),  // error
                    metadata().partitionKeyType.getString(key) })); // string
        }
        return new CompositeDataSupport(SAMPLING_RESULT, SAMPLER_NAMES, new Object[]{
                samplerResults.cardinality, result});
    }

    public boolean isCompactionDiskSpaceCheckEnabled()
    {
        return compactionSpaceCheck;
    }

    public void compactionDiskSpaceCheck(boolean enable)
    {
        compactionSpaceCheck = enable;
    }

    public void cleanupCache()
    {
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(keyspace.getName());

        for (Iterator<RowCacheKey> keyIter = CacheService.instance.rowCache.keyIterator();
             keyIter.hasNext(); )
        {
            RowCacheKey key = keyIter.next();
            DecoratedKey dk = decorateKey(ByteBuffer.wrap(key.key));
            if (key.sameTable(metadata()) && !Range.isInRanges(dk.getToken(), ranges))
                invalidateCachedPartition(dk);
        }

        if (metadata().isCounter())
        {
            for (Iterator<CounterCacheKey> keyIter = CacheService.instance.counterCache.keyIterator();
                 keyIter.hasNext(); )
            {
                CounterCacheKey key = keyIter.next();
                DecoratedKey dk = decorateKey(key.partitionKey());
                if (key.sameTable(metadata()) && !Range.isInRanges(dk.getToken(), ranges))
                    CacheService.instance.counterCache.remove(key);
            }
        }
    }

    public ClusteringComparator getComparator()
    {
        return metadata().comparator;
    }

    public void snapshotWithoutFlush(String snapshotName)
    {
        storageHandler.snapshotWithoutFlush(snapshotName, null, false);
    }

    protected static void clearEphemeralSnapshots(Directories directories)
    {
        for (String ephemeralSnapshot : directories.listEphemeralSnapshots())
        {
            logger.trace("Clearing ephemeral snapshot {} leftover from previous session.", ephemeralSnapshot);
            Directories.clearSnapshot(ephemeralSnapshot, directories.getCFDirectories());
        }
    }

    /**
     * Take a snap shot of this columnfamily store.
     *
     * @param snapshotName the name of the associated with the snapshot
     */
    public Set<SSTableReader> snapshot(String snapshotName)
    {
        return snapshot(snapshotName, false);
    }

    /**
     * Take a snap shot of this columnfamily store.
     *
     * @param snapshotName the name of the associated with the snapshot
     * @param skipFlush Skip blocking flush of memtable
     */
    public Set<SSTableReader> snapshot(String snapshotName, boolean skipFlush)
    {
        return snapshot(snapshotName, null, false, skipFlush);
    }


    /**
     * @param ephemeral If this flag is set to true, the snapshot will be cleaned up during next startup
     * @param skipFlush Skip blocking flush of memtable
     */
    public Set<SSTableReader> snapshot(String snapshotName, Predicate<SSTableReader> predicate, boolean ephemeral, boolean skipFlush)
    {
        if (!skipFlush)
        {
            forceBlockingFlush();
        }
        return storageHandler.snapshotWithoutFlush(snapshotName, predicate, ephemeral);
    }

    public boolean snapshotExists(String snapshotName)
    {
        return getDirectories().snapshotExists(snapshotName);
    }

    public long getSnapshotCreationTime(String snapshotName)
    {
        return getDirectories().snapshotCreationTime(snapshotName);
    }

    /**
     * Clear all the snapshots for a given column family.
     *
     * @param snapshotName the user supplied snapshot name. If left empty,
     *                     all the snapshots will be cleaned.
     */
    public void clearSnapshot(String snapshotName)
    {
        List<File> snapshotDirs = getDirectories().getCFDirectories();
        Directories.clearSnapshot(snapshotName, snapshotDirs);
    }
    /**
     *
     * @return  Return a map of all snapshots to space being used
     * The pair for a snapshot has true size and size on disk.
     */
    public Map<String, Pair<Long,Long>> getSnapshotDetails()
    {
        return getDirectories().getSnapshotDetails();
    }

    /**
     * @return the cached partition for @param key if it is already present in the cache.
     * Not that this will not readAndCache the parition if it is not present, nor
     * are these calls counted in cache statistics.
     *
     * Note that this WILL cause deserialization of a SerializingCache partition, so if all you
     * need to know is whether a partition is present or not, use containsCachedParition instead.
     */
    public CachedPartition getRawCachedPartition(DecoratedKey key)
    {
        if (!isRowCacheEnabled())
            return null;
        IRowCacheEntry cached = CacheService.instance.rowCache.getInternal(new RowCacheKey(metadata(), key));
        return cached == null || cached instanceof RowCacheSentinel ? null : (CachedPartition)cached;
    }

    protected void invalidateCaches()
    {
        CacheService.instance.invalidateKeyCacheForCf(metadata());
        CacheService.instance.invalidateRowCacheForCf(metadata());
        if (metadata().isCounter())
            CacheService.instance.invalidateCounterCacheForCf(metadata());
    }

    public int invalidateRowCache(Collection<Bounds<Token>> boundsToInvalidate)
    {
        int invalidatedKeys = 0;
        for (Iterator<RowCacheKey> keyIter = CacheService.instance.rowCache.keyIterator();
             keyIter.hasNext(); )
        {
            RowCacheKey key = keyIter.next();
            DecoratedKey dk = decorateKey(ByteBuffer.wrap(key.key));
            if (key.sameTable(metadata()) && Bounds.isInBounds(dk.getToken(), boundsToInvalidate))
            {
                invalidateCachedPartition(dk);
                invalidatedKeys++;
            }
        }
        return invalidatedKeys;
    }

    public int invalidateCounterCache(Collection<Bounds<Token>> boundsToInvalidate)
    {
        int invalidatedKeys = 0;
        for (Iterator<CounterCacheKey> keyIter = CacheService.instance.counterCache.keyIterator();
             keyIter.hasNext(); )
        {
            CounterCacheKey key = keyIter.next();
            DecoratedKey dk = decorateKey(key.partitionKey());
            if (key.sameTable(metadata()) && Bounds.isInBounds(dk.getToken(), boundsToInvalidate))
            {
                CacheService.instance.counterCache.remove(key);
                invalidatedKeys++;
            }
        }
        return invalidatedKeys;
    }

    /**
     * @return true if @param key is contained in the row cache
     */
    public boolean containsCachedParition(DecoratedKey key)
    {
        return CacheService.instance.rowCache.getCapacity() != 0 && CacheService.instance.rowCache.containsKey(new RowCacheKey(metadata(), key));
    }

    public void invalidateCachedPartition(RowCacheKey key)
    {
        CacheService.instance.rowCache.remove(key);
    }

    public void invalidateCachedPartition(DecoratedKey key)
    {
        if (!isRowCacheEnabled())
            return;

        invalidateCachedPartition(new RowCacheKey(metadata(), key));
    }

    public ClockAndCount getCachedCounter(ByteBuffer partitionKey, Clustering clustering, ColumnMetadata column, CellPath path)
    {
        if (CacheService.instance.counterCache.getCapacity() == 0L) // counter cache disabled.
            return null;
        return CacheService.instance.counterCache.get(CounterCacheKey.create(metadata(), partitionKey, clustering, column, path));
    }

    public void putCachedCounter(ByteBuffer partitionKey, Clustering clustering, ColumnMetadata column, CellPath path, ClockAndCount clockAndCount)
    {
        if (CacheService.instance.counterCache.getCapacity() == 0L) // counter cache disabled.
            return;
        CacheService.instance.counterCache.put(CounterCacheKey.create(metadata(), partitionKey, clustering, column, path), clockAndCount);
    }

    public void forceMajorCompaction() throws InterruptedException, ExecutionException
    {
        forceMajorCompaction(false);
    }

    public void forceMajorCompaction(boolean splitOutput) throws InterruptedException, ExecutionException
    {
        CompactionManager.instance.performMaximal(this, splitOutput);
    }

    public void forceCompactionForTokenRange(Collection<Range<Token>> tokenRanges) throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.forceCompactionForTokenRange(this, tokenRanges);
    }

    public static Iterable<ColumnFamilyStore> all()
    {
        List<Iterable<ColumnFamilyStore>> stores = new ArrayList<Iterable<ColumnFamilyStore>>(Schema.instance.getKeyspaces().size());
        for (Keyspace keyspace : Keyspace.all())
        {
            stores.add(keyspace.getColumnFamilyStores());
        }
        return Iterables.concat(stores);
    }

    public Iterable<DecoratedKey> keySamples(Range<Token> range)
    {
        return storageHandler.keySamples(range);
    }

    public long estimatedKeysForRange(Range<Token> range)
    {
        return storageHandler.estimatedKeysForRange(range);
    }

    /**
     * For testing.  No effort is made to clear historical or even the current memtables, nor for
     * thread safety.  All we do is wipe the sstable containers clean, while leaving the actual
     * data files present on disk.  (This allows tests to easily call refresh on them.)
     */
    @VisibleForTesting
    public void clearUnsafe()
    {
        for (final ColumnFamilyStore cfs : concatWithIndexes())
        {
            cfs.getStorageHandler().clearUnsafe();
        }
    }

    /**
     * Truncate deletes the entire column family's data with no expensive tombstone creation
     */
    public void truncateBlocking()
    {
        storageHandler.truncateBlocking();
        logger.info("Truncate of {}.{} is complete", keyspace.getName(), name);
    }

    public <V> V runWithCompactionsDisabled(Callable<V> callable, boolean interruptValidation, boolean interruptViews)
    {
        // synchronize so that concurrent invocations don't re-enable compactions partway through unexpectedly,
        // and so we only run one major compaction at a time
        synchronized (this)
        {
            logger.trace("Cancelling in-progress compactions for {}", metadata.name);

            Iterable<ColumnFamilyStore> selfWithAuxiliaryCfs = interruptViews
                                                               ? Iterables.concat(concatWithIndexes(), viewManager.allViewsCfs())
                                                               : concatWithIndexes();

            for (ColumnFamilyStore cfs : selfWithAuxiliaryCfs)
                cfs.getCompactionStrategyManager().pause();
            try
            {
                // interrupt in-progress compactions
                CompactionManager.instance.interruptCompactionForCFs(selfWithAuxiliaryCfs, interruptValidation);
                CompactionManager.instance.waitForCessation(selfWithAuxiliaryCfs);

                // doublecheck that we finished, instead of timing out
                for (ColumnFamilyStore cfs : selfWithAuxiliaryCfs)
                {
                    if (!cfs.getStorageHandler().getTracker().getCompacting().isEmpty())
                    {
                        logger.warn("Unable to cancel in-progress compactions for {}.  Perhaps there is an unusually large row in progress somewhere, or the system is simply overloaded.", metadata.name);
                        return null;
                    }
                }
                logger.trace("Compactions successfully cancelled");

                // run our task
                try
                {
                    return callable.call();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
            finally
            {
                for (ColumnFamilyStore cfs : selfWithAuxiliaryCfs)
                    cfs.getCompactionStrategyManager().resume();
            }
        }
    }

    @Override
    public String toString()
    {
        return "CFS(" +
               "Keyspace='" + keyspace.getName() + '\'' +
               ", ColumnFamily='" + name + '\'' +
               ')';
    }

    public void disableAutoCompaction()
    {
        // we don't use CompactionStrategy.pause since we don't want users flipping that on and off
        // during runWithCompactionsDisabled
        compactionStrategyManager.disable();
    }

    public void enableAutoCompaction()
    {
        enableAutoCompaction(false);
    }

    /**
     * used for tests - to be able to check things after a minor compaction
     * @param waitForFutures if we should block until autocompaction is done
     */
    @VisibleForTesting
    public void enableAutoCompaction(boolean waitForFutures)
    {
        compactionStrategyManager.enable();
        List<Future<?>> futures = CompactionManager.instance.submitBackground(this);
        if (waitForFutures)
            FBUtilities.waitOnFutures(futures);
    }

    public boolean isAutoCompactionDisabled()
    {
        return !this.compactionStrategyManager.isEnabled();
    }

    /*
     JMX getters and setters for the Default<T>s.
       - get/set minCompactionThreshold
       - get/set maxCompactionThreshold
       - get     memsize
       - get     memops
       - get/set memtime
     */

    public CompactionStrategyManager getCompactionStrategyManager()
    {
        return compactionStrategyManager;
    }

    public void setCrcCheckChance(double crcCheckChance)
    {
        storageHandler.setCrcCheckChance(crcCheckChance);
    }


    public Double getCrcCheckChance()
    {
        return crcCheckChance.value();
    }

    public void setCompactionThresholds(int minThreshold, int maxThreshold)
    {
        validateCompactionThresholds(minThreshold, maxThreshold);

        minCompactionThreshold.set(minThreshold);
        maxCompactionThreshold.set(maxThreshold);
        CompactionManager.instance.submitBackground(this);
    }

    public int getMinimumCompactionThreshold()
    {
        return minCompactionThreshold.value();
    }

    public void setMinimumCompactionThreshold(int minCompactionThreshold)
    {
        validateCompactionThresholds(minCompactionThreshold, maxCompactionThreshold.value());
        this.minCompactionThreshold.set(minCompactionThreshold);
    }

    public int getMaximumCompactionThreshold()
    {
        return maxCompactionThreshold.value();
    }

    public void setMaximumCompactionThreshold(int maxCompactionThreshold)
    {
        validateCompactionThresholds(minCompactionThreshold.value(), maxCompactionThreshold);
        this.maxCompactionThreshold.set(maxCompactionThreshold);
    }

    private void validateCompactionThresholds(int minThreshold, int maxThreshold)
    {
        if (minThreshold > maxThreshold)
            throw new RuntimeException(String.format("The min_compaction_threshold cannot be larger than the max_compaction_threshold. " +
                                                     "Min is '%d', Max is '%d'.", minThreshold, maxThreshold));

        if (maxThreshold == 0 || minThreshold == 0)
            throw new RuntimeException("Disabling compaction by setting min_compaction_threshold or max_compaction_threshold to 0 " +
                    "is deprecated, set the compaction strategy option 'enabled' to 'false' instead or use the nodetool command 'disableautocompaction'.");
    }

    // End JMX get/set.

    public int getMeanColumns()
    {
        return storageHandler.getMeanColumns();
    }

    public double getMeanPartitionSize()
    {
        return storageHandler.getMeanPartitionSize();
    }

    public long estimateKeys()
    {
        return storageHandler.estimateKeys();
    }

    public IPartitioner getPartitioner()
    {
        return metadata().partitioner;
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return getPartitioner().decorateKey(key);
    }

    /** true if this CFS contains secondary index data */
    public boolean isIndex()
    {
        return metadata().isIndex();
    }

    public Iterable<ColumnFamilyStore> concatWithIndexes()
    {
        // we return the main CFS first, which we rely on for simplicity in switchMemtable(), for getting the
        // latest commit log segment position
        return Iterables.concat(Collections.singleton(this), indexManager.getAllIndexColumnFamilyStores());
    }

    public List<String> getBuiltIndexes()
    {
       return indexManager.getBuiltIndexNames();
    }

    public int getUnleveledSSTables()
    {
        return compactionStrategyManager.getUnleveledSSTables();
    }

    public int[] getSSTableCountPerLevel()
    {
        return compactionStrategyManager.getSSTableCountPerLevel();
    }

    public int getLevelFanoutSize()
    {
        return compactionStrategyManager.getLevelFanoutSize();
    }

    public static class ViewFragment
    {
        public final List<SSTableReader> sstables;
        public final Iterable<Memtable> memtables;

        public ViewFragment(List<SSTableReader> sstables, Iterable<Memtable> memtables)
        {
            this.sstables = sstables;
            this.memtables = memtables;
        }
    }

    public static class RefViewFragment extends ViewFragment implements AutoCloseable
    {
        public final Refs<SSTableReader> refs;

        public RefViewFragment(List<SSTableReader> sstables, Iterable<Memtable> memtables, Refs<SSTableReader> refs)
        {
            super(sstables, memtables);
            this.refs = refs;
        }

        public void release()
        {
            refs.release();
        }

        public void close()
        {
            refs.release();
        }
    }

    public boolean isEmpty()
    {
        return storageHandler.isEmpty();
    }

    public boolean isRowCacheEnabled()
    {

        boolean retval = metadata().params.caching.cacheRows() && CacheService.instance.rowCache.getCapacity() > 0;
        assert(!retval || !isIndex());
        return retval;
    }

    public boolean isCounterCacheEnabled()
    {
        return metadata().isCounter() && CacheService.instance.counterCache.getCapacity() > 0;
    }

    public boolean isKeyCacheEnabled()
    {
        return metadata().params.caching.cacheKeys() && CacheService.instance.keyCache.getCapacity() > 0;
    }

    /**
     * Discard all DB files that were created before given timestamp.
     *
     * Caller should first ensure that comapctions have quiesced.
     *
     * @param truncatedAt The timestamp of the truncation
     *                    (all SSTables before that timestamp are going be marked as compacted)
     */
    public void discardDBFiles(long truncatedAt)
    {
        storageHandler.discardSSTables(truncatedAt);
    }

    public double getDroppableTombstoneRatio()
    {
        return storageHandler.getDroppableTombstoneRatio();
    }

    public long trueSnapshotsSize()
    {
        return getDirectories().trueSnapshotsSize();
    }

    /**
     * Returns a ColumnFamilyStore by id if it exists, null otherwise
     * Differently from others, this method does not throw exception if the table does not exist.
     */
    public static ColumnFamilyStore getIfExists(TableId id)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(id);
        if (metadata == null)
            return null;

        Keyspace keyspace = Keyspace.open(metadata.keyspace);
        if (keyspace == null)
            return null;

        return keyspace.hasColumnFamilyStore(id)
             ? keyspace.getColumnFamilyStore(id)
             : null;
    }

    /**
     * Returns a ColumnFamilyStore by ksname and cfname if it exists, null otherwise
     * Differently from others, this method does not throw exception if the keyspace or table does not exist.
     */
    public static ColumnFamilyStore getIfExists(String ksName, String cfName)
    {
        if (ksName == null || cfName == null)
            return null;

        Keyspace keyspace = Keyspace.open(ksName);
        if (keyspace == null)
            return null;

        TableMetadata table = Schema.instance.getTableMetadata(ksName, cfName);
        if (table == null)
            return null;

        return keyspace.getColumnFamilyStore(table.id);
    }

    public static TableMetrics metricsFor(TableId tableId)
    {
        return getIfExists(tableId).metric;
    }

    public DiskBoundaries getDiskBoundaries()
    {
        return diskBoundaryManager.getDiskBoundaries(this);
    }

    public void invalidateDiskBoundaries()
    {
        diskBoundaryManager.invalidate();
    }

    @Override
    public void setNeverPurgeTombstones(boolean value)
    {
        if (neverPurgeTombstones != value)
            logger.info("Changing neverPurgeTombstones for {}.{} from {} to {}", keyspace.getName(), getTableName(), neverPurgeTombstones, value);
        else
            logger.info("Not changing neverPurgeTombstones for {}.{}, it is {}", keyspace.getName(), getTableName(), neverPurgeTombstones);

        neverPurgeTombstones = value;
    }

    @Override
    public boolean getNeverPurgeTombstones()
    {
        return neverPurgeTombstones;
    }
}
