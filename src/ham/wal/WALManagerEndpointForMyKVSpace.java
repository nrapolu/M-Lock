package ham.wal;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.hadoop.hbase.util.Threads;

/**
 * This class implements WAL management for entity-groups/microshards using
 * Coprocessor endpoints. TO-DO: 1. Make sure the table created to store these
 * logs have the needed structure. 2. Use mutateRow in HRegion for all
 * operations -- it is already public in 0.94 3. Methodology: Each new commit
 * creates a LogEntry, which is stored as a value in a column. We will have to
 * use ColumnPrefixFilter to select only those columns which are part of a
 * snapshot, or those that need to-be checked against for conflict detection.
 * The other option is to store LogEntries as values with different timestamps.
 */
public class WALManagerEndpointForMyKVSpace extends BaseEndpointCoprocessor
		implements WALManagerProtocol {
	private static final Log LOG = LogFactory
			.getLog("ham.wal.WALManagerEndpoint");
	public static final ConcurrentHashMap<HashedBytes, CountDownLatch> lockedRows = new ConcurrentHashMap<HashedBytes, CountDownLatch>();

	protected ConcurrentHashMap<String, TreeMap<Long, byte[]>> myKVSpace = new ConcurrentHashMap<String, TreeMap<Long, byte[]>>();

	// To-be-flushed-logs. Whenever the currentTs for a log exceeds some periodic
	// number, the thread executing that transaction puts the log into this list
	// and
	// interrupts the store-updater. The store-updater reads this list and flushes
	// the individual logs.
	private final ConcurrentLinkedQueue<LogId> toBeFlushedLogs = new ConcurrentLinkedQueue<LogId>();
	int flushIntervalForTimestamps = 4000;

	protected Configuration conf = null;
	private StoreUpdaterForMyKVSpace storeUpdater = null;
	private boolean stopped = false;

	public static void sysout(String otp) {
		// System.out.println(otp);
	}

	@Override
	public void start(CoprocessorEnvironment env) {
		super.start(env);
		this.conf = env.getConfiguration();
		// The logFlusherPeriod and wakeFrequency can also be set through
		// hbase-site.xml.
		// In that case, we'll have to take those values from conf. For now, we
		// hardcode them
		// and assume that transactions will themselves trigger the asynchronous
		// flush when they need.
		this.storeUpdater = new StoreUpdaterForMyKVSpace(this, 10 * 60 * 1000,
				50 * 60 * 1000);

		UncaughtExceptionHandler handler = new UncaughtExceptionHandler() {
			public void uncaughtException(Thread t, Throwable e) {
				System.err.println("Uncaught exception in service thread "
						+ t.getName() + e);
			}
		};

		// BIGNOTE: Since transactions are synchronously flushing, we won't need an
		// asynchronous
		// storeUpdater.
		// Threads.setDaemonThreadRunning(this.storeUpdater.getThread(),
		// ".StoreUpdater", handler);
	}

	@Override
	public void stop(CoprocessorEnvironment env) {
		// TODO Auto-generated method stub
		super.stop(env);
		this.stopped = true;
		this.storeUpdater.interruptIfNecessary();
	}

	public boolean isStopped() {
		return stopped;
	}

	@Override
	public Snapshot start(LogId id) throws IOException {
		Snapshot snapshot = null;
		try {
			// Get the coprocessor environment
			RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();

			// Snapshot includes all the writes in the log entries with
			// timestamp > syncTs and <= currentTs.
			snapshot = new Snapshot();

			// Get g = new Get(id.getKey());
			// g.addColumn(WALTableProperties.WAL_FAMILY,
			// WALTableProperties.SYNC_TS_COL);
			// g.addColumn(WALTableProperties.WAL_FAMILY,
			// WALTableProperties.CURRENT_TS_COL);
			// g.setTimeStamp(WALTableProperties.GENERIC_TIMESTAMP);
			//
			// Result r = env.getRegion().get(g, null);
			// long syncTs = 0;
			// long currentTs = 0;
			// if (r.isEmpty()) {
			// sysout("Result is empty");
			// // Add a new WAL to the table with the given id.
			// Put p = new Put(id.getKey());
			// p.add(WALTableProperties.WAL_FAMILY, WALTableProperties.SYNC_TS_COL,
			// WALTableProperties.GENERIC_TIMESTAMP, Bytes.toBytes(syncTs));
			// p.add(WALTableProperties.WAL_FAMILY, WALTableProperties.CURRENT_TS_COL,
			// WALTableProperties.GENERIC_TIMESTAMP, Bytes.toBytes(currentTs));
			// p.add(WALTableProperties.WAL_FAMILY, WALTableProperties.OLDEST_TS_COL,
			// WALTableProperties.GENERIC_TIMESTAMP, Bytes.toBytes(syncTs));
			// env.getRegion().put(p);
			//
			// Map<String, Write> writeMap = new HashMap<String, Write>();
			// snapshot.setTimestamp(currentTs);
			// sysout("Setting timestamp for snapshot: " + currentTs);
			// snapshot.setWriteMap(writeMap);
			// return snapshot;
			// }

			long syncTs = 0;
			long currentTs = 0;
			String syncKey = Bytes.toString(id.getKey())
					+ Bytes.toString(WALTableProperties.WAL_FAMILY)
					+ Bytes.toString(WALTableProperties.SYNC_TS_COL);
			TreeMap<Long, byte[]> syncTimestampMap = myKVSpace.get(syncKey);

			String currentKey = Bytes.toString(id.getKey())
					+ Bytes.toString(WALTableProperties.WAL_FAMILY)
					+ Bytes.toString(WALTableProperties.CURRENT_TS_COL);
			TreeMap<Long, byte[]> currentTimestampMap = myKVSpace.get(currentKey);

			if (syncTimestampMap == null || currentTimestampMap == null) {
				sysout("Result is empty");
				// Add a new WAL to the table with the given id.
				syncTimestampMap = new TreeMap<Long, byte[]>();
				syncTimestampMap.put(WALTableProperties.GENERIC_TIMESTAMP, Bytes
						.toBytes(syncTs));
				myKVSpace.put(syncKey, syncTimestampMap);

				// Similarly, we put the timestamp maps for currentTs and oldestTs.
				currentTimestampMap = new TreeMap<Long, byte[]>();
				currentTimestampMap.put(WALTableProperties.GENERIC_TIMESTAMP, Bytes
						.toBytes(currentTs));
				myKVSpace.put(currentKey, currentTimestampMap);

				String oldestKey = Bytes.toString(id.getKey())
						+ Bytes.toString(WALTableProperties.WAL_FAMILY)
						+ Bytes.toString(WALTableProperties.OLDEST_TS_COL);
				TreeMap<Long, byte[]> oldestTimestampMap = new TreeMap<Long, byte[]>();
				oldestTimestampMap.put(WALTableProperties.GENERIC_TIMESTAMP, Bytes
						.toBytes(syncTs));
				myKVSpace.put(oldestKey, oldestTimestampMap);

				Map<String, Write> writeMap = new HashMap<String, Write>();
				snapshot.setTimestamp(currentTs);
				sysout("Setting timestamp for snapshot: " + currentTs);
				snapshot.setWriteMap(writeMap);
				return snapshot;
			}

			// Grab the currentTs and syncTs from the table.
			// syncTs = Bytes.toLong(r.getValue(WALTableProperties.WAL_FAMILY,
			// WALTableProperties.SYNC_TS_COL));
			// currentTs = Bytes.toLong(r.getValue(WALTableProperties.WAL_FAMILY,
			// WALTableProperties.CURRENT_TS_COL));

			// Grab the currentTs and syncTs from the table.
			syncTs = Bytes.toLong(syncTimestampMap
					.get(WALTableProperties.GENERIC_TIMESTAMP));
			currentTs = Bytes.toLong(currentTimestampMap
					.get(WALTableProperties.GENERIC_TIMESTAMP));

			// We should return with empty snapshot if currentTs is equal to syncTs.
			if (currentTs == syncTs) {
				Map<String, Write> writeMap = new HashMap<String, Write>();
				snapshot.setTimestamp(currentTs);
				sysout("Setting timestamp for snapshot: " + currentTs);
				snapshot.setWriteMap(writeMap);
				return snapshot;
			}

			// Grab all the LogEntries at timestamps > syncTs && <= currentTs.
			// Get getLogEntries = new Get(id.getKey());
			// getLogEntries.addColumn(WALTableProperties.WAL_FAMILY,
			// WALTableProperties.WAL_ENTRY_COL);
			// // In timerange, left is inclusive and right is exclusive.
			// sysout("For snapshot: Setting timerange: " + (syncTs + 1)
			// + " - " + (currentTs + 1));
			// getLogEntries.setTimeRange(syncTs + 1, currentTs + 1);
			// getLogEntries.setMaxVersions(Integer.MAX_VALUE);
			// Result logEntries = env.getRegion().get(getLogEntries, null);

			// Grab all the LogEntries at timestamps > syncTs && <= currentTs.
			String key = Bytes.toString(id.getKey())
					+ Bytes.toString(WALTableProperties.WAL_FAMILY)
					+ Bytes.toString(WALTableProperties.WAL_ENTRY_COL);
			TreeMap<Long, byte[]> timestampMap = myKVSpace.get(key);

			// Add the writes from all logEntries into the Snapshot.
			// NavigableMap<Long, byte[]> timestampMap = logEntries.getMap().get(
			// WALTableProperties.WAL_FAMILY).get(WALTableProperties.WAL_ENTRY_COL);
			// sysout("Nb of log entries retrieved for snapshot: "
			// + timestampMap.size());
			// Map<String, Write> writeMap = new HashMap<String, Write>();
			// for (Long timestamp : timestampMap.keySet()) {
			// sysout("Reading logEntry with timestamp: " + timestamp);
			// LogEntry logEntry = LogEntry.fromBytes(timestampMap.get(timestamp));
			// for (Write w : logEntry.getWrites()) {
			// if (writeMap.get(w.getNameAndKey()) != null)
			// // Another entry with a greater timestamp is already persent.
			// continue;
			// // System.out.println("Adding a write to the snapshot: " +
			// // w.toString());
			// writeMap.put(w.getNameAndKey(), w);
			// }
			// }

			// Add the writes from all logEntries into the Snapshot.
			Map<String, Write> writeMap = new HashMap<String, Write>();
			// In timerange, left is inclusive and right is exclusive.
			// NavigableMap<Long, byte[]> subMap = timestampMap.subMap(syncTs + 1,
			// true,
			// currentTs + 1, false);
			// for (Long timestamp : subMap.descendingKeySet()) {
			sysout("For log: " + Bytes.toString(id.getKey())
					+ ", for snapshot: Setting timerange: " + (syncTs + 1) + " - "
					+ (currentTs + 1));
			for (long ts = currentTs; ts >= syncTs + 1; ts--) {
				sysout("Reading logEntry with timestamp: " + ts);
				byte[] val = timestampMap.get(ts);
				if (val == null) {
					sysout("Skipped");
					continue;
				}
				LogEntry logEntry = LogEntry.fromBytes(val);
				for (Write w : logEntry.getWrites()) {
					if (writeMap.get(w.getNameAndKey()) != null) {
						sysout("Skipped this Write: " + w.toString());
						// Another entry with a greater timestamp is already persent.
						continue;
					}
					// System.out.println("Adding a write to the snapshot: " +
					// w.toString());
					writeMap.put(w.getNameAndKey(), w);
				}
			}

			sysout("Setting timestamp for snapshot: " + currentTs);
			snapshot.setTimestamp(currentTs);
			snapshot.setWriteMap(writeMap);
		} catch (Exception e) {
			System.err.println("Error in start function: " + e.getMessage());
			e.printStackTrace();
		}
		return snapshot;
	}

	@Override
	public long startTime(LogId id) throws IOException {
		// Get the coprocessor environment
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();

		// By calling this method, the client indicates its inability to merge
		// store-reads with
		// snapshots. Its betting on the fact that, items in the snapshot may not be
		// used by the
		// client. Could be a performance optimization for Orders trx, where only
		// order entries
		// are added to the store; no need for any reads.

		// We just return the syncTs as the startTime.
		Get g = new Get(id.getKey());
		g.addColumn(WALTableProperties.WAL_FAMILY, WALTableProperties.SYNC_TS_COL);
		g.setTimeStamp(WALTableProperties.GENERIC_TIMESTAMP);

		Result r = env.getRegion().get(g, null);
		long syncTs = Bytes.toLong(r.getValue(WALTableProperties.WAL_FAMILY,
				WALTableProperties.SYNC_TS_COL));

		return syncTs;
	}

	@Override
	public boolean commit(LogId id, Check check, List<Write> writes,
			List<ImmutableBytesWritable> toBeUnlockedKeys,
			List<Integer> commitTypeInfo) throws IOException {
		sysout("Committing at logId: " + id.toString());
		// get the coprocessor environment
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HRegion region = env.getRegion();

		HashedBytes rowKey = new HashedBytes(id.getKey());
		CountDownLatch rowLatch = new CountDownLatch(1);
		final int DEFAULT_ROWLOCK_WAIT_DURATION = 30000;

		// loop until we acquire the row lock (unless !waitForLock)
		while (true) {
			CountDownLatch existingLatch = lockedRows.putIfAbsent(rowKey, rowLatch);
			if (existingLatch == null) {
				break;
			} else {
				// row already locked
				try {
					if (!existingLatch.await(DEFAULT_ROWLOCK_WAIT_DURATION,
							TimeUnit.MILLISECONDS)) {
						return false;
					}
				} catch (InterruptedException ie) {
					// Empty
				}
			}
		}

		try {
			// A lock acquired above is similar to the row lock for the row containing
			// all WAL related info. So, we can now fetch the CURRENT_TS, SYNC_TS, and
			// OLDEST_TS from the row, without caring for someone else changing them
			// after
			// we've read them, as no one else can modify the row's content.
			// Process the conflict detection check.
			// Get g = new Get(id.getKey());
			// g.addColumn(WALTableProperties.WAL_FAMILY,
			// WALTableProperties.OLDEST_TS_COL);
			// g.addColumn(WALTableProperties.WAL_FAMILY,
			// WALTableProperties.CURRENT_TS_COL);
			// g.setTimeStamp(WALTableProperties.GENERIC_TIMESTAMP);
			//
			// Result r = env.getRegion().get(g, null);
			// long oldestTs = Bytes.toLong(r.getValue(WALTableProperties.WAL_FAMILY,
			// WALTableProperties.OLDEST_TS_COL));
			// long currentTs = Bytes.toLong(r.getValue(WALTableProperties.WAL_FAMILY,
			// WALTableProperties.CURRENT_TS_COL));

			// We get the timestamps from myKVSpace.
			String key = Bytes.toString(id.getKey())
					+ Bytes.toString(WALTableProperties.WAL_FAMILY)
					+ Bytes.toString(WALTableProperties.OLDEST_TS_COL);
			byte[] val = myKVSpace.get(key).get(WALTableProperties.GENERIC_TIMESTAMP);
			long oldestTs = Bytes.toLong(val);

			key = Bytes.toString(id.getKey())
					+ Bytes.toString(WALTableProperties.WAL_FAMILY)
					+ Bytes.toString(WALTableProperties.CURRENT_TS_COL);
			val = myKVSpace.get(key).get(WALTableProperties.GENERIC_TIMESTAMP);
			long currentTs = Bytes.toLong(val);

			// if the oldestTs > check.timestamp, then we return false, as trx is very
			// old
			// and we don't have sufficient info to judge its correctness.
			if (oldestTs > check.getTimestamp())
				return false;

			// Grab all the LogEntries at timestamps > check.timestamp && <=
			// currentTs.
			// Get getLogEntries = new Get(id.getKey());
			// getLogEntries.addColumn(WALTableProperties.WAL_FAMILY,
			// WALTableProperties.WAL_ENTRY_COL);
			// // In timerange, left is inclusive and right is exclusive.
			// getLogEntries.setTimeRange(check.getTimestamp() + 1, currentTs + 1);
			// getLogEntries.setMaxVersions();
			// Result logEntries = env.getRegion().get(getLogEntries, null);

			// if (!logEntries.isEmpty()) {
			// sysout("IN COMMIT CHECK: checkTimestamp: "
			// + check.getTimestamp() + ", currentTs: " + currentTs);
			// sysout("IN COMMIT CHECK: check object contains: "
			// + check.toString());
			// }

			// From myKVSpace, grab all the LogEntries at timestamps > check.timestamp
			// && <=
			// currentTs.
			key = Bytes.toString(id.getKey())
					+ Bytes.toString(WALTableProperties.WAL_FAMILY)
					+ Bytes.toString(WALTableProperties.WAL_ENTRY_COL);
			TreeMap<Long, byte[]> timestampMap = myKVSpace.get(key);
			if (timestampMap != null) {
				NavigableMap<Long, byte[]> subMap = timestampMap.subMap(check
						.getTimestamp(), false, currentTs, true);

				if (!subMap.isEmpty()) {
					sysout("IN COMMIT CHECK: checkTimestamp: " + check.getTimestamp()
							+ ", currentTs: " + currentTs);
					sysout("IN COMMIT CHECK: check object contains: " + check.toString());
				}

				// Check if readSet overlaps with the writes that occurred in the
				// meantime.
				// Return false even if one conflict is detected.
				if (check.getReadSets() != null) {
					for (Map.Entry<Long, byte[]> entry : subMap.entrySet()) {
						LogEntry logEntry = LogEntry.fromBytes(entry.getValue());
						for (ReadSet readSet : check.getReadSets()) {
							byte[] name = readSet.getName();
							Set<ImmutableBytesWritable> keySet = readSet.getKeys();
							if (keySet != null) {
								for (Write w : logEntry.getWrites()) {
									if (Bytes.compareTo(name, w.getName()) == 0
											&& keySet
													.contains(new ImmutableBytesWritable(w.getKey()))) {
										return false;
									}
								}
							}
						}
					}
				}
			}
			// // Check if readSet overlaps with the writes that occurred in the
			// // meantime.
			// // Return false even if one conflict is detected.
			// for (KeyValue kv : logEntries.raw()) {
			// LogEntry logEntry = LogEntry.fromBytes(kv.getValue());
			// if (check.getReadSets() != null) {
			// for (ReadSet readSet : check.getReadSets()) {
			// byte[] name = readSet.getName();
			// Set<ImmutableBytesWritable> keySet = readSet.getKeys();
			// if (keySet != null) {
			// for (Write w : logEntry.getWrites()) {
			// if (Bytes.compareTo(name, w.getName()) == 0
			// && keySet.contains(new ImmutableBytesWritable(w.getKey()))) {
			// return false;
			// }
			// }
			// }
			// }
			// }
			// }

			// If all checks passed, commit the LogEntry, and also the update the
			// other
			// timestamp fields.
			// Increase the currentTs for every commit.
			currentTs++;
			LogEntry newLogEntry = new LogEntry();
			newLogEntry.setTimestamp(currentTs);
			sysout("Adding the following writes to the log entry at timestamp: "
					+ currentTs);
			for (Write w : writes)
				sysout(w.toString());

			newLogEntry.setWrites(writes);

			// First persisting to Write-ahead-log of HBase and then writing to
			// myKVSpace.
			// Once we move to a consistent hashing style mechanism, we
			// should switch
			// to the "writing to slaves" mechanism for persistance.
			// TODO: This WALEdit should also contain all the information related to
			// "Causal Lock Updates";
			// they are supposed to be first committed onto the local WAL and then
			// flushed onto the
			// remote WALs.
			WALEdit walEdit = new WALEdit();
			long now = EnvironmentEdgeManager.currentTimeMillis();
			HLog log = env.getRegion().getLog();
			HTableDescriptor htd = new HTableDescriptor(
					WALTableProperties.dataTableName);

			for (Write w : writes) {
				KeyValue kv = new KeyValue(w.getKey(), WALTableProperties.dataFamily, w
						.getName(), w.getValue());
				walEdit.add(kv);
			}
			log.append(env.getRegion().getRegionInfo(),
					WALTableProperties.dataTableName, walEdit, now, htd);

			// Put commitPut = new Put(id.getKey());
			// commitPut.add(WALTableProperties.WAL_FAMILY,
			// WALTableProperties.CURRENT_TS_COL,
			// WALTableProperties.GENERIC_TIMESTAMP, Bytes.toBytes(currentTs));
			// commitPut.add(WALTableProperties.WAL_FAMILY,
			// WALTableProperties.WAL_ENTRY_COL, currentTs, newLogEntry.toBytes());
			// env.getRegion().put(commitPut);

			// Writing to myKVSpace instead of HBase.
			// Each HBase key with its family and column will become one key for
			// myKVSpace.
			key = Bytes.toString(id.getKey())
					+ Bytes.toString(WALTableProperties.WAL_FAMILY)
					+ Bytes.toString(WALTableProperties.CURRENT_TS_COL);
			val = Bytes.toBytes(currentTs);
			timestampMap = myKVSpace.get(key);
			if (timestampMap == null) {
				timestampMap = new TreeMap<Long, byte[]>();
				myKVSpace.put(key, timestampMap);
			}
			timestampMap.put(WALTableProperties.GENERIC_TIMESTAMP, val);

			// next key,family,col,value
			key = Bytes.toString(id.getKey())
					+ Bytes.toString(WALTableProperties.WAL_FAMILY)
					+ Bytes.toString(WALTableProperties.WAL_ENTRY_COL);
			val = newLogEntry.toBytes();
			timestampMap = myKVSpace.get(key);
			if (timestampMap == null) {
				timestampMap = new TreeMap<Long, byte[]>();
				myKVSpace.put(key, timestampMap);
			}
			timestampMap.put(currentTs, val);

			// Flush these writes onto the local region, since we assume that data
			// items under a write-ahead-log
			// are placed in the same region hosting the WAL.
			flushLogEntryToLocalRegion(region, newLogEntry);

			// Unlock the keys in this commit by placing zero in the corresponding
			// log + key.
			HTable logTable = new HTable(this.conf, WALTableProperties.walTableName);
			logTable.setAutoFlush(false);

			// All unlocking and reseting goes into the first set.
			List<Row> firstSetOfCausalLockReleases = new LinkedList<Row>();
			// All deletes for remote, migrated locks go into the second set.
			List<Row> secondSetOfCausalLockReleases = new LinkedList<Row>();

			for (int index = 0; index < toBeUnlockedKeys.size(); index++) {
				// For every key, check the corresponding commitType in commitTypeInfo
				// list,
				// and perform the action accordingly.
				ImmutableBytesWritable toBeUnlockedDestKey = toBeUnlockedKeys
						.get(index);
				if (commitTypeInfo.get(index) == LogId.ONLY_DELETE) {
					sysout("ONLY DELETING KEY: "
							+ Bytes.toString(toBeUnlockedDestKey.get()));
					Delete delDest = new Delete(toBeUnlockedDestKey.get());
					delDest.deleteColumn(WALTableProperties.WAL_FAMILY,
							WALTableProperties.writeLockColumn,
							WALTableProperties.appTimestamp);
					delDest.deleteColumn(WALTableProperties.WAL_FAMILY,
							WALTableProperties.isLockMigratedColumn,
							WALTableProperties.appTimestamp);
					delDest.deleteColumn(WALTableProperties.WAL_FAMILY,
							WALTableProperties.isLockPlacedOrMigratedColumn,
							WALTableProperties.appTimestamp);
					delDest.deleteColumn(WALTableProperties.WAL_FAMILY,
							WALTableProperties.regionObserverMarkerColumn,
							WALTableProperties.appTimestamp);
					secondSetOfCausalLockReleases.add(delDest);
				} else if (commitTypeInfo.get(index) == LogId.ONLY_UNLOCK) {
					sysout("ONLY UNLOCKING KEY: "
							+ Bytes.toString(toBeUnlockedDestKey.get()));
					Put p = new Put(toBeUnlockedDestKey.get());
					p.add(WALTableProperties.WAL_FAMILY,
							WALTableProperties.writeLockColumn,
							WALTableProperties.appTimestamp, Bytes
									.toBytes(WALTableProperties.zero));
					p.add(WALTableProperties.WAL_FAMILY,
							WALTableProperties.isLockPlacedOrMigratedColumn,
							WALTableProperties.appTimestamp, Bytes
									.toBytes(WALTableProperties.zero));
					p.add(WALTableProperties.WAL_FAMILY,
							WALTableProperties.regionObserverMarkerColumn,
							WALTableProperties.appTimestamp, WALTableProperties.randomValue);
					if (HRegion.rowIsInRange(region.getRegionInfo(), toBeUnlockedDestKey
							.get())) {
						region.put(p);
					} else {
						firstSetOfCausalLockReleases.add(p);
					}
				} else if (commitTypeInfo.get(index) == LogId.UNLOCK_AND_RESET_MIGRATION) {
					sysout("UNLOCKING AND RESETING KEY: "
							+ Bytes.toString(toBeUnlockedDestKey.get()));
					Put pSrc = new Put(toBeUnlockedDestKey.get());
					pSrc.add(WALTableProperties.WAL_FAMILY,
							WALTableProperties.writeLockColumn,
							WALTableProperties.appTimestamp, Bytes
									.toBytes(WALTableProperties.zero));
					pSrc.add(WALTableProperties.WAL_FAMILY,
							WALTableProperties.isLockMigratedColumn,
							WALTableProperties.appTimestamp, Bytes
									.toBytes(WALTableProperties.zero));
					pSrc.add(WALTableProperties.WAL_FAMILY,
							WALTableProperties.isLockPlacedOrMigratedColumn,
							WALTableProperties.appTimestamp, Bytes
									.toBytes(WALTableProperties.zero));
					pSrc.add(WALTableProperties.WAL_FAMILY,
							WALTableProperties.regionObserverMarkerColumn,
							WALTableProperties.appTimestamp, WALTableProperties.randomValue);
					if (HRegion.rowIsInRange(region.getRegionInfo(), toBeUnlockedDestKey
							.get())) {
						region.put(pSrc);
					} else {
						firstSetOfCausalLockReleases.add(pSrc);
					}
				}
			}

			// Flush both sets of causal lock releases sequentially.
			if (!firstSetOfCausalLockReleases.isEmpty())
				logTable.batch(firstSetOfCausalLockReleases);
			if (!secondSetOfCausalLockReleases.isEmpty())
				logTable.batch(secondSetOfCausalLockReleases);

			logTable.flushCommits();
			logTable.close();

			// Since we performed our own log flush, change the syncTs to the
			// currentTs.
			// This will indirectly make the start() function return an empty snapshot
			// whenever
			// someone requests it. Another way is to delete the LogEntry from
			// myKVSpace, but we
			// won't do that as the LogEntry is useful in detecting conflicts during
			// commit.
			key = Bytes.toString(id.getKey())
					+ Bytes.toString(WALTableProperties.WAL_FAMILY)
					+ Bytes.toString(WALTableProperties.SYNC_TS_COL);
			val = Bytes.toBytes(currentTs);
			timestampMap = myKVSpace.get(key);
			if (timestampMap == null) {
				timestampMap = new TreeMap<Long, byte[]>();
				myKVSpace.put(key, timestampMap);
			}
			timestampMap.put(WALTableProperties.GENERIC_TIMESTAMP, val);

			/*
			 * // If its time to flush the log, if (currentTs %
			 * flushIntervalForTimestamps == 0) { toBeFlushedLogs.add(id); // Also
			 * inform the StoreUpdater to take care of flushing.
			 * this.storeUpdater.logFlushRequested(); }
			 */
		} catch (Exception e) {
			System.err.println("Error in commit function: " + e.getMessage());
			e.printStackTrace();
		} finally {
			// Give up the lock on the wal.
			// Latches are one-time gates. So we have to open the gate to release the
			// waiting
			// prisoners and then purge the gate completely.
			// New guy wanting the lock will install another gate.
			CountDownLatch internalRowLatch = lockedRows.remove(rowKey);
			internalRowLatch.countDown();
		}
		return true;
	}

	public void flushLogEntryToLocalRegion(HRegion region, LogEntry logEntry)
			throws IOException {
		WALEdit walEdit = new WALEdit();
		long now = EnvironmentEdgeManager.currentTimeMillis();
		HLog log = region.getLog();
		HTableDescriptor htd = new HTableDescriptor(
				WALTableProperties.dataTableName);

		for (Write w : logEntry.getWrites()) {
			KeyValue kv = new KeyValue(w.getKey(), WALTableProperties.dataFamily, w
					.getName(), w.getValue());
			walEdit.add(kv);
		}
		log.append(region.getRegionInfo(), WALTableProperties.dataTableName,
				walEdit, now, htd);

		for (Write w : logEntry.getWrites()) {
			String writeName = Bytes.toString(w.getName());
			String[] tokens = writeName.split("[" + Write.nameDelimiter + "]+");
			String tableName = tokens[0];
			byte[] family = Bytes.toBytes(tokens[1]);
			byte[] qualifier = Bytes.toBytes(tokens[2]);
			byte[] key = w.getKey();
			byte[] value = w.getValue();

			Put p = new Put(key);
			p.setWriteToWAL(false);
			p.add(family, qualifier, WALTableProperties.appTimestamp, value);
			region.put(p, false);
		}
	}

	public void flushLogs() throws IOException {
		// get the coprocessor environment
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HTablePool tablePool = new HTablePool(this.conf, Integer.MAX_VALUE,
				new HTableFactory() {
					public HTableInterface createHTableInterface(Configuration config,
							byte[] tableName) {
						try {
							HTable table = new HTable(config, tableName);
							table.setAutoFlush(false);
							return table;
						} catch (IOException ioe) {
							throw new RuntimeException(ioe);
						}
					}
				});

		// Do not acquire any locks, as the log flushing is not needed for
		// application correctness.
		LogId id = null;
		try {
			while ((id = toBeFlushedLogs.peek()) != null) {
				Get g = new Get(id.getKey());
				g.addColumn(WALTableProperties.WAL_FAMILY,
						WALTableProperties.SYNC_TS_COL);
				g.addColumn(WALTableProperties.WAL_FAMILY,
						WALTableProperties.CURRENT_TS_COL);
				g.setTimeStamp(WALTableProperties.GENERIC_TIMESTAMP);

				Result r = env.getRegion().get(g, null);
				long syncTs = 0;
				long currentTs = 0;

				// Grab the currentTs and syncTs from the table.
				syncTs = Bytes.toLong(r.getValue(WALTableProperties.WAL_FAMILY,
						WALTableProperties.SYNC_TS_COL));
				currentTs = Bytes.toLong(r.getValue(WALTableProperties.WAL_FAMILY,
						WALTableProperties.CURRENT_TS_COL));
				// There is nothing to do if the syncTs and currentTs have the same
				// value.
				if (syncTs == currentTs)
					return;

				// Grab all the LogEntries at timestamps > syncTs && <= currentTs.
				Get getLogEntries = new Get(id.getKey());
				getLogEntries.addColumn(WALTableProperties.WAL_FAMILY,
						WALTableProperties.WAL_ENTRY_COL);
				// In timerange, left is inclusive and right is exclusive.
				sysout("Inside Flush: Setting timerange: " + (syncTs + 1) + " - "
						+ (currentTs + 1));
				getLogEntries.setTimeRange(syncTs + 1, currentTs + 1);
				getLogEntries.setMaxVersions(Integer.MAX_VALUE);
				Result logEntries = env.getRegion().get(getLogEntries, null);

				// Add the writes from all logEntries into the Snapshot.
				NavigableMap<Long, byte[]> timestampMap = logEntries.getMap().get(
						WALTableProperties.WAL_FAMILY)
						.get(WALTableProperties.WAL_ENTRY_COL);
				sysout("Inside Flush: Nb of log entries retrieved for snapshot: "
						+ timestampMap.size());
				Map<String, Write> writeMap = new HashMap<String, Write>();
				for (Long timestamp : timestampMap.keySet()) {
					LogEntry logEntry = LogEntry.fromBytes(timestampMap.get(timestamp));
					for (Write w : logEntry.getWrites()) {
						if (writeMap.get(w.getNameAndKey()) != null)
							// Another entry with a greater timestamp is already persent.
							continue;
						sysout("Inside Flush: Adding a write to the snapshot: "
								+ w.toString());
						writeMap.put(w.getNameAndKey(), w);
					}
				}

				Map<HTable, HTable> usedTables = new HashMap<HTable, HTable>();

				// Once we collected all writes, we just need to flush.
				for (Map.Entry<String, Write> writeEntry : writeMap.entrySet()) {
					Write w = writeEntry.getValue();
					String writeName = Bytes.toString(w.getName());
					String[] tokens = writeName.split("[" + Write.nameDelimiter + "]+");
					String tableName = tokens[0];
					byte[] family = Bytes.toBytes(tokens[1]);
					byte[] qualifier = Bytes.toBytes(tokens[2]);
					byte[] key = w.getKey();
					byte[] value = w.getValue();

					HTable table = (HTable) tablePool.getTable(tableName);
					Put p = new Put(key);
					p.add(family, qualifier, WALTableProperties.appTimestamp, value);
					table.put(p);
					usedTables.put(table, table);
				}

				// flush all the used tables before changing the syncTs on
				// the log.
				for (HTable table : usedTables.keySet()) {
					table.flushCommits();
				}

				// Change the syncTs to the currentTs obtained by this function. Note
				// that
				// currentTs
				// might have changed after we've fetched it, since this log flusher
				// does
				// not
				// block the transactions by acquiring any locks.
				Put commitPut = new Put(id.getKey());
				commitPut.add(WALTableProperties.WAL_FAMILY,
						WALTableProperties.SYNC_TS_COL,
						WALTableProperties.GENERIC_TIMESTAMP, Bytes.toBytes(currentTs));
				env.getRegion().put(commitPut);
			}
		} catch (Exception e) {
			System.err.println("Error in flushLogs: " + e.getMessage());
			e.printStackTrace();
		} finally {
			// If the put was successful, then remove the logId from toBeFlushedLogs
			// list.
			sysout("Flushed log to store: " + id.toString());
			toBeFlushedLogs.poll();

			// Close all the tables.
			tablePool.close();
		}
	}

	// A lock gets addded (through migration) only when it is unlocked -- i.e.,
	// value can be
	// assumed to be zero.
	public boolean addLock(LogId logId, ImmutableBytesWritable key)
			throws IOException {
		String localKey = Bytes.toString(logId.getKey())
				+ Bytes.toString(key.get())
				+ Bytes.toString(WALTableProperties.dataFamily)
				+ Bytes.toString(WALTableProperties.writeLockColumn);
		byte[] val = Bytes.toBytes(WALTableProperties.zero);
		TreeMap<Long, byte[]> timestampMap = myKVSpace.get(localKey);
		if (timestampMap == null) {
			timestampMap = new TreeMap<Long, byte[]>();
			myKVSpace.put(localKey, timestampMap);
		}
		timestampMap.put(WALTableProperties.GENERIC_TIMESTAMP, val);
		return true;
	}

	// The localKey (logId along with key) is locked when indirection is being
	// placed
	// or it is being locked. Thus indirection is attempted in the first place
	// only if there is no lock.
	public ImmutableBytesWritable migrateLock(final Long transactionId,
			final LogId logId, final ImmutableBytesWritable key,
			final LogId destLogId, final ImmutableBytesWritable destKey)
			throws IOException {
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HRegion region = env.getRegion();

		byte[] tableCachedLock = Bytes.toBytes(Bytes.toString(logId.getKey())
				+ WALTableProperties.logAndKeySeparator + Bytes.toString(key.get()));

		String localKey = Bytes.toString(logId.getKey())
				+ WALTableProperties.logAndKeySeparator + Bytes.toString(key.get())
				+ Bytes.toString(WALTableProperties.WAL_FAMILY)
				+ Bytes.toString(WALTableProperties.writeLockColumn);
		HashedBytes rowKey = new HashedBytes(Bytes.toBytes(localKey));
		CountDownLatch rowLatch = new CountDownLatch(1);

		CountDownLatch existingLatch = lockedRows.get(rowKey);

		boolean canAttemptMigration = true;

		// The only way to check if a migration can be attempted is to directly do a
		// Get
		// on the lock-object and see if its isLockPlacedOrMigrated field is 0. If
		// it is, then
		// attempt migration. Otherwise, set canAttemptMigration to false. Later, at
		// the end
		// of this function.
		Get g = new Get(tableCachedLock);
		g.addColumn(WALTableProperties.WAL_FAMILY,
				WALTableProperties.writeLockColumn);
		g.addColumn(WALTableProperties.WAL_FAMILY,
				WALTableProperties.isLockMigratedColumn);
		g.addColumn(WALTableProperties.WAL_FAMILY,
				WALTableProperties.isLockPlacedOrMigratedColumn);
		g.addColumn(WALTableProperties.WAL_FAMILY,
				WALTableProperties.destinationKeyColumn);
		g.addColumn(WALTableProperties.WAL_FAMILY,
				WALTableProperties.regionObserverMarkerColumn);
		g.setTimeStamp(WALTableProperties.appTimestamp);
		Result r = null;
		if (HRegion.rowIsInRange(region.getRegionInfo(), tableCachedLock)) {
			r = region.get(g, null);
			if (!r.isEmpty()) {
				long isLockPlacedOrMigratedColumnInfo = Bytes.toLong(r.getValue(
						WALTableProperties.WAL_FAMILY,
						WALTableProperties.isLockPlacedOrMigratedColumn));

				if (isLockPlacedOrMigratedColumnInfo == WALTableProperties.one) {
					// Some other transaction placed a migration or locked in-place.
					canAttemptMigration = false;
				}
			}
		}

		sysout("Adding a lock at the remote logId: " + destLogId.toString());

		// Migrate the lock as it is unlocked and no one is attempting to do it.
		// Migration happens in two steps:
		// 1. Without acquiring lock, we just add a lock for key at destKey (at
		// destLog).
		// 2. Then we acquire our lock and then place the indirection.
		String destination = Bytes.toString(destLogId.getKey())
				+ WALTableProperties.logAndKeySeparator + Bytes.toString(destKey.get());

		byte[] selfPlacedDestinationKey = Bytes.toBytes(destination);
		boolean migrationResult = false;

		// TODO: Make this a global variable. Aren't we using a HTable in
		// StoreUpdater?
		HTable logTable = new HTable(this.conf, WALTableProperties.walTableName);

		if (canAttemptMigration) {
			Put p = new Put(selfPlacedDestinationKey);
			p.add(WALTableProperties.WAL_FAMILY, WALTableProperties.writeLockColumn,
					WALTableProperties.appTimestamp, Bytes
							.toBytes(WALTableProperties.zero));
			p.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.isLockMigratedColumn,
					WALTableProperties.appTimestamp, Bytes
							.toBytes(WALTableProperties.two));
			p.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.isLockPlacedOrMigratedColumn,
					WALTableProperties.appTimestamp, Bytes
							.toBytes(WALTableProperties.zero));
			p.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.regionObserverMarkerColumn, WALTableProperties.randomValue);
			logTable.put(p);
			logTable.flushCommits();

			sysout("Finished adding a lock at the remote logId for tableCachedLock: "
					+ Bytes.toString(tableCachedLock));
			// For now, we don't do any in-memory indirection placement because then
			// when the locks are removed, we'll also have to remove this in-memory
			// indirection
			// which will need coprocessor call from another coprocessor.

			// We shall also create a key in HTable for indirection because locking
			// is
			// being performed
			// on HTable, as HBase isn't allowing calling another coprocessor
			// through
			// a coprocessor.
			// TODO: Think about the need for both (in-memory indirection placement
			// --
			// as done above,
			// and also an indirection in HTable). We probably only need one.
			// Can we make this a function call through HRegion?
			// TODO: When multiple clients exist, we'll have to make the indirection
			// placement
			// through checkAndSet and remove this synchronization through in-memory
			// hash maps.
			// Basically, this whole function can be written as a pure HTable based
			// migration.
			p = new Put(tableCachedLock);
			p.add(WALTableProperties.WAL_FAMILY, WALTableProperties.writeLockColumn,
					WALTableProperties.appTimestamp, Bytes.toBytes(transactionId));
			p.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.isLockMigratedColumn,
					WALTableProperties.appTimestamp, Bytes
							.toBytes(WALTableProperties.one));
			p.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.isLockPlacedOrMigratedColumn,
					WALTableProperties.appTimestamp, Bytes
							.toBytes(WALTableProperties.one));
			p.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.destinationKeyColumn,
					WALTableProperties.appTimestamp, selfPlacedDestinationKey);
			p.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.regionObserverMarkerColumn, WALTableProperties.randomValue);
			if (HRegion.rowIsInRange(region.getRegionInfo(), tableCachedLock)) {
				migrationResult = region.checkAndMutate(tableCachedLock,
						WALTableProperties.WAL_FAMILY,
						WALTableProperties.isLockPlacedOrMigratedColumn,
						CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes
								.toBytes(WALTableProperties.zero)), p, null, true);
				sysout("For tableCachedLock: " + Bytes.toString(tableCachedLock)
						+ ", CheckAndMutate succeeded and so we placed the migration info ");
			} else {
				migrationResult = logTable.checkAndPut(tableCachedLock,
						WALTableProperties.WAL_FAMILY,
						WALTableProperties.isLockPlacedOrMigratedColumn, Bytes
								.toBytes(WALTableProperties.zero), p);
				sysout("For tableCachedLock: "
						+ Bytes.toString(tableCachedLock)
						+ ", CheckAndPut succeeded through logTable and so we placed the migration info.");
			}

			if (migrationResult == false) {
				g = new Get(tableCachedLock);
				g.addColumn(WALTableProperties.WAL_FAMILY,
						WALTableProperties.writeLockColumn);
				g.addColumn(WALTableProperties.WAL_FAMILY,
						WALTableProperties.isLockMigratedColumn);
				g.addColumn(WALTableProperties.WAL_FAMILY,
						WALTableProperties.isLockPlacedOrMigratedColumn);
				g.addColumn(WALTableProperties.WAL_FAMILY,
						WALTableProperties.destinationKeyColumn);
				g.addColumn(WALTableProperties.WAL_FAMILY,
						WALTableProperties.regionObserverMarkerColumn);
				g.setTimeStamp(WALTableProperties.appTimestamp);
				if (HRegion.rowIsInRange(region.getRegionInfo(), tableCachedLock)) {
					r = region.get(g, null);
					// If r is empty, that means there was no lock created in the first
					// place.
					// Thus create it and mark it as migrated.
					if (r.isEmpty()) {
						sysout("NO TABLE_CACHED_LOCK found; we are inserting it ourself!");
						region.put(p);
						migrationResult = true;
					}
				}
			}

			if (migrationResult == true) {
				sysout("PLACED A DETOUR AT CACHED LOCK: "
						+ Bytes.toString(tableCachedLock));
				return new ImmutableBytesWritable(selfPlacedDestinationKey);
			}
		}

		// If the function reached this line, then we're either not allowed to
		// migrate
		// or that migration failed. In either case, read the present migration
		// information
		// on the lock and return that to client.

		// The key to which this lock was migrated by some other transaction.
		// If this lock is acquired by some other trx, then existingDestinationKey
		// will be this same tableCachedLock.
		byte[] existingDestinationKey = tableCachedLock;

		if (migrationResult == false) {
			g = new Get(tableCachedLock);
			g.addColumn(WALTableProperties.WAL_FAMILY,
					WALTableProperties.writeLockColumn);
			g.addColumn(WALTableProperties.WAL_FAMILY,
					WALTableProperties.isLockMigratedColumn);
			g.addColumn(WALTableProperties.WAL_FAMILY,
					WALTableProperties.isLockPlacedOrMigratedColumn);
			g.addColumn(WALTableProperties.WAL_FAMILY,
					WALTableProperties.destinationKeyColumn);
			g.addColumn(WALTableProperties.WAL_FAMILY,
					WALTableProperties.regionObserverMarkerColumn);
			g.setTimeStamp(WALTableProperties.appTimestamp);
			if (HRegion.rowIsInRange(region.getRegionInfo(), tableCachedLock)) {
				r = region.get(g, null);
				if (!r.isEmpty()) {
					// Check for migration info and write lock. If some one else
					// migrated, note
					// that destinationKey and send it back to client. Otherwise, if no
					// one
					// migrated and someone has placed a lock, then come back to this
					// same
					// lock for acquisition.
					long isLockMigratedColumnInfo = Bytes.toLong(r.getValue(
							WALTableProperties.WAL_FAMILY,
							WALTableProperties.isLockMigratedColumn));

					long lockInfo = Bytes.toLong(r
							.getValue(WALTableProperties.WAL_FAMILY,
									WALTableProperties.writeLockColumn));
					if (isLockMigratedColumnInfo == WALTableProperties.one) {
						// Some other transaction placed a migration.
						existingDestinationKey = r.getValue(WALTableProperties.WAL_FAMILY,
								WALTableProperties.destinationKeyColumn);
					} else {
						// Come back to this same lock.
						existingDestinationKey = tableCachedLock;
					}
				}
			}
		}

		if (migrationResult == false) {
			sysout("COULD NOT PLACE A DETOUR AT CACHED LOCK: "
					+ Bytes.toString(tableCachedLock));
		}

		return new ImmutableBytesWritable(existingDestinationKey);
	}
}
