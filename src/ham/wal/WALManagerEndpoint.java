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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.util.Bytes;
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
public class WALManagerEndpoint extends BaseEndpointCoprocessor implements
		WALManagerProtocol {
	private static final Log LOG = LogFactory
			.getLog("ham.wal.WALManagerEndpoint");
	private final ConcurrentHashMap<HashedBytes, CountDownLatch> lockedRows = new ConcurrentHashMap<HashedBytes, CountDownLatch>();

	// To-be-flushed-logs. Whenever the currentTs for a log exceeds some periodic
	// number, the thread executing that transaction puts the log into this list
	// and
	// interrupts the store-updater. The store-updater reads this list and flushes
	// the individual logs.
	private final ConcurrentLinkedQueue<LogId> toBeFlushedLogs = new ConcurrentLinkedQueue<LogId>();
	int flushIntervalForTimestamps = 400;

	protected Configuration conf = null;
	private StoreUpdater storeUpdater = null;
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
		this.storeUpdater = new StoreUpdater(this, 10 * 60 * 1000, 50 * 60 * 1000);

		UncaughtExceptionHandler handler = new UncaughtExceptionHandler() {
			public void uncaughtException(Thread t, Throwable e) {
				System.err.println("Uncaught exception in service thread "
						+ t.getName() + e);
			}
		};
		Threads.setDaemonThreadRunning(this.storeUpdater.getThread(),
				".StoreUpdater", handler);
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

			Get g = new Get(id.getKey());
			g
					.addColumn(WALTableProperties.WAL_FAMILY,
							WALTableProperties.SYNC_TS_COL);
			g.addColumn(WALTableProperties.WAL_FAMILY,
					WALTableProperties.CURRENT_TS_COL);
			g.setTimeStamp(WALTableProperties.GENERIC_TIMESTAMP);

			Result r = env.getRegion().get(g, null);
			long syncTs = 0;
			long currentTs = 0;
			if (r.isEmpty()) {
				sysout("Result is empty");
				// Add a new WAL to the table with the given id.
				Put p = new Put(id.getKey());
				p.add(WALTableProperties.WAL_FAMILY, WALTableProperties.SYNC_TS_COL,
						WALTableProperties.GENERIC_TIMESTAMP, Bytes.toBytes(syncTs));
				p.add(WALTableProperties.WAL_FAMILY, WALTableProperties.CURRENT_TS_COL,
						WALTableProperties.GENERIC_TIMESTAMP, Bytes.toBytes(currentTs));
				p.add(WALTableProperties.WAL_FAMILY, WALTableProperties.OLDEST_TS_COL,
						WALTableProperties.GENERIC_TIMESTAMP, Bytes.toBytes(syncTs));
				env.getRegion().put(p);

				Map<String, Write> writeMap = new HashMap<String, Write>();
				snapshot.setTimestamp(currentTs);
				sysout("Setting timestamp for snapshot: " + currentTs);
				snapshot.setWriteMap(writeMap);
				return snapshot;
			}
			// Grab the currentTs and syncTs from the table.
			syncTs = Bytes.toLong(r.getValue(WALTableProperties.WAL_FAMILY,
					WALTableProperties.SYNC_TS_COL));
			currentTs = Bytes.toLong(r.getValue(WALTableProperties.WAL_FAMILY,
					WALTableProperties.CURRENT_TS_COL));
			// We should return with empty snapshot if currentTs is equal to syncTs.
			if (currentTs == syncTs) {
				Map<String, Write> writeMap = new HashMap<String, Write>();
				snapshot.setTimestamp(currentTs);
				sysout("Setting timestamp for snapshot: " + currentTs);
				snapshot.setWriteMap(writeMap);
				return snapshot;
			}

			// Grab all the LogEntries at timestamps > syncTs && <= currentTs.
			Get getLogEntries = new Get(id.getKey());
			getLogEntries.addColumn(WALTableProperties.WAL_FAMILY,
					WALTableProperties.WAL_ENTRY_COL);
			// In timerange, left is inclusive and right is exclusive.
			sysout("For snapshot: Setting timerange: " + (syncTs + 1) + " - "
					+ (currentTs + 1));
			getLogEntries.setTimeRange(syncTs + 1, currentTs + 1);
			getLogEntries.setMaxVersions(Integer.MAX_VALUE);
			Result logEntries = env.getRegion().get(getLogEntries, null);

			// Add the writes from all logEntries into the Snapshot.
			NavigableMap<Long, byte[]> timestampMap = logEntries.getMap().get(
					WALTableProperties.WAL_FAMILY).get(WALTableProperties.WAL_ENTRY_COL);
			sysout("Nb of log entries retrieved for snapshot: " + timestampMap.size());
			Map<String, Write> writeMap = new HashMap<String, Write>();
			for (Long timestamp : timestampMap.keySet()) {
				sysout("Reading logEntry with timestamp: " + timestamp);
				LogEntry logEntry = LogEntry.fromBytes(timestampMap.get(timestamp));
				for (Write w : logEntry.getWrites()) {
					if (writeMap.get(w.getNameAndKey()) != null)
						// Another entry with a greater timestamp is already persent.
						continue;
					// System.out.println("Adding a write to the snapshot: " +
					// w.toString());
					writeMap.put(w.getNameAndKey(), w);
				}
			}

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
		// get the coprocessor environment
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();

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
			Get g = new Get(id.getKey());
			g.addColumn(WALTableProperties.WAL_FAMILY,
					WALTableProperties.OLDEST_TS_COL);
			g.addColumn(WALTableProperties.WAL_FAMILY,
					WALTableProperties.CURRENT_TS_COL);
			g.setTimeStamp(WALTableProperties.GENERIC_TIMESTAMP);

			Result r = env.getRegion().get(g, null);
			long oldestTs = Bytes.toLong(r.getValue(WALTableProperties.WAL_FAMILY,
					WALTableProperties.OLDEST_TS_COL));
			long currentTs = Bytes.toLong(r.getValue(WALTableProperties.WAL_FAMILY,
					WALTableProperties.CURRENT_TS_COL));

			// if the oldestTs > check.timestamp, then we return false, as trx is very
			// old
			// and we don't have sufficient info to judge its correctness.
			if (oldestTs > check.getTimestamp())
				return false;

			// Grab all the LogEntries at timestamps > check.timestamp && <=
			// currentTs.
			Get getLogEntries = new Get(id.getKey());
			getLogEntries.addColumn(WALTableProperties.WAL_FAMILY,
					WALTableProperties.WAL_ENTRY_COL);
			// In timerange, left is inclusive and right is exclusive.
			getLogEntries.setTimeRange(check.getTimestamp() + 1, currentTs + 1);
			getLogEntries.setMaxVersions();
			Result logEntries = env.getRegion().get(getLogEntries, null);

			if (!logEntries.isEmpty()) {
				sysout("IN COMMIT CHECK: checkTimestamp: " + check.getTimestamp()
						+ ", currentTs: " + currentTs);
				sysout("IN COMMIT CHECK: check object contains: " + check.toString());
			}

			// Check if readSet overlaps with the writes that occurred in the
			// meantime.
			// Return false even if one conflict is detected.
			for (KeyValue kv : logEntries.raw()) {
				LogEntry logEntry = LogEntry.fromBytes(kv.getValue());
				if (check.getReadSets() != null) {
					for (ReadSet readSet : check.getReadSets()) {
						byte[] name = readSet.getName();
						Set<ImmutableBytesWritable> keySet = readSet.getKeys();
						if (keySet != null) {
							for (Write w : logEntry.getWrites()) {
								if (Bytes.compareTo(name, w.getName()) == 0
										&& keySet.contains(new ImmutableBytesWritable(w.getKey()))) {
									return false;
								}
							}
						}
					}
				}
			}

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

			Put commitPut = new Put(id.getKey());
			commitPut.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.CURRENT_TS_COL,
					WALTableProperties.GENERIC_TIMESTAMP, Bytes.toBytes(currentTs));
			commitPut.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.WAL_ENTRY_COL, currentTs, newLogEntry.toBytes());
			env.getRegion().put(commitPut);

			// If its time to flush the log,
			if (currentTs % flushIntervalForTimestamps == 0) {
				toBeFlushedLogs.add(id);
				// Also inform the StoreUpdater to take care of flushing.
				this.storeUpdater.logFlushRequested();
			}
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

	@Override
	public boolean addLock(LogId logId, ImmutableBytesWritable key)
			throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ImmutableBytesWritable migrateLock(Long transactionId, LogId logId,
			ImmutableBytesWritable key, LogId destLogId,
			ImmutableBytesWritable destKey) throws IOException {
		// TODO Auto-generated method stub
		return new ImmutableBytesWritable();
	}
}
