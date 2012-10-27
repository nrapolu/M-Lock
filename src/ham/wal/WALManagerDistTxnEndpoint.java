package ham.wal;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.hadoop.hbase.util.Pair;

public class WALManagerDistTxnEndpoint extends WALManagerEndpointForMyKVSpace
		implements WALManagerDistTxnProtocol {

	byte[] dataFamily = WALTableProperties.dataFamily;
	byte[] dataColumn = WALTableProperties.dataColumn;
	byte[] versionColumn = WALTableProperties.versionColumn;
	byte[] writeLockColumn = WALTableProperties.writeLockColumn;
	byte[] walTableName = WALTableProperties.walTableName;
	byte[] logFamily = WALTableProperties.WAL_FAMILY;
	long appTimestamp = WALTableProperties.appTimestamp;

	private static final Log LOG = LogFactory
			.getLog("ham.wal.WALManagerDistTxnEndpoint");

	ExecutorService pool = Executors.newCachedThreadPool();

	// new ThreadPoolExecutor(10, Integer.MAX_VALUE,
	// 600, TimeUnit.SECONDS,
	// new SynchronousQueue<Runnable>(),
	// new DaemonThreadFactory());

	public static void sysout(String otp) {
		// System.out.println(otp);
	}

	@Override
	public List<Long> commitRequestAcquireLocks(Long transactionId,
			List<ImmutableBytesWritable> keys) throws IOException {
		long zero = 0;
		// Creating a HTablePool just in case we need to pull in a HTable to acquire
		// the lock for
		// the key, if the key does not fall in the region's key-range.
		HTablePool tablePool = new HTablePool(super.conf, Integer.MAX_VALUE,
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

		// Get the coprocessor environment
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HRegion region = env.getRegion();
		long count = 0;
		long otherTrxId = 0;
		for (ImmutableBytesWritable key : keys) {
			sysout("Acquiring lock for key: " + Bytes.toString(key.get()));
			boolean isLockPlaced = false;
			if (HRegion.rowIsInRange(region.getRegionInfo(), key.get())) {
				isLockPlaced = region.checkAndMutate(key.get(), dataFamily,
						writeLockColumn, CompareFilter.CompareOp.EQUAL,
						new BinaryComparator(Bytes.toBytes(zero)), new Put(key.get()).add(
								dataFamily, writeLockColumn, appTimestamp, Bytes
										.toBytes(transactionId)), null, true);
				// If there is already another lock placed, then go get the
				// transactionId which placed that
				// lock. We send this trxId to the client which can roll-forward the trx
				// before re-trying to
				// get this lock.
				if (!isLockPlaced) {
					sysout("Could not obtain lock for key: " + Bytes.toString(key.get()));
					Get g = new Get(key.get());
					g.addColumn(dataFamily, writeLockColumn);
					Result r = region.get(g, null);
					otherTrxId = Bytes.toLong(r.getValue(dataFamily, writeLockColumn));
					sysout("OtherTrxId holding the lock is: " + otherTrxId);
				}
			} else {
				sysout("Not in region, hence sending a request to lock through HTable, for key: "
						+ Bytes.toString(key.get()));
				HTable table = (HTable) tablePool
						.getTable(WALTableProperties.dataTableName);
				isLockPlaced = table.checkAndPut(key.get(), dataFamily,
						writeLockColumn, Bytes.toBytes(zero), new Put(key.get()).add(
								dataFamily, writeLockColumn, appTimestamp, Bytes
										.toBytes(transactionId)));

				if (!isLockPlaced) {
					Get g = new Get(key.get());
					g.addColumn(dataFamily, writeLockColumn);
					Result r = table.get(g);
					otherTrxId = Bytes.toLong(r.getValue(dataFamily, writeLockColumn));
					sysout("Could not obtain lock for key: " + Bytes.toString(key.get()));
					sysout("OtherTrxId holding the lock is: " + otherTrxId);
				}
			}

			if (isLockPlaced)
				count++;
			else {
				tablePool.close();
				break;
			}
		}
		List<Long> returnVals = new ArrayList<Long>(2);
		returnVals.add(count);
		returnVals.add(otherTrxId);
		return returnVals;
	}

	@Override
	public Long commitRequestAcquireLocksFromWAL(Long transactionId,
			List<LogId> logs, List<List<ImmutableBytesWritable>> keys)
			throws IOException {
		// We traverse the list of logIds and the keys inside them,
		// which need to be locked, in the same order as was sent. This order
		// could be based on simple lexicographical ordering or contention based
		// ordering. We don't care how the client wants to order the locks. We just
		// execute its intentions.
		// Also, we decided that the waiting for locks should happen in this
		// function,
		// as opposed to locking at the client side. If that is the case, the return
		// values for this function would be different -- we can send back the
		// number of
		// unsuccessful attempts at acquiring locks.
		// TODO: The whole reason for grouping logs on a node is to acquire its
		// locks
		// from a single node without hitting network latencies. For that to work,
		// the locks should always be present in the
		// WAL snapshot. We can assure this by specifying that the WALs always
		// maintain
		// a consistent snapshot of locks. Since, writes to these locks anyway go
		// through
		// WALs, the snapshot gets updated first before reflecting on the store.
		long unsuccessfulAttempts = 0;
		// The logs list and the keys list should be of the same length.
		for (int i = 0; i < logs.size(); i++) {
			// Grab the snapshot from WAL.
			LogId logId = logs.get(i);
			sysout("Acquiring locks on logId: " + logId.toString());
			List<ImmutableBytesWritable> keyList = keys.get(i);
			// We would need a local transaction for every write lock, since locking
			// is a checkAndSet
			// operation which can be emulated by a local transaction.
			boolean waitMode = false;
			for (int index = 0; index < keyList.size(); index++) {
				ImmutableBytesWritable key = keyList.get(index);
				sysout("Acquiring lock on key: " + Bytes.toString(key.get()));
				if (waitMode) {
					// Do some amount of waiting before proceeding to acquire the lock.
					try {
						sysout("Sleeping....zzzzz....");
						Thread.sleep(100);
					} catch (InterruptedException e) {
						// Fall through
					}
				}
				boolean canPlaceLock = false;
				boolean foundInSnapshot = false;

				Snapshot snapshot = super.start(logId);
				sysout("Thread id: " + ManagementFactory.getRuntimeMXBean().getName()
						+ ", Fetched a snapshot with currentTs: " + snapshot.getTimestamp());
				Map<String, Write> snapshotWriteMap = snapshot.getWriteMap();
				// Final readSet to be "checked" and final writeSet to be committed.
				Map<byte[], Set<ImmutableBytesWritable>> readSets = new TreeMap<byte[], Set<ImmutableBytesWritable>>(
						Bytes.BYTES_COMPARATOR);
				List<Write> finalWrites = new LinkedList<Write>();

				// Read the write lock for the key from the data-store. If the snapshot
				// contains it, we don't need to go to the data-store.
				// Updating the read-set.
				byte[] lockName = Bytes.toBytes(Bytes
						.toString(WALTableProperties.dataTableName)
						+ Write.nameDelimiter
						+ Bytes.toString(WALTableProperties.dataFamily)
						+ Write.nameDelimiter
						+ Bytes.toString(WALTableProperties.writeLockColumn));
				Set<ImmutableBytesWritable> readSet = readSets.get(lockName);
				if (readSet == null) {
					readSet = new HashSet<ImmutableBytesWritable>();
					readSets.put(lockName, readSet);
				}
				readSet.add(key);

				// To update different columns, we have to create different Write
				// objects, since each write
				// only accepts a single itemName (tableName + familyName + columnName).
				Write lockWriteToKey = new Write();
				lockWriteToKey.setName(lockName);
				lockWriteToKey.setKey(key.get());
				lockWriteToKey.setValue(Bytes.toBytes(transactionId));

				if (!snapshot.isEmpty()) {
					// If present, read the item from the snapshot and update its
					// value.
					Write lockWriteFromSnapshot = null;
					if ((lockWriteFromSnapshot = snapshotWriteMap.get(Write
							.getNameAndKey(lockName, key.get()))) != null) {
						foundInSnapshot = true;
						long lockVal = Bytes.toLong(lockWriteFromSnapshot.getValue());
						sysout("LockVal from snapshot: " + lockVal);
						if (lockVal == WALTableProperties.zero) {
							canPlaceLock = true;
						}
					}
				}

				if (!foundInSnapshot) {
					sysout("Not found in snapshot, reading from table");
					// Read the item from the datastore and increment the stock count.
					HTable dataTable = new HTable(super.conf,
							WALTableProperties.dataTableName);
					Get g = new Get(key.get());
					g.addColumn(WALTableProperties.dataFamily,
							WALTableProperties.writeLockColumn);
					Result r = dataTable.get(g);
					if (Bytes.toLong(r.getValue(dataFamily, writeLockColumn)) == WALTableProperties.zero) {
						canPlaceLock = true;
					}
				}

				if (!canPlaceLock) {
					index--;
					waitMode = true;
					unsuccessfulAttempts++;
					continue;
				}

				// We can place the lock. Remove the waitMode and add the lock to
				// writes.
				waitMode = false;
				finalWrites.add(lockWriteToKey);
				// Prepare a check object and commit the write-set with optimistic
				// checks.
				Check check = new Check();
				check.setTimestamp(snapshot.getTimestamp());
				List<ReadSet> readSetsList = new LinkedList<ReadSet>();
				for (Map.Entry<byte[], Set<ImmutableBytesWritable>> readSetEntry : readSets
						.entrySet()) {
					ReadSet r = new ReadSet();
					r.setName(readSetEntry.getKey());
					r.setKeys(readSetEntry.getValue());
					readSetsList.add(r);
				}
				check.setReadSets(readSetsList);
				sysout("Thread id: " + ManagementFactory.getRuntimeMXBean().getName()
						+ ", Check object being sent: " + check.toString());

				// Commit the check object and the write-sets.
				boolean commitResponse = super.commit(logId, check, finalWrites, null,
						null);
				sysout("CommitResponse: " + commitResponse);
				if (commitResponse == false) {
					index--;
					waitMode = true;
					unsuccessfulAttempts++;
					continue;
				}
			}
		}
		return unsuccessfulAttempts;
	}

	@Override
	public List<ImmutableBytesWritable> commitRequestAcquireLocksViaIndirection(
			Long transactionId, List<LogId> logs, List<ImmutableBytesWritable> keys,
			List<Boolean> isKeyMigrated) throws IOException {
		long zero = WALTableProperties.zero;

		// Get the coprocessor environment
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HRegion region = env.getRegion();
		long count = 0;
		long otherTrxId = 0;
		byte[] destinationKey = null;
		long flag = 0;
		for (int i = 0; i < keys.size(); i++) {
			boolean isMigrated = isKeyMigrated.get(i);
			ImmutableBytesWritable origKey = keys.get(i);
			LogId logId = logs.get(i);
			byte[] indirectionKey = Bytes.toBytes(Bytes.toString(logId.getKey())
					+ WALTableProperties.logAndKeySeparator
					+ Bytes.toString(origKey.get()));

			// This rest of the code in this function should eventually be moved to
			// WALManagerEndpointForMyKVSpace as it is specific to WAL commits. This
			// functionality is
			// similar to checkAndMutate but for multiple objects (locks) in a single
			// WAL. At present,
			// the code for only reads and only writes is present in
			// WALManagerEndpointForMyKVSpace.
			// Adding this will give atomic read-modify-write operations on the WAL.

			// We first acquire the WAL lock, after that we can do whatever we want
			// with the lock objects.
			HashedBytes rowKey = new HashedBytes(logId.getKey());
			CountDownLatch rowLatch = new CountDownLatch(1);
			final int DEFAULT_ROWLOCK_WAIT_DURATION = 30000;

			// loop until we acquire the row lock (unless !waitForLock)
			boolean walLockAcquired = false;
			while (true) {
				CountDownLatch existingLatch = lockedRows.putIfAbsent(rowKey, rowLatch);
				if (existingLatch == null) {
					break;
				} else {
					// row already locked
					try {
						if (!existingLatch.await(DEFAULT_ROWLOCK_WAIT_DURATION,
								TimeUnit.MILLISECONDS)) {
							walLockAcquired = true;
							break;
						}
					} catch (InterruptedException ie) {
						// Empty
					}
				}
			}

			try {
				// A lock acquired above is similar to the row lock for the row
				// containing
				// all WAL related info. So, we can now fetch the CURRENT_TS, SYNC_TS,
				// and
				// OLDEST_TS from the row, without caring for someone else changing them
				// after
				// we've read them, as no one else can modify the row's content.
				boolean nextLockIsUnderTheSameWAL = false;
				WALEdit walEdit = new WALEdit();
				long now = EnvironmentEdgeManager.currentTimeMillis();
				HLog log = region.getLog();
				HTableDescriptor htd = new HTableDescriptor(
						WALTableProperties.dataTableName);
				boolean isLockPlaced = false;

				do {
					sysout("Acquiring lock for key: " + Bytes.toString(indirectionKey));
					isLockPlaced = false;
					Put toBePersistedPut = null;
					if (HRegion.rowIsInRange(region.getRegionInfo(), indirectionKey)) {
						Put p = new Put(indirectionKey);
						p.setWriteToWAL(false);
						p.add(WALTableProperties.WAL_FAMILY, writeLockColumn, appTimestamp,
								Bytes.toBytes(transactionId));
						p.add(WALTableProperties.WAL_FAMILY,
								WALTableProperties.isLockPlacedOrMigratedColumn, appTimestamp,
								Bytes.toBytes(WALTableProperties.one));
						toBePersistedPut = p;

						isLockPlaced = region.checkAndMutate(indirectionKey,
								WALTableProperties.WAL_FAMILY, writeLockColumn,
								CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes
										.toBytes(zero)), p, null, true);

						// A lock can't be placed due to either of three cases (The flag
						// indicates the case number):
						// 1. Someone else placed a lock at the source -- in which case, the
						// "transactionId"
						// would be found as value, and
						// "isLockMigrated" column would have a "0".
						// 2. Someone else migrated the lock -- in which case, the
						// "transactionId" would be found
						// as value, "isLockMigrated" column would have a "1" and
						// "destinationKey" column would
						// contain the destinationKey at which the lock would be found.
						// The return value would depend on the encountered case.
						// If we found a migration, inform the client about it by placing
						// the
						// "destinationKey"
						// as the 4rd value in the returned list.
						// 3. Someone deleted the lock -- implying that this was a detour
						// and
						// the
						// client has to go back to the original position to place a lock.
						if (!isLockPlaced) {
							sysout("Could not obtain lock for key: "
									+ Bytes.toString(indirectionKey));
							Get g = new Get(indirectionKey);
							g.addColumn(WALTableProperties.WAL_FAMILY, writeLockColumn);
							g.addColumn(WALTableProperties.WAL_FAMILY,
									WALTableProperties.isLockMigratedColumn);
							g.addColumn(WALTableProperties.WAL_FAMILY,
									WALTableProperties.destinationKeyColumn);
							Result r = region.get(g, null);
							if (!r.isEmpty()) {
								// first guess is lock has been placed by someone else.
								sysout("Result r is NOT empty");
								flag = 1;
								otherTrxId = Bytes.toLong(r.getValue(
										WALTableProperties.WAL_FAMILY, writeLockColumn));
								long isLockMigrated = Bytes.toLong(r.getValue(
										WALTableProperties.WAL_FAMILY,
										WALTableProperties.isLockMigratedColumn));
								// If the value of "isLockMigrated" is 2, that means the key you
								// are
								// locking is the
								// destination key. In that case, we don't send the
								// destinationKey
								// to the client.
								// The client has to retry on this key again to acquire the
								// lock.
								if (isLockMigrated == 1) {
									flag = 2;
									destinationKey = r.getValue(WALTableProperties.WAL_FAMILY,
											WALTableProperties.destinationKeyColumn);
								}
							} else {
								sysout("Checking for migration options. If its not migrated, we'll create the lock");
								// Here there are two options:
								// 1. We are at the lock's original place but couldn't find a
								// lock
								// entry
								// as no one used it yet. If so, we shall create and place the
								// lock.
								// This
								// option is valid if "isMigrated" is false.
								// 2. We are at the lock's destination place but couldn't find a
								// lock entry,
								// implying that the entry was deleted by the migrator. In this
								// case,
								// we just return with flag=3, to retry at the source.
								if (isMigrated) {
									// This shows that the detour lock was deleted.
									flag = 3;
								} else {
									sysout("Creating a new lock and pre-locking it.");
									Put createLock = new Put(indirectionKey);
									createLock.setWriteToWAL(false);
									createLock.add(WALTableProperties.WAL_FAMILY,
											writeLockColumn, appTimestamp, Bytes
													.toBytes(transactionId));
									createLock.add(WALTableProperties.WAL_FAMILY,
											WALTableProperties.isLockPlacedOrMigratedColumn,
											appTimestamp, Bytes.toBytes(WALTableProperties.one));
									createLock.add(WALTableProperties.WAL_FAMILY, versionColumn,
											appTimestamp, Bytes.toBytes(Long.toString(zero)));
									createLock.add(WALTableProperties.WAL_FAMILY,
											WALTableProperties.isLockMigratedColumn,
											WALTableProperties.appTimestamp, Bytes
													.toBytes(WALTableProperties.zero));
									toBePersistedPut = createLock;

									region.put(createLock);

									isLockPlaced = true;
								}
							}
							sysout("OtherTrxId holding the lock is: " + otherTrxId);
						}
					}
					/*
					 * else {sysout(
					 * "Not in region, hence sending a request to lock through HTable, for key: "
					 * + Bytes.toString(indirectionKey)); HTable table = (HTable)
					 * tablePool .getTable(WALTableProperties.walTableName); isLockPlaced
					 * = table.checkAndPut(indirectionKey, WALTableProperties.WAL_FAMILY,
					 * writeLockColumn, Bytes.toBytes(zero), new Put(indirectionKey).add(
					 * WALTableProperties.WAL_FAMILY, writeLockColumn, appTimestamp, Bytes
					 * .toBytes(transactionId)));
					 * 
					 * if (!isLockPlaced) { Get g = new Get(indirectionKey);
					 * g.addColumn(WALTableProperties.WAL_FAMILY, writeLockColumn); Result
					 * r = table.get(g); otherTrxId =
					 * Bytes.toLong(r.getValue(WALTableProperties.WAL_FAMILY,
					 * writeLockColumn)); sysout("Could not obtain lock for key: " +
					 * Bytes.toString(indirectionKey));
					 * sysout("OtherTrxId holding the lock is: " + otherTrxId); } }
					 */

					// Find out if the next lock is under the same WAL.
					// If so, change the value of "keyBeingOperatedOn" to the next WAL.
					// If not, then persist the KeyValue objects collected for all Lock
					// Writes in this iteration
					// and increment the outer loop variable.
					// BIGNOTE: In our implementation, unlocking is happening directly on
					// individual lock objects.
					// It is not going through the WAL lock.
					// In this case persistance information in region-WAL could become
					// out-of-order, and so in
					// case of region failure, recovery might be slightly messed-up (very
					// low chance but might happen).
					// Note that the in-memory structures will not get corrupted at all.
					// The checkAndMutate being
					// used for locks and simple Put operation for Unlock are both using
					// same row-locks for
					// synchronization.
					// CHANGES NEEDED: Somehow, we need to figure out why one coprocessor
					// is not able to call
					// a function on another coprocessor. Once we do that, unlock requests
					// will also go through
					// the WAL and hence will acquire the WAL lock before proceeding.
					if (isLockPlaced) {
						count++;

						// Add persistence info to WALEdit for the present lock.
						Map<byte[], List<KeyValue>> familyMap = toBePersistedPut
								.getFamilyMap();
						for (List<KeyValue> kvs : familyMap.values()) {
							for (KeyValue kv : kvs) {
								walEdit.add(kv);
							}
						}

						// Read the next lock and see if it belongs to the same WAL.
						LogId nextLogId = null;
						if (i + 1 < keys.size())
							nextLogId = logs.get(i + 1);

						if (nextLogId != null && nextLogId.equals(logId)) {
							nextLockIsUnderTheSameWAL = true;
							logId = nextLogId;

							isMigrated = isKeyMigrated.get(i + 1);
							origKey = keys.get(i + 1);
							sysout("Processed lock: " + Bytes.toString(indirectionKey));
							indirectionKey = Bytes.toBytes(Bytes.toString(logId.getKey())
									+ WALTableProperties.logAndKeySeparator
									+ Bytes.toString(origKey.get()));
							sysout("About to process lock: " + Bytes.toString(indirectionKey));

							// Increase the top-level loop index forever.
							i++;
						} else {
							nextLockIsUnderTheSameWAL = false;
							// Persist the WALEdit information.
							log.append(region.getRegionInfo(),
									WALTableProperties.dataTableName, walEdit, now, htd);
						}
					}
				} while (isLockPlaced && nextLockIsUnderTheSameWAL);
			} catch (Exception e) {
				System.err.println("Error in commit function: " + e.getMessage());
				e.printStackTrace();
			} finally {
				// Give up the lock on the wal.
				// Latches are one-time gates. So we have to open the gate to release
				// the
				// waiting
				// prisoners and then purge the gate completely.
				// New guy wanting the lock will install another gate.
				CountDownLatch internalRowLatch = lockedRows.remove(rowKey);
				internalRowLatch.countDown();
			}
		}

		List<ImmutableBytesWritable> returnVals = new ArrayList<ImmutableBytesWritable>();
		returnVals.add(new ImmutableBytesWritable(Bytes.toBytes(flag)));
		returnVals.add(new ImmutableBytesWritable(Bytes.toBytes(count)));
		// If not all locks were placed, send back the information of the otherTrxId
		// which placed
		// a lock or migrated the lock.
		if (count != keys.size() && flag != 3) {
			returnVals.add(new ImmutableBytesWritable(Bytes.toBytes(otherTrxId)));
			if (destinationKey != null)
				returnVals.add(new ImmutableBytesWritable(destinationKey));
		}
		return returnVals;
	}

	@Override
	public Boolean commitWritesPerEntityGroup(Long transactionId, LogId logId,
			List<ImmutableBytesWritable> keyList,
			List<ImmutableBytesWritable> shadowKeyList) throws IOException {
		// Execute the local transaction on the logId.
		// Grab the snapshot from WAL.
		Snapshot snapshot = super.start(logId);
		sysout("Inside Commit per entity group");
		sysout("Thread id: " + ManagementFactory.getRuntimeMXBean().getName()
				+ ", Fetched a snapshot with currentTs: " + snapshot.getTimestamp());
		Map<String, Write> snapshotWriteMap = snapshot.getWriteMap();
		// Final readSet to be "checked" and final writeSet to be committed.
		Map<byte[], Set<ImmutableBytesWritable>> readSets = new TreeMap<byte[], Set<ImmutableBytesWritable>>(
				Bytes.BYTES_COMPARATOR);
		List<Write> finalWrites = new LinkedList<Write>();

		// Read the shadow object from the data-store. If the snapshot
		// contains it, we don't need to go to the data-store.
		for (int index = 0; index < keyList.size(); index++) {
			ImmutableBytesWritable origKey = keyList.get(index);
			ImmutableBytesWritable shadowKey = shadowKeyList.get(index);
			// Updating the read-set.
			byte[] itemName = Bytes
					.toBytes(Bytes.toString(WALTableProperties.dataTableName)
							+ Write.nameDelimiter
							+ Bytes.toString(WALTableProperties.dataFamily)
							+ Write.nameDelimiter
							+ Bytes.toString(WALTableProperties.dataColumn));
			Set<ImmutableBytesWritable> readSet = readSets.get(itemName);
			if (readSet == null) {
				readSet = new HashSet<ImmutableBytesWritable>();
				readSets.put(itemName, readSet);
			}
			readSet.add(shadowKey);

			// To update different columns, we have to create different Write objects,
			// since each write
			// only accepts a single itemName (tableName + familyName + columnName).
			Write versionWriteToOrigKey = new Write();
			byte[] versionName = Bytes.toBytes(Bytes
					.toString(WALTableProperties.dataTableName)
					+ Write.nameDelimiter
					+ Bytes.toString(WALTableProperties.dataFamily)
					+ Write.nameDelimiter
					+ Bytes.toString(WALTableProperties.versionColumn));
			versionWriteToOrigKey.setName(versionName);
			versionWriteToOrigKey.setKey(origKey.get());
			versionWriteToOrigKey.setValue(Bytes.toBytes(transactionId));
			finalWrites.add(versionWriteToOrigKey);

			Write lockWriteToOrigKey = new Write();
			byte[] lockName = Bytes.toBytes(Bytes
					.toString(WALTableProperties.dataTableName)
					+ Write.nameDelimiter
					+ Bytes.toString(WALTableProperties.dataFamily)
					+ Write.nameDelimiter
					+ Bytes.toString(WALTableProperties.writeLockColumn));
			lockWriteToOrigKey.setName(lockName);
			lockWriteToOrigKey.setKey(origKey.get());
			lockWriteToOrigKey.setValue(Bytes.toBytes(WALTableProperties.zero));
			sysout("Commit per entity group: Write for unlocking the key: "
					+ lockWriteToOrigKey.toString());
			finalWrites.add(lockWriteToOrigKey);

			Write valueWriteToOrigKey = new Write();
			valueWriteToOrigKey.setName(itemName);
			valueWriteToOrigKey.setKey(origKey.get());

			if (!snapshot.isEmpty()) {
				// If present, read the item from the snapshot and update its
				// value.
				Write shadowWriteFromSnapshot = null;
				if ((shadowWriteFromSnapshot = snapshotWriteMap.get(Write
						.getNameAndKey(itemName, shadowKey.get()))) != null) {
					// Change the key of the write and make it the write for the origKey.
					valueWriteToOrigKey.setValue(shadowWriteFromSnapshot.getValue());
					finalWrites.add(valueWriteToOrigKey);
					sysout("Final to-be-committed Write using data from " + "snapshot: "
							+ valueWriteToOrigKey.toString());
					continue;
				}
			}

			// Read the item from the datastore
			HTable dataTable = new HTable(super.conf,
					WALTableProperties.dataTableName);
			Get g = new Get(shadowKey.get());
			g.addColumn(WALTableProperties.dataFamily, WALTableProperties.dataColumn);
			g.setTimeStamp(appTimestamp);
			Result r = dataTable.get(g);
			sysout("Reading shadow objects.");
			for (KeyValue kv : r.raw()) {
				sysout("KV key: " + kv.toString());
				sysout("KV val: " + Bytes.toString(kv.getValue()));
			}
			valueWriteToOrigKey.setValue(r.getValue(WALTableProperties.dataFamily,
					WALTableProperties.dataColumn));
			finalWrites.add(valueWriteToOrigKey);
			sysout("Final to-be-committed Write using data from " + "store: "
					+ valueWriteToOrigKey.toString());
		}

		// Prepare a check object and commit the write-set with optimistic
		// checks.
		Check check = new Check();
		check.setTimestamp(snapshot.getTimestamp());
		List<ReadSet> readSetsList = new LinkedList<ReadSet>();
		for (Map.Entry<byte[], Set<ImmutableBytesWritable>> readSetEntry : readSets
				.entrySet()) {
			ReadSet r = new ReadSet();
			r.setName(readSetEntry.getKey());
			r.setKeys(readSetEntry.getValue());
			readSetsList.add(r);
		}
		check.setReadSets(readSetsList);
		sysout("Thread id: " + ManagementFactory.getRuntimeMXBean().getName()
				+ ", Check object being sent: " + check.toString());

		// Commit the check object and the write-sets.
		boolean commitResponse = super
				.commit(logId, check, finalWrites, null, null);
		return commitResponse;
	}

	@Override
	public Long abortRequestReleaseLocksFromWAL(Long transactionId,
			List<LogId> logs, List<List<ImmutableBytesWritable>> keys)
			throws IOException {
		long unsuccessfulAttempts = 0;
		// The logs list and the keys list should be of the same length.
		for (int logIndex = 0; logIndex < logs.size(); logIndex++) {
			// Grab the snapshot from WAL.
			LogId logId = logs.get(logIndex);
			sysout("Releasing locks on logId: " + logId.toString());
			List<ImmutableBytesWritable> keyList = keys.get(logIndex);
			// We would need a local transaction for every write lock.
			// Unlocking however does not need any reads. Its just all writes which
			// need to happen in isolation.
			boolean waitMode = false;
			Snapshot snapshot = super.start(logId);
			sysout("Thread id: " + ManagementFactory.getRuntimeMXBean().getName()
					+ ", Fetched a snapshot with currentTs: " + snapshot.getTimestamp());
			Map<String, Write> snapshotWriteMap = snapshot.getWriteMap();
			List<Write> finalWrites = new LinkedList<Write>();

			for (int keyIndex = 0; keyIndex < keyList.size(); keyIndex++) {
				ImmutableBytesWritable key = keyList.get(keyIndex);
				sysout("Releasing lock on key: " + Bytes.toString(key.get()));
				if (waitMode) {
					// Do some amount of waiting before proceeding to acquire the lock.
					try {
						sysout("Sleeping....zzzzz....");
						Thread.sleep(100);
					} catch (InterruptedException e) {
						// Fall through
					}
				}

				// To update different columns, we have to create different Write
				// objects, since each write
				// only accepts a single itemName (tableName + familyName + columnName).
				Write lockWriteToKey = new Write();
				byte[] lockName = Bytes.toBytes(Bytes
						.toString(WALTableProperties.dataTableName)
						+ Write.nameDelimiter
						+ Bytes.toString(WALTableProperties.dataFamily)
						+ Write.nameDelimiter
						+ Bytes.toString(WALTableProperties.writeLockColumn));
				lockWriteToKey.setName(lockName);
				lockWriteToKey.setKey(key.get());
				lockWriteToKey.setValue(Bytes.toBytes(WALTableProperties.zero));
				finalWrites.add(lockWriteToKey);
			}
			// Prepare a check object and commit the write-set with optimistic
			// checks.
			Check check = new Check();
			check.setTimestamp(snapshot.getTimestamp());

			// Commit the check object and the write-sets.
			boolean commitResponse = super.commit(logId, check, finalWrites, null,
					null);
			sysout("CommitResponse: " + commitResponse);
			if (commitResponse == false) {
				logIndex--;
				waitMode = true;
				unsuccessfulAttempts++;
				continue;
			}
		}
		return unsuccessfulAttempts;
	}

	@Override
	public List<Snapshot> getSnapshotsForLogIds(List<LogId> logIdList)
			throws IOException {
		List<Snapshot> snapshots = new ArrayList<Snapshot>(logIdList.size());
		for (int i = 0; i < logIdList.size(); i++)
			snapshots.add(i, null);
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HRegion region = env.getRegion();

		List<Future<Snapshot>> futures = new ArrayList<Future<Snapshot>>(logIdList
				.size());
		for (int i = 0; i < logIdList.size(); i++)
			futures.add(i, null);
		for (int logIdIndex = 0; logIdIndex < logIdList.size(); logIdIndex++) {
			final LogId logId = logIdList.get(logIdIndex);
			if (HRegion.rowIsInRange(region.getRegionInfo(), logId.getKey())) {
				sysout("LogId in region: " + logId.toString());
				final WALManagerDistTxnEndpoint walManagerDistTxnEndpoint = this;
				// Snapshot snapshot = super.start(logId);
				// snapshots.add(logIdIndex, snapshot);
				Future<Snapshot> future = pool.submit(new Callable<Snapshot>() {
					public Snapshot call() throws Exception {
						return walManagerDistTxnEndpoint.start(logId);
					}
				});
				futures.set(logIdIndex, future);
				// System.out.println("Adding snapshot : " + snapshot.toString());
			} else {
				// Fill the snapshots list with negatively timed snapshot, because the
				// client
				// checks the non exitence
				// of a logId on a region by the presence of a negative timestamp at
				// that position.
				// System.out.println("LogId not in region: " + logId.toString());
				Snapshot snapshot = new Snapshot();
				snapshot.setTimestamp(-1);
				Map<String, Write> writeMap = new HashMap<String, Write>();
				snapshot.setWriteMap(writeMap);
				snapshots.set(logIdIndex, snapshot);
			}
		}

		// Go through the futures and grab the snapshots.
		for (int i = 0; i < futures.size(); i++) {
			Future<Snapshot> future = futures.get(i);
			if (future == null)
				continue;
			try {
				Snapshot snapshot = future.get();
				snapshots.set(i, snapshot);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return snapshots;
	}

	// I'm assuming the destination keys are the same as the original keys, only
	// the logId will
	// change.
	public List<List<ImmutableBytesWritable>> migrateLocks(
			final Long transactionId, final List<LogId> logs,
			final List<List<ImmutableBytesWritable>> keys, final LogId destLogId)
			throws IOException {
		List<List<ImmutableBytesWritable>> returnValList = new ArrayList<List<ImmutableBytesWritable>>();
		for (int i = 0; i < logs.size(); i++) {
			returnValList.add(null);
		}

		// Get the coprocessor environment
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HRegion region = env.getRegion();
		// Among the logIds sent to us, not all them will be processed by us. This
		// list
		// will contain those logs which we will process, and the map will contain
		// the indirection
		// from "our id" to the id present in the sent list.
		List<LogId> ourLogs = new LinkedList<LogId>();
		List<List<ImmutableBytesWritable>> ourKeys = new LinkedList<List<ImmutableBytesWritable>>();
		int ourKeyId = 0;
		Map<Integer, String> localKeyIdToGlobalKeyIdMap = new HashMap<Integer, String>();

		List<Future<ImmutableBytesWritable>> futures = new ArrayList<Future<ImmutableBytesWritable>>();
		final WALManagerDistTxnEndpoint walManagerDistTxnEndpoint = this;
		for (int logIndex = 0; logIndex < logs.size(); logIndex++) {
			final LogId logId = logs.get(logIndex);
			// If the logId does not belong to this region, we just skip.
			if (!HRegion.rowIsInRange(region.getRegionInfo(), logId.getKey())) {
				sysout("ROW NOT IN REGION: logId: " + Bytes.toString(logId.getKey())
						+ ", region info: " + region.getRegionInfo().toString());
				returnValList.set(logIndex, new LinkedList<ImmutableBytesWritable>());
				continue;
			}

			ourLogs.add(logId);
			List<ImmutableBytesWritable> correspKeys = keys.get(logIndex);
			for (int keyIndex = 0; keyIndex < correspKeys.size(); keyIndex++) {
				localKeyIdToGlobalKeyIdMap.put(ourKeyId, Integer.toString(logIndex)
						+ "=" + Integer.toString(keyIndex));
				final ImmutableBytesWritable key = correspKeys.get(keyIndex);
				Future<ImmutableBytesWritable> future = pool.submit(new Callable<ImmutableBytesWritable>() {
					public ImmutableBytesWritable call() throws Exception {
						// Note that the destination key is th same as the original key,
						// only the
						// LogId changes.
						return walManagerDistTxnEndpoint.migrateLock(transactionId, logId,
								key, destLogId, key);
					}
				});
				// The index at which this future is being added will be the same as
				// ourKeyId at this point.
				futures.add(future);
				ourKeyId++;
			}
		}

		// Go through the futures and grab the results.
		for (int i = 0; i < futures.size(); i++) {
			// All futures will have a value -- none will have a null.
			Future<ImmutableBytesWritable> future = futures.get(i);
			String globalKeyId = localKeyIdToGlobalKeyIdMap.get(i);
			String[] tokens = globalKeyId.split("=");
			int logIndex = Integer.parseInt(tokens[0]);
			int keyIndex = Integer.parseInt(tokens[1]);
			List<ImmutableBytesWritable> boolList = returnValList.get(logIndex);
			if (boolList == null) {
				boolList = new ArrayList<ImmutableBytesWritable>();
				returnValList.set(logIndex, boolList);
			}
			// KeyIndex will usually be the size of the boolList at any time because
			// the
			// keys were inserted sequentially into the futures list.
			assert (keyIndex == boolList.size());
			try {
				ImmutableBytesWritable result = future.get();
				boolList.add(result);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		for (int k = 0; k < logs.size(); k++) {
			LogId log = logs.get(k);
			List<ImmutableBytesWritable> boolList = returnValList.get(k);
			sysout("Verification: for log: " + log.toString() + ", boolList size: "
					+ boolList.size());
		}
		return returnValList;
	}

	@Override
	public Boolean commitWritesPerEntityGroupWithoutShadows(Long transactionId,
			List<LogId> logIdList, List<List<Put>> allUpdates,
			List<List<LogId>> toBeUnlockedDestLogIds) throws IOException {
		// Get the coprocessor environment
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HRegion region = env.getRegion();
		// Among the logIds sent to us, not all them will be processed by us. This
		// list
		// will contain those logs which we will process, and the map will contain
		// the indirection
		// from "our id" to the id present in the sent list.
		List<LogId> ourLogIds = new LinkedList<LogId>();
		int ourIdCount = 0;
		Map<Integer, Integer> localIdToGlobalIdMap = new HashMap<Integer, Integer>();

		boolean allCommitsSucceeded = true;
		for (int logIndex = 0; logIndex < logIdList.size(); logIndex++) {
			LogId logId = logIdList.get(logIndex);
			// If the logId does not belong to this region, we just skip.
			if (!HRegion.rowIsInRange(region.getRegionInfo(), logId.getKey()))
				continue;

			ourLogIds.add(logId);
			localIdToGlobalIdMap.put(ourIdCount, logIndex);
			ourIdCount++;
		}

		List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
		List<Snapshot> snapshots = getSnapshotsForLogIds(ourLogIds);
		for (int localId = 0; localId < snapshots.size(); localId++) {
			// Execute the local transaction on the logId using the fetched snapshot.
			Snapshot snapshot = snapshots.get(localId);
			int globalId = localIdToGlobalIdMap.get(localId);
			final LogId logId = logIdList.get(globalId);
			sysout("Inside Commit per entity group, processing logId: "
					+ logId.toString());
			sysout("Thread id: " + ManagementFactory.getRuntimeMXBean().getName()
					+ ", Fetched a snapshot with currentTs: " + snapshot.getTimestamp());
			Map<String, Write> snapshotWriteMap = snapshot.getWriteMap();
			// Final readSet to be "checked" and final writeSet to be committed.
			Map<byte[], Set<ImmutableBytesWritable>> readSets = new TreeMap<byte[], Set<ImmutableBytesWritable>>(
					Bytes.BYTES_COMPARATOR);
			final List<Write> finalWrites = new LinkedList<Write>();

			// In this function, since we do not use shadow objects, there will be not
			// be any
			// reads. All the given updates (Puts) are converted to writes and
			// committed to the WAL.
			List<Put> updates = allUpdates.get(globalId);
			List<LogId> destLogIds = toBeUnlockedDestLogIds.get(globalId);
			final List<ImmutableBytesWritable> toBeUnlockedKeys = new LinkedList<ImmutableBytesWritable>();
			final List<Integer> commitTypeInfo = new LinkedList<Integer>();
			for (int updateIndex = 0; updateIndex < updates.size(); updateIndex++) {
				Put put = updates.get(updateIndex);
				// Add locks to be released first at destination. The destination could
				// be our-migrated-position, someone-else's-migrated-position or the
				// base
				// position. Only in the first case, since we migrated, we will also
				// unlock the source and reset
				// the migration information.
				LogId destLogId = destLogIds.get(updateIndex);
				byte[] toBeUnlockedKeyAtDest = Bytes.toBytes(Bytes.toString(destLogIds
						.get(updateIndex).getKey())
						+ WALTableProperties.logAndKeySeparator
						+ Bytes.toString(put.getRow()));
				toBeUnlockedKeys.add(new ImmutableBytesWritable(toBeUnlockedKeyAtDest));
				commitTypeInfo.add(destLogId.getCommitType());

				// Add the base logId to be unlocked and reset if we migrated this lock.
				if (destLogId.getCommitType() == LogId.ONLY_DELETE) {
					byte[] toBeUnlockedKeyAtSource = Bytes.toBytes(Bytes.toString(logId
							.getKey())
							+ WALTableProperties.logAndKeySeparator
							+ Bytes.toString(put.getRow()));

					toBeUnlockedKeys.add(new ImmutableBytesWritable(
							toBeUnlockedKeyAtSource));
					commitTypeInfo.add(LogId.UNLOCK_AND_RESET_MIGRATION);
				}
				// To update different columns, we have to create different Write
				// objects,
				// since each write
				// only accepts a single itemName (tableName + familyName + columnName).
				/*
				 * Write versionWriteToOrigKey = new Write(); byte[] versionName =
				 * Bytes.toBytes(Bytes .toString(WALTableProperties.dataTableName) +
				 * Write.nameDelimiter + Bytes.toString(WALTableProperties.dataFamily) +
				 * Write.nameDelimiter +
				 * Bytes.toString(WALTableProperties.versionColumn));
				 * versionWriteToOrigKey.setName(versionName);
				 * versionWriteToOrigKey.setKey(put.getRow());
				 * versionWriteToOrigKey.setValue(Bytes.toBytes("" + transactionId));
				 * finalWrites.add(versionWriteToOrigKey);
				 */

				Write lockWriteToOrigKey = new Write();
				byte[] lockName = Bytes.toBytes(Bytes
						.toString(WALTableProperties.dataTableName)
						+ Write.nameDelimiter
						+ Bytes.toString(WALTableProperties.dataFamily)
						+ Write.nameDelimiter
						+ Bytes.toString(WALTableProperties.writeLockColumn));
				lockWriteToOrigKey.setName(lockName);
				lockWriteToOrigKey.setKey(put.getRow());
				lockWriteToOrigKey.setValue(Bytes.toBytes(WALTableProperties.zero));
				sysout("Commit per entity group: Write for unlocking the key: "
						+ lockWriteToOrigKey.toString());
				finalWrites.add(lockWriteToOrigKey);

				// For every KeyValue, we create a write and add it to finalWrites.
				// Typically, we expect only one KeyValue with the value information.
				// TODO: This can be used to have writes to multiple columns; in which
				// case,
				// put.getFamilyMap() would be the starting point for iteration.
				for (KeyValue kv : put.getFamilyMap().get(dataFamily)) {
					byte[] itemName = Bytes.toBytes(Bytes
							.toString(WALTableProperties.dataTableName)
							+ Write.nameDelimiter
							+ Bytes.toString(WALTableProperties.dataFamily)
							+ Write.nameDelimiter + Bytes.toString(kv.getQualifier()));

					Write valueWriteToOrigKey = new Write();
					valueWriteToOrigKey.setName(itemName);
					valueWriteToOrigKey.setKey(put.getRow());
					valueWriteToOrigKey.setValue(kv.getValue());
					finalWrites.add(valueWriteToOrigKey);
					sysout("Final to-be-committed Write using data from " + "store: "
							+ valueWriteToOrigKey.toString());
				}
			}

			// Prepare a check object and commit the write-set with optimistic
			// checks. In this case, we didn't have any reads, so the check object
			// will be empty.
			final Check check = new Check();
			check.setTimestamp(snapshot.getTimestamp());

			// Commit the check object and the write-sets.
			// boolean commitResponse = super.commit(logId, check, finalWrites, null);
			final WALManagerDistTxnEndpoint walManagerDistTxnEndpoint = this;
			Future<Boolean> future = pool.submit(new Callable<Boolean>() {
				public Boolean call() throws Exception {
					return walManagerDistTxnEndpoint.commit(logId, check, finalWrites,
							toBeUnlockedKeys, commitTypeInfo);
				}
			});
			futures.add(future);
		}

		for (Future<Boolean> future : futures) {
			boolean commitResponse = true;
			try {
				commitResponse = future.get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			allCommitsSucceeded = allCommitsSucceeded && commitResponse;
		}

		return allCommitsSucceeded;
	}

	@Override
	public Boolean commitWritesPerEntityGroupWithShadows(Long transactionId,
			List<LogId> logIdList, List<List<Put>> allUpdates,
			List<List<LogId>> toBeUnlockedDestLogIds) throws IOException {
		// Get the coprocessor environment
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HRegion region = env.getRegion();
		// Among the logIds sent to us, not all them will be processed by us. This
		// list
		// will contain those logs which we will process, and the map will contain
		// the indirection
		// from "our id" to the id present in the sent list.
		List<LogId> ourLogIds = new LinkedList<LogId>();
		int ourIdCount = 0;
		Map<Integer, Integer> localIdToGlobalIdMap = new HashMap<Integer, Integer>();

		boolean allCommitsSucceeded = true;
		for (int logIndex = 0; logIndex < logIdList.size(); logIndex++) {
			LogId logId = logIdList.get(logIndex);
			// If the logId does not belong to this region, we just skip.
			if (!HRegion.rowIsInRange(region.getRegionInfo(), logId.getKey()))
				continue;

			ourLogIds.add(logId);
			localIdToGlobalIdMap.put(ourIdCount, logIndex);
			ourIdCount++;
		}

		// For all "ourLogIds", accumulate all the Puts; form their
		// corresponding shadowPuts and fetch them from store in a single batched
		// request. While preparing the final Put, the code below will combine
		// the values from snapshot and the list acquired by this batched request.
		List<Get> toBeFetchedFromDataStore = new ArrayList<Get>();
		for (int localId = 0; localId < ourLogIds.size(); localId++) {
			int globalId = localIdToGlobalIdMap.get(localId);
			final LogId logId = logIdList.get(globalId);
			List<Put> updates = allUpdates.get(globalId);

			for (Put p : updates) {
				if (!p.isEmpty()) {
					byte[] shadowKey = Bytes.toBytes(Bytes.toString(p.getRow())
							+ WALTableProperties.shadowKeySeparator
							+ Long.toString(transactionId));

					Get g = new Get(shadowKey);
					g.addFamily(dataFamily);
					toBeFetchedFromDataStore.add(g);
				}
			}
		}
		HTable dataTable = new HTable(super.conf, WALTableProperties.dataTableName);
		/*
		 * try { if (!toBeFetchedFromDataStore.isEmpty()) { //Object[] results =
		 * dataTable.batch(toBeFetchedFromDataStore); } } catch
		 * (InterruptedException e1) { // fall through. }
		 */
		// TODO: From a map of the above results. For now, we don't really
		// use this data as this function is taking in the actual updates (Puts).
		// If another transaction is doing a roll-forward, we won't have those Puts.

		List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
		List<Snapshot> snapshots = getSnapshotsForLogIds(ourLogIds);
		for (int localId = 0; localId < snapshots.size(); localId++) {
			// Execute the local transaction on the logId using the fetched snapshot.
			Snapshot snapshot = snapshots.get(localId);
			int globalId = localIdToGlobalIdMap.get(localId);
			final LogId logId = logIdList.get(globalId);
			sysout("Inside Commit per entity group, processing logId: "
					+ logId.toString());
			sysout("Thread id: " + ManagementFactory.getRuntimeMXBean().getName()
					+ ", Fetched a snapshot with currentTs: " + snapshot.getTimestamp());
			Map<String, Write> snapshotWriteMap = snapshot.getWriteMap();
			// Final readSet to be "checked" and final writeSet to be committed.
			Map<byte[], Set<ImmutableBytesWritable>> readSets = new TreeMap<byte[], Set<ImmutableBytesWritable>>(
					Bytes.BYTES_COMPARATOR);
			final List<Write> finalWrites = new LinkedList<Write>();

			// In this function, since we do not use shadow objects, there will be not
			// be any
			// reads. All the given updates (Puts) are converted to writes and
			// committed to the WAL.
			List<Put> updates = allUpdates.get(globalId);
			List<LogId> destLogIds = toBeUnlockedDestLogIds.get(globalId);
			final List<ImmutableBytesWritable> toBeUnlockedKeys = new LinkedList<ImmutableBytesWritable>();
			final List<Integer> commitTypeInfo = new LinkedList<Integer>();

			byte[] versionColName = Bytes.toBytes(Bytes
					.toString(WALTableProperties.dataTableName)
					+ Write.nameDelimiter
					+ Bytes.toString(WALTableProperties.dataFamily)
					+ Write.nameDelimiter
					+ Bytes.toString(WALTableProperties.versionColumn));

			for (int updateIndex = 0; updateIndex < updates.size(); updateIndex++) {
				Put put = updates.get(updateIndex);
				// Form the key for this put's shadow. Read that from snapshot; if not
				// available,
				// read it from datastore.
				// TODO: Ideally, the contents of the ShadowPut should be read and
				// copied into the
				// original Put object. However, since this function takes in the update
				// list as
				// entire Put object, we just serialize the sent Put object and store it
				// in WAL.
				// When we remove the sending of Put objects in this function
				// (decreasing serialization
				// overhead), then we'll have to resort to this copying. For now, we
				// escape that.
				// Also, the check object sent to the commit function will not have any
				// readSets in it,
				// as we know that there won't be any concurrent transaction writing to
				// the same shadow
				// key -- they have transactionId appended to them.
				byte[] shadowKey = Bytes.toBytes(Bytes.toString(put.getRow())
						+ WALTableProperties.shadowKeySeparator
						+ Long.toString(transactionId));
				boolean foundInSnapshot = false;
				if (!snapshot.isEmpty()) {
					// If present, read the shadow from the snapshot.
					// TODO: For now we are just reading the "versionColName" from the
					// snapshot.
					// Ideally, the Write objects should have more structure, not just
					// "name" and "key" (which is
					// treated as key,family, and column, for us). Either that, or the
					// writeMap of Snapshot
					// object should parse all the writes and form nested hash tables, so
					// that we can search
					// all Write entries for a particular rowKey -- better than mentioning
					// all its columns too,
					// which we may not know all the time.

					Write shadowWriteFromSnapshot = null;
					if ((shadowWriteFromSnapshot = snapshotWriteMap.get(Write
							.getNameAndKey(versionColName, shadowKey))) != null) {
						// Change the key of the write and make it the write for the
						// origKey.
						// TODO: Look at the above TODO.
						// valueWriteToOrigKey.setValue(shadowWriteFromSnapshot.getValue());
						// finalWrites.add(valueWriteToOrigKey);
						foundInSnapshot = true;
					}
				}

				if (foundInSnapshot == false) {
					// Read the item from the datastore
					// Since we already fetched all the needed shadow objects in a batched
					// request above, we search and pick from among them.
					// TODO: we should form a map of the fetched results and pick from it
					// using the shadowKey.
				}

				// TODO: Look at the TODO above this loop.
				// valueWriteToOrigKey.setValue(r.getValue(WALTableProperties.dataFamily,
				// WALTableProperties.dataColumn));
				// finalWrites.add(valueWriteToOrigKey);

				// Add locks to be released first at destination. The destination could
				// be our-migrated-position, someone-else's-migrated-position or the
				// base
				// position. Only in the first case, since we migrated, we will also
				// unlock the source and reset
				// the migration information.
				LogId destLogId = destLogIds.get(updateIndex);
				byte[] toBeUnlockedKeyAtDest = Bytes.toBytes(Bytes.toString(destLogIds
						.get(updateIndex).getKey())
						+ WALTableProperties.logAndKeySeparator
						+ Bytes.toString(put.getRow()));
				toBeUnlockedKeys.add(new ImmutableBytesWritable(toBeUnlockedKeyAtDest));
				commitTypeInfo.add(destLogId.getCommitType());

				// Add the base logId to be unlocked and reset if we migrated this lock.
				if (destLogId.getCommitType() == LogId.ONLY_DELETE) {
					byte[] toBeUnlockedKeyAtSource = Bytes.toBytes(Bytes.toString(logId
							.getKey())
							+ WALTableProperties.logAndKeySeparator
							+ Bytes.toString(put.getRow()));

					toBeUnlockedKeys.add(new ImmutableBytesWritable(
							toBeUnlockedKeyAtSource));
					commitTypeInfo.add(LogId.UNLOCK_AND_RESET_MIGRATION);
				}

				// To update different columns, we have to create different Write
				// objects,
				// since each write
				// only accepts a single itemName (tableName + familyName + columnName).
				/*
				 * Write versionWriteToOrigKey = new Write(); byte[] versionName =
				 * Bytes.toBytes(Bytes .toString(WALTableProperties.dataTableName) +
				 * Write.nameDelimiter + Bytes.toString(WALTableProperties.dataFamily) +
				 * Write.nameDelimiter +
				 * Bytes.toString(WALTableProperties.versionColumn));
				 * versionWriteToOrigKey.setName(versionName);
				 * versionWriteToOrigKey.setKey(put.getRow());
				 * versionWriteToOrigKey.setValue(Bytes.toBytes("" + transactionId));
				 * finalWrites.add(versionWriteToOrigKey);
				 */

				// Since locking is being taken care of separately on a different
				// columnFamily (WAL_FAMILY),
				// we don't need any writes specific to locks on data.
				/*
				 * Write lockWriteToOrigKey = new Write(); byte[] lockName =
				 * Bytes.toBytes(Bytes .toString(WALTableProperties.dataTableName) +
				 * Write.nameDelimiter + Bytes.toString(WALTableProperties.dataFamily) +
				 * Write.nameDelimiter +
				 * Bytes.toString(WALTableProperties.writeLockColumn));
				 * lockWriteToOrigKey.setName(lockName);
				 * lockWriteToOrigKey.setKey(put.getRow());
				 * lockWriteToOrigKey.setValue(Bytes.toBytes(WALTableProperties.zero));
				 * sysout("Commit per entity group: Write for unlocking the key: " +
				 * lockWriteToOrigKey.toString()); finalWrites.add(lockWriteToOrigKey);
				 */

				// For every KeyValue, we create a write and add it to finalWrites.
				// Typically, we expect only one KeyValue with the value information.
				// TODO: This can be used to have writes to multiple columns; in which
				// case,
				// put.getFamilyMap() would be the starting point for iteration.
				if (!put.isEmpty()) {
					for (KeyValue kv : put.getFamilyMap().get(dataFamily)) {
						byte[] itemName = Bytes.toBytes(Bytes
								.toString(WALTableProperties.dataTableName)
								+ Write.nameDelimiter
								+ Bytes.toString(WALTableProperties.dataFamily)
								+ Write.nameDelimiter + Bytes.toString(kv.getQualifier()));

						Write valueWriteToOrigKey = new Write();
						valueWriteToOrigKey.setName(itemName);
						valueWriteToOrigKey.setKey(put.getRow());
						valueWriteToOrigKey.setValue(kv.getValue());
						finalWrites.add(valueWriteToOrigKey);
						sysout("Final to-be-committed Write using data from " + "store: "
								+ valueWriteToOrigKey.toString());
					}
				}
			}

			// Prepare a check object and commit the write-set with optimistic
			// checks. In this case, we didn't have any reads, so the check object
			// will be empty.
			final Check check = new Check();
			check.setTimestamp(snapshot.getTimestamp());

			// Commit the check object and the write-sets.
			// boolean commitResponse = super.commit(logId, check, finalWrites, null);
			final WALManagerDistTxnEndpoint walManagerDistTxnEndpoint = this;
			Future<Boolean> future = pool.submit(new Callable<Boolean>() {
				public Boolean call() throws Exception {
					return walManagerDistTxnEndpoint.commit(logId, check, finalWrites,
							toBeUnlockedKeys, commitTypeInfo);
				}
			});
			futures.add(future);
		}

		for (Future<Boolean> future : futures) {
			boolean commitResponse = true;
			try {
				commitResponse = future.get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			allCommitsSucceeded = allCommitsSucceeded && commitResponse;
		}

		return allCommitsSucceeded;
	}

	@Override
	public Boolean abortWithoutShadows(Long transactionId, List<LogId> logIdList,
			List<List<ImmutableBytesWritable>> allKeysToBeUnlocked,
			List<List<LogId>> toBeUnlockedDestLogIds) throws IOException {
		// Get the coprocessor environment
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HRegion region = env.getRegion();
		// Among the logIds sent to us, not all them will be processed by us. This
		// list
		// will contain those logs which we will process, and the map will contain
		// the indirection
		// from "our id" to the id present in the sent list.
		List<LogId> ourLogIds = new LinkedList<LogId>();
		int ourIdCount = 0;
		Map<Integer, Integer> localIdToGlobalIdMap = new HashMap<Integer, Integer>();

		boolean allAbortsSucceeded = true;
		for (int logIndex = 0; logIndex < logIdList.size(); logIndex++) {
			LogId logId = logIdList.get(logIndex);
			// If the logId does not belong to this region, we just skip.
			if (!HRegion.rowIsInRange(region.getRegionInfo(), logId.getKey()))
				continue;

			ourLogIds.add(logId);
			localIdToGlobalIdMap.put(ourIdCount, logIndex);
			ourIdCount++;
		}

		List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
		List<Snapshot> snapshots = getSnapshotsForLogIds(ourLogIds);
		for (int localId = 0; localId < ourLogIds.size(); localId++) {
			// Execute the local transaction on the logId on the fetched snapshot.
			Snapshot snapshot = snapshots.get(localId);
			int globalId = localIdToGlobalIdMap.get(localId);
			final LogId logId = logIdList.get(globalId);
			sysout("Inside Abort per entity group, processing logId: "
					+ logId.toString());
			// Final readSet to be "checked" and final writeSet to be committed.
			Map<byte[], Set<ImmutableBytesWritable>> readSets = new TreeMap<byte[], Set<ImmutableBytesWritable>>(
					Bytes.BYTES_COMPARATOR);
			final List<Write> finalWrites = new LinkedList<Write>();

			// In this function, since we do not use shadow objects, there will be not
			// be any
			// reads or writes. All the given updates (Puts) are converted to writes
			// and
			// committed to the WAL.
			List<ImmutableBytesWritable> toBeAbortedKeys = allKeysToBeUnlocked
					.get(globalId);
			List<LogId> destLogIds = toBeUnlockedDestLogIds.get(globalId);
			// This list will contain the key names of the cached locks. The argument
			// to this function
			// does not know of the cached locks and thus sends only the original
			// key-names.
			final List<ImmutableBytesWritable> toBeUnlockedKeys = new LinkedList<ImmutableBytesWritable>();
			final List<Integer> commitTypeInfo = new LinkedList<Integer>();
			for (int keyIndex = 0; keyIndex < toBeAbortedKeys.size(); keyIndex++) {
				ImmutableBytesWritable key = toBeAbortedKeys.get(keyIndex);
				// Add locks to be released first at destination. The destination could
				// be our-migrated-position, someone-else's-migrated-position or the
				// base
				// position. Only in the first case, since we migrated, we will also
				// unlock the source and reset
				// the migration information.
				LogId destLogId = destLogIds.get(keyIndex);
				byte[] toBeUnlockedKeyAtDest = Bytes
						.toBytes(Bytes.toString(destLogIds.get(keyIndex).getKey())
								+ WALTableProperties.logAndKeySeparator
								+ Bytes.toString(key.get()));
				toBeUnlockedKeys.add(new ImmutableBytesWritable(toBeUnlockedKeyAtDest));
				commitTypeInfo.add(destLogId.getCommitType());

				// Add the base logId to be unlocked and reset if we migrated this lock.
				if (destLogId.getCommitType() == LogId.ONLY_DELETE) {
					byte[] toBeUnlockedKeyAtSource = Bytes.toBytes(Bytes.toString(logId
							.getKey())
							+ WALTableProperties.logAndKeySeparator
							+ Bytes.toString(key.get()));

					toBeUnlockedKeys.add(new ImmutableBytesWritable(
							toBeUnlockedKeyAtSource));
					commitTypeInfo.add(LogId.UNLOCK_AND_RESET_MIGRATION);
				}
				// To update different columns, we have to create different Write
				// objects,
				// since each write
				// only accepts a single itemName (tableName + familyName + columnName).
				/*
				 * Write versionWriteToOrigKey = new Write(); byte[] versionName =
				 * Bytes.toBytes(Bytes .toString(WALTableProperties.dataTableName) +
				 * Write.nameDelimiter + Bytes.toString(WALTableProperties.dataFamily) +
				 * Write.nameDelimiter +
				 * Bytes.toString(WALTableProperties.versionColumn));
				 * versionWriteToOrigKey.setName(versionName);
				 * versionWriteToOrigKey.setKey(key.get());
				 * versionWriteToOrigKey.setValue(Bytes.toBytes("" + transactionId));
				 * finalWrites.add(versionWriteToOrigKey);
				 */

				Write lockWriteToOrigKey = new Write();
				byte[] lockName = Bytes.toBytes(Bytes
						.toString(WALTableProperties.dataTableName)
						+ Write.nameDelimiter
						+ Bytes.toString(WALTableProperties.dataFamily)
						+ Write.nameDelimiter
						+ Bytes.toString(WALTableProperties.writeLockColumn));
				lockWriteToOrigKey.setName(lockName);
				lockWriteToOrigKey.setKey(key.get());
				lockWriteToOrigKey.setValue(Bytes.toBytes(WALTableProperties.zero));
				sysout("Abort per entity group: Write for unlocking the key: "
						+ lockWriteToOrigKey.toString());
				finalWrites.add(lockWriteToOrigKey);
			}

			// Prepare a check object and commit the write-set with optimistic
			// checks. In this case, we didn't have any reads, so the check object
			// will be empty.
			final Check check = new Check();
			check.setTimestamp(snapshot.getTimestamp());

			// Commit the check object and the write-sets.
			// boolean commitResponse = super.commit(logId, check, finalWrites, null);
			final WALManagerDistTxnEndpoint walManagerDistTxnEndpoint = this;
			Future<Boolean> future = pool.submit(new Callable<Boolean>() {
				public Boolean call() throws Exception {
					return walManagerDistTxnEndpoint.commit(logId, check, finalWrites,
							toBeUnlockedKeys, commitTypeInfo);
				}
			});
			futures.add(future);
		}

		for (Future<Boolean> future : futures) {
			boolean commitResponse = true;
			try {
				commitResponse = future.get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			allAbortsSucceeded = allAbortsSucceeded && commitResponse;
		}

		return allAbortsSucceeded;
	}

	@Override
	public List<List<Result>> getAfterServerSideMerge(List<LogId> logs,
			List<List<Get>> gets) throws IOException {
		// Get the coprocessor environment
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HRegion region = env.getRegion();

		List<List<Result>> results = new LinkedList<List<Result>>();
		for (int j = 0; j < logs.size(); j++) {
			results.add(new LinkedList<Result>());
		}

		List<Snapshot> snapshots = getSnapshotsForLogIds(logs);
		for (int i = 0; i < snapshots.size(); i++) {
			Snapshot snapshot = snapshots.get(i);
			if (snapshot != null && snapshot.getTimestamp() < 0) {
				// This log isn't in our region.
				continue;
			}
			Map<String, Write> snapshotWriteMap = snapshot.getWriteMap();
			List<List<KeyValue>> resultKvs = new ArrayList<List<KeyValue>>();

			// Grab the Get from the table in this same region
			List<Get> getsForThisLog = gets.get(i);
			List<Result> resultsForThisLog = results.get(i);
			for (Get g : getsForThisLog) {
				sysout("Seeking Get: " + g.toString() + ", for the log: "
						+ logs.get(i).toString());
				if (!HRegion.rowIsInRange(region.getRegionInfo(), g.getRow())) {
					sysout("Get not in this region: " + Bytes.toString(g.getRow()));
					continue;
				}

				Result resultFromRegion = region.get(g, null);

				List<KeyValue> kvs = new ArrayList<KeyValue>();
				for (byte[] column : g.getFamilyMap().get(dataFamily)) {
					byte[] dataName = Bytes.toBytes(Bytes
							.toString(WALTableProperties.dataTableName)
							+ Write.nameDelimiter
							+ Bytes.toString(WALTableProperties.dataFamily)
							+ Write.nameDelimiter + Bytes.toString(column));
					Write dataWriteFromSnapshot = snapshotWriteMap.get(Write
							.getNameAndKey(dataName, g.getRow()));
					if (dataWriteFromSnapshot != null) {
						KeyValue kv = new KeyValue(g.getRow(), dataFamily, column,
								dataWriteFromSnapshot.getValue());
						kvs.add(kv);
					} else {
						// Since its not found in the snapshot, we take it from store's
						// result.
						kvs.addAll(resultFromRegion.getColumn(dataFamily, column));
					}
				}
				// Push the kvs for this Get into the resultKvs. This Get request
				// might not
				// have been satisfied through the snapshot. Thus, we wait until all
				// the kvs have
				// been fetched and then create the Result object eventually.
				resultKvs.add(kvs);
			}

			// Now that resultKvs has been filled up, create individual Result objects
			// with
			// its corresponding kv list.
			for (int j = 0; j < resultKvs.size(); j++) {
				List<KeyValue> kvs = resultKvs.get(j);
				if (kvs == null || kvs.isEmpty())
					sysout("Empty kvs for row: " + getsForThisLog.get(j).toString());
				Result finalResult = new Result(kvs);
				sysout("Adding result for this log: " + finalResult.toString());
				resultsForThisLog.add(finalResult);
			}
		}
		return results;
	}
}
