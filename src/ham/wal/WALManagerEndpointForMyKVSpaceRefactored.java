package ham.wal;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HashedBytes;

public class WALManagerEndpointForMyKVSpaceRefactored extends
		WALManagerEndpointForMyKVSpace {
	private long currentTs = 0;
	private LinkedList<Row> putsToPlaceLocksAtMWAL = new LinkedList<Row>();
	HTablePool tablePool = null;

	private boolean debug = false;
	public static void sysout(String otp) {
		 //System.out.println(otp);
	}
	
	@Override
	public void start(org.apache.hadoop.hbase.CoprocessorEnvironment env) {
		super.start(env);
		this.tablePool = new HTablePool(env.getConfiguration(), 30,new HTableFactory() {
			public HTableInterface createHTableInterface(Configuration config,
					byte[] tableName) {
				try {
					HTable table = new HTable(config, tableName);
					table.setAutoFlush(false);
					table.setWriteBufferSize(100*1000);
					return table;
				} catch (IOException ioe) {
					throw new RuntimeException(ioe);
				}
			}
		});
	}

	public boolean commitToMemory(LogId id, Check check, List<Write> writes,
			List<ImmutableBytesWritable> toBeUnlockedKeys,
			List<Integer> commitTypeInfo, List<Row> firstSetOfCausalLockReleases,
			List<Row> secondSetOfCausalLockReleases, List<WALEdit> walEdits)
			throws IOException {
		try {
			sysout("Committing at logId: " + id.toString());
			// get the coprocessor environment
			RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
			HRegion region = env.getRegion();

			// If all checks passed, commit the LogEntry, and also the update the
			// other
			// timestamp fields.
			// Increase the currentTs for every commit.
			currentTs++;
			LogEntry newLogEntry = new LogEntry();
			newLogEntry.setTimestamp(currentTs);
			
			if (debug) {
				sysout("Adding the following writes to the log entry at timestamp: "
					+ currentTs);
				for (Write w : writes)
					sysout(w.toString());
			}

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

			// Returning the WALEdit, it will be combined and flushed by the caller.
			walEdits.add(walEdit);
			// log.append(env.getRegion().getRegionInfo(),
			// WALTableProperties.dataTableName, walEdit, now, htd);

			// Flush these writes onto the local region, since we assume that data
			// items under a write-ahead-log
			// are placed in the same region hosting the WAL.
			WALEdit logEntryWalEdit = flushLogEntryToLocalRegionInMemory(region,
					newLogEntry);
			walEdits.add(logEntryWalEdit);

			// All unlocking and reseting goes into the first set.
			// All deletes for remote, migrated locks go into the second set.
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
					// Since the unlocking and reseting at H-WAL and M-WAL are ideally
					// done by an asynchronous
					// thread, we setWriteToWAL as false. This will partly remove the
					// asynchronous thread latency
					// from the commit operation.
					delDest.setWriteToWAL(false);
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
					// Since the unlocking and reseting at H-WAL and M-WAL are ideally
					// done by an asynchronous
					// thread, we setWriteToWAL as false. This will partly remove the
					// asynchronous thread latency
					// from the commit operation. Furthermore, in case of H-WAL, all
					// walEdits must be combined
					// and logged as one. We need to merge the unlocking WALEdits with
					// those of data-updates.
					p.setWriteToWAL(false);
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
					// TODO: The choice of putting this operation in first or second
					// causal lock releases
					// depends on whether unlocking is happening at H-WAL or M-WAL --
					// first set if H-WAL, second-set
					// if M-WAL. For now, we are just plainly putting this in secondSet,
					// because firstSet prematurely
					// assumes that all keys in that set are in the present (same) region.
					secondSetOfCausalLockReleases.add(p);
				} else if (commitTypeInfo.get(index) == LogId.UNLOCK_AND_RESET_MIGRATION) {
					sysout("UNLOCKING AND RESETING KEY: "
							+ Bytes.toString(toBeUnlockedDestKey.get()));
					Put pSrc = new Put(toBeUnlockedDestKey.get());
					// Since the unlocking and reseting at H-WAL and M-WAL are ideally
					// done by an asynchronous
					// thread, we setWriteToWAL as false. This will partly remove the
					// asynchronous thread latency
					// from the commit operation. Furthermore, in case of H-WAL, all
					// walEdits must be combined
					// and logged as one. We need to merge the unlocking WALEdits with
					// those of data-updates.
					pSrc.setWriteToWAL(false);
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
					firstSetOfCausalLockReleases.add(pSrc);
				}
			}
		} catch (Exception e) {
			System.err.println("Error in commit function: " + e.getMessage());
			e.printStackTrace();
		}
		return true;
	}

	// This function should be split into 3 functions. First function simply
	// checks for the possibility of
	// migration and returns the appropriate value. Second takes in all the formed
	// Puts for the M-WALs and
	// sends them through using a single HTable.batch call. Third function,
	// depending on the result of the
	// second, either places a migration information at H-WAL or just leaves it
	// alone.
	public boolean canMigrateLock(final Long transactionId, final LogId logId,
			final ImmutableBytesWritable key, final LogId destLogId,
			final ImmutableBytesWritable destKey) throws IOException {
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HRegion region = env.getRegion();

		byte[] tableCachedLock = Bytes.toBytes(Bytes.toString(logId.getKey())
				+ WALTableProperties.logAndKeySeparator + Bytes.toString(key.get()));

		String localKey = Bytes.toString(logId.getKey())
				+ WALTableProperties.logAndKeySeparator + Bytes.toString(key.get())
				+ Bytes.toString(WALTableProperties.WAL_FAMILY)
				+ Bytes.toString(WALTableProperties.writeLockColumn);
		HashedBytes rowKey = new HashedBytes(Bytes.toBytes(localKey));

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
		g.addColumn(WALTableProperties.WAL_FAMILY,
				TPCCTableProperties.runningAvgForDTProportionColumn);
		g.setTimeStamp(WALTableProperties.appTimestamp);
		Result r = null;
		r = region.get(g, null);
		if (!r.isEmpty()) {
			long isLockPlacedOrMigratedColumnInfo = Bytes.toLong(r.getValue(
					WALTableProperties.WAL_FAMILY,
					WALTableProperties.isLockPlacedOrMigratedColumn));

			if (isLockPlacedOrMigratedColumnInfo == WALTableProperties.one) {
				// Some other transaction placed a migration or locked in-place.
				canAttemptMigration = false;
			}

			/*
			// Do a check for the running average of DT proportion to ensure that
			// the lock is eligible for migration.
			if (canAttemptMigration) {
				double runningAvgDtProportion = Bytes.toDouble(r.getValue(
						WALTableProperties.WAL_FAMILY,
						TPCCTableProperties.runningAvgForDTProportionColumn));
				sysout("Current runningAvgDtProportion is: " + runningAvgDtProportion);
				if (runningAvgDtProportion < TPCCTableProperties.dtProportionUpperThreshold)
					canAttemptMigration = false;
			}
			*/
		}
		// Note that canAttemptMigration will return its default value of true if the Result r was empty.
		// This is the case where no lock was present at H-WAL.
		return canAttemptMigration;
	}

	public void createLockToPlaceAtMWAL(final Long transactionId,
			final LogId logId, final ImmutableBytesWritable key,
			final LogId destLogId, final ImmutableBytesWritable destKey)
			throws IOException {
		String destination = Bytes.toString(destLogId.getKey())
				+ WALTableProperties.logAndKeySeparator + Bytes.toString(destKey.get());

		byte[] selfPlacedDestinationKey = Bytes.toBytes(destination);

		Put p = new Put(selfPlacedDestinationKey);
		p
				.add(WALTableProperties.WAL_FAMILY, WALTableProperties.writeLockColumn,
						WALTableProperties.appTimestamp, Bytes
								.toBytes(WALTableProperties.zero));
		p.add(WALTableProperties.WAL_FAMILY,
				WALTableProperties.isLockMigratedColumn,
				WALTableProperties.appTimestamp, Bytes.toBytes(WALTableProperties.two));
		p
				.add(WALTableProperties.WAL_FAMILY,
						WALTableProperties.isLockPlacedOrMigratedColumn,
						WALTableProperties.appTimestamp, Bytes
								.toBytes(WALTableProperties.zero));
		p.add(WALTableProperties.WAL_FAMILY,
				WALTableProperties.regionObserverMarkerColumn,
				WALTableProperties.appTimestamp, WALTableProperties.randomValue);
		this.putsToPlaceLocksAtMWAL.add(p);
	}

	public void placeLocksAtMWAL() throws IOException {
		HTableInterface walTable = tablePool
				.getTable(WALTableProperties.walTableName);

		for (Row p: putsToPlaceLocksAtMWAL) {
			if (debug)
				sysout("Sending a remote put to place migrated lock for row: " + Bytes.toString(p.getRow())); 
			walTable.put((Put)p);
		}
			
		walTable.flushCommits();
		// TODO: The above batch operation should always work if tried sufficient
		// times.
		// We can extend the above functionality to retry only a few times, and if
		// remote locks
		// aren't successfully placed in the mean time, then just abort migration.
		// If we add
		// this stuff, the return value for this function should be a list of
		// booleans corresponding
		// to individual entries.

		// If all locks got placed at MWAL, then remove all entries from
		// putsToPlaceLocksAtMWAL.
		putsToPlaceLocksAtMWAL.clear();
		walTable.close();
	}

	public ImmutableBytesWritable addMigrationInfoOnLocalHWALAndReturnFinalLockPosition(
			final Long transactionId, final LogId logId,
			final ImmutableBytesWritable key, final LogId destLogId,
			final ImmutableBytesWritable destKey) throws IOException {
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HRegion region = env.getRegion();

		byte[] tableCachedLock = Bytes.toBytes(Bytes.toString(logId.getKey())
				+ WALTableProperties.logAndKeySeparator + Bytes.toString(key.get()));

		String localKey = Bytes.toString(logId.getKey())
				+ WALTableProperties.logAndKeySeparator + Bytes.toString(key.get())
				+ Bytes.toString(WALTableProperties.WAL_FAMILY)
				+ Bytes.toString(WALTableProperties.writeLockColumn);

		String destination = Bytes.toString(destLogId.getKey())
				+ WALTableProperties.logAndKeySeparator + Bytes.toString(destKey.get());

		byte[] selfPlacedDestinationKey = Bytes.toBytes(destination);

		boolean migrationResult = false;
		Put p = new Put(tableCachedLock);
		// TODO: For now, we are disabling the writeToWAL for updating
		// tableCachedLock with
		// indirection information (see that we've put a false in checkAndMutate()
		// function.
		// Once this function is refactored, we'll have to accumulate walEdits for
		// all
		// local lock modifications and log them as one.
		p.setWriteToWAL(false);
		p.add(WALTableProperties.WAL_FAMILY, WALTableProperties.writeLockColumn,
				WALTableProperties.appTimestamp, Bytes.toBytes(transactionId));
		p.add(WALTableProperties.WAL_FAMILY,
				WALTableProperties.isLockMigratedColumn,
				WALTableProperties.appTimestamp, Bytes.toBytes(WALTableProperties.one));
		p.add(WALTableProperties.WAL_FAMILY,
				WALTableProperties.isLockPlacedOrMigratedColumn,
				WALTableProperties.appTimestamp, Bytes.toBytes(WALTableProperties.one));
		p.add(WALTableProperties.WAL_FAMILY,
				WALTableProperties.destinationKeyColumn,
				WALTableProperties.appTimestamp, selfPlacedDestinationKey);
		p.add(WALTableProperties.WAL_FAMILY,
				WALTableProperties.regionObserverMarkerColumn,
				WALTableProperties.randomValue);
		migrationResult = region.checkAndMutate(tableCachedLock,
				WALTableProperties.WAL_FAMILY,
				WALTableProperties.isLockPlacedOrMigratedColumn,
				CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes
						.toBytes(WALTableProperties.zero)), p, null, false);
	
		if (debug)
			sysout("For tableCachedLock: " + Bytes.toString(tableCachedLock)
				+ ", CheckAndMutate succeeded and so we placed the migration info ");

		ImmutableBytesWritable finalLockPositionToReturn = null;
		if (migrationResult = true) {
			if (debug)
				sysout("PLACED A DETOUR AT CACHED LOCK: "
					+ Bytes.toString(tableCachedLock));
			finalLockPositionToReturn = new ImmutableBytesWritable(
					selfPlacedDestinationKey);
		} else {
			// If the function reached this line, then we're either not allowed to
			// migrate
			// or that migration failed. In either case, read the present migration
			// information
			// on the lock and return to client with the key to which this lock was
			// migrated by some other transaction.
			// If this lock is acquired by some other trx, then
			// finalLockPositionToReturn
			// will be this same tableCachedLock.
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

			Result r = region.get(g, null);
			// If r is empty, that means there was no lock created in the first
			// place.
			// Thus create it and mark it as migrated.
			if (r.isEmpty()) {
				sysout("NO TABLE_CACHED_LOCK found; we are inserting it ourself!");
				region.put(p);
				migrationResult = true;
				finalLockPositionToReturn = new ImmutableBytesWritable(
						selfPlacedDestinationKey);
			} else {
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

				if (isLockMigratedColumnInfo == WALTableProperties.one) {
					// Some other transaction placed a migration.
					byte[] existingDestinationKey = r.getValue(
							WALTableProperties.WAL_FAMILY,
							WALTableProperties.destinationKeyColumn);
					finalLockPositionToReturn = new ImmutableBytesWritable(
							existingDestinationKey);
				} else {
					// Come back to this same lock.
					finalLockPositionToReturn = new ImmutableBytesWritable(
							tableCachedLock);
				}
			}
		}
		return finalLockPositionToReturn;
	}

	public ImmutableBytesWritable readAndReturnFinalLockPosition(
			final Long transactionId, final LogId logId,
			final ImmutableBytesWritable key, final LogId destLogId,
			final ImmutableBytesWritable destKey) throws IOException {
		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		HRegion region = env.getRegion();

		byte[] tableCachedLock = Bytes.toBytes(Bytes.toString(logId.getKey())
				+ WALTableProperties.logAndKeySeparator + Bytes.toString(key.get()));

		// Read the present migration
		// information
		// on the lock and return to client with the key to which this lock was
		// migrated by some other transaction.
		// If this lock is acquired by some other trx, then
		// finalLockPositionToReturn
		// will be this same tableCachedLock.
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

		Result r = region.get(g, null);
		// The result will not be empty, as no transaction will ever delete that
		// row.
		// If the row was empty to begin with, then "canMigrateLock" should not have
		// returned false,
		// and so this function will never have been called.
		// Check for migration info and write lock. If some one else
		// migrated, note
		// that destinationKey and send it back to client. Otherwise, if no
		// one
		// migrated and someone has placed a lock, then come back to this
		// same
		// lock for acquisition.

		ImmutableBytesWritable finalLockPositionToReturn = null;
		long isLockMigratedColumnInfo = Bytes
				.toLong(r.getValue(WALTableProperties.WAL_FAMILY,
						WALTableProperties.isLockMigratedColumn));

		if (isLockMigratedColumnInfo == WALTableProperties.one) {
			// Some other transaction placed a migration.
			byte[] existingDestinationKey = r.getValue(WALTableProperties.WAL_FAMILY,
					WALTableProperties.destinationKeyColumn);
			finalLockPositionToReturn = new ImmutableBytesWritable(
					existingDestinationKey);
		} else {
			// Come back to this same lock.
			finalLockPositionToReturn = new ImmutableBytesWritable(tableCachedLock);
		}
		return finalLockPositionToReturn;
	}

	// The localKey (logId along with key) is locked when indirection is being
	// placed
	// or it is being locked. Thus indirection is attempted
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
		g.addColumn(WALTableProperties.WAL_FAMILY,
				TPCCTableProperties.runningAvgForDTProportionColumn);
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

				// Do a check for the running average of DT proportion to ensure that
				// the lock is eligible for migration.
				if (canAttemptMigration) {
					double runningAvgDtProportion = Bytes.toDouble(r.getValue(
							WALTableProperties.WAL_FAMILY,
							TPCCTableProperties.runningAvgForDTProportionColumn));
					if (runningAvgDtProportion < TPCCTableProperties.dtProportionUpperThreshold)
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
					WALTableProperties.regionObserverMarkerColumn,
					WALTableProperties.appTimestamp, WALTableProperties.randomValue);
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
			// TODO: For now, we are disabling the writeToWAL for updating
			// tableCachedLock with
			// indirection information (see that we've put a false in checkAndMutate()
			// function.
			// Once this function is refactored, we'll have to accumulate walEdits for
			// all
			// local lock modifications and log them as one.
			p.setWriteToWAL(false);
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
					WALTableProperties.regionObserverMarkerColumn,
					WALTableProperties.randomValue);
			if (HRegion.rowIsInRange(region.getRegionInfo(), tableCachedLock)) {
				migrationResult = region.checkAndMutate(tableCachedLock,
						WALTableProperties.WAL_FAMILY,
						WALTableProperties.isLockPlacedOrMigratedColumn,
						CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes
								.toBytes(WALTableProperties.zero)), p, null, false);
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
