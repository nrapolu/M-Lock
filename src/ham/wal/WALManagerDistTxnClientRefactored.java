package ham.wal;

import ham.wal.scheduler.RequestPriority;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class WALManagerDistTxnClientRefactored extends WALManagerDistTxnClient {

	public WALManagerDistTxnClientRefactored() throws IOException {
		super();
		// TODO Auto-generated constructor stub
	}

	private boolean debug = false;
	public static void sysout(long trxId, String otp) {
		//System.out.println(otp);
	}
	
	public List<Result> get(final HTableInterface logTable,
			final HTableInterface dataTable, final DistTxnState transactionState,
			final List<Get> gets, int requestPriorityTag) throws Throwable {
		return getViaHTableBatchCall(logTable, dataTable, transactionState, gets);
	}
	
	private List<Result> getViaHTableBatchCall(final HTableInterface logTable,
			final HTableInterface dataTable, final DistTxnState transactionState,
			final List<Get> gets) throws Throwable {
		long trxId = transactionState.getTransactionId();
		
		Object[] resultsAsObjects = logTable.batch(gets);
		List<Result> finalResults = new ArrayList<Result>(resultsAsObjects.length);
		// For all results, store the version number of the read result in
		// DistTxnMetadata's
		// getWithVersionList
		for (int i = 0; i < resultsAsObjects.length; i++) {
			Result r = (Result)resultsAsObjects[i];
			finalResults.add(r);
			// sysout(transactionState.getTransactionId(), "Examining result : "
			// + r.toString());
			long version = 0;
			try {
				version = WALTableProperties.getVersion(r);
			} catch (Exception e) {
				sysout(trxId, "Exception for row: " + Bytes.toString(r.getRow()));
				e.printStackTrace();
			}
			ImmutableBytesWritable key = new ImmutableBytesWritable(r.getRow());
			transactionState.addToReadCache(key, r);
			transactionState.addReadInfoToDistTxnMetadata(key, version);
		}
		return finalResults;
	}
	
	public boolean commitRequestCheckVersions(final HTable logTable,
			final HTable dataTable, final DistTxnState transactionState)
			throws Throwable {
		return checkVersionsViaHTableBatchCall(logTable, dataTable, transactionState);
	}
	
	private boolean checkVersionsViaHTableBatchCall(final HTable logTable,
			final HTable dataTable, final DistTxnState transactionState)
			throws Throwable {
		final long trxId = transactionState.getTransactionId();
		List<Pair<ImmutableBytesWritable, Long>> readVersionList = transactionState
				.getReadVersionList();
		List<Get> actions = new LinkedList<Get>();
		for (Pair<ImmutableBytesWritable, Long> readWithVersion : readVersionList) {
			ImmutableBytesWritable key = readWithVersion.getFirst();
			// TODO: Should also fetch write-lock information for this object. Should
			// abort if
			// we found a lock. Specifically, in our case, we need to fetch
			// isLockPlacedOrMigrated
			// column and also isLockMigrated column. If the former is set to 1 and
			// the latter is set
			// to zero, then the lock is placed.
			Get g = new Get(key.get());
			g.addColumn(dataFamily, versionColumn);
			g.addColumn(WALTableProperties.WAL_FAMILY,
					WALTableProperties.regionObserverMarkerColumn);
			actions.add(g);
		}

		// We use the List<Get> gets function in this client. It first checks in the
		// snapshot
		// and if doesn't find it, fetches it from the table.
		Object[] resultsAsObjs = logTable.batch(actions);
		List<Result> results = new ArrayList<Result>(resultsAsObjs.length);
		for (Object o: resultsAsObjs) {
			results.add((Result)o);
		}
		
		boolean isFresh = true;
		for (int i = 0; i < results.size(); i++) {
			Result r = results.get(i);
			long presentVersion = Long.parseLong(Bytes.toString(r.getValue(
					dataFamily, versionColumn)));
			long cacheVersion = readVersionList.get(i).getSecond();
			
			if (debug) {
				sysout(trxId, "Present version for key: " + Bytes.toString(r.getRow())
					+ ", is: " + presentVersion);
				sysout(trxId, "Cache version is: " + cacheVersion);
			}
			
			if (cacheVersion != presentVersion) {
				isFresh = isFresh && false;
			}
		}
		return isFresh;
	}
	
	@Override
	public boolean commitRequestAcquireLocksViaIndirection(final HTableInterface logTable,
			final DistTxnState transactionState, boolean stopForLockMigration)
			throws Throwable {
		final long trxId = transactionState.getTransactionId();
		if (stopForLockMigration) {
			long timeForMigration = stopForLockMigration();
			transactionState.setLockMigrationTime(timeForMigration);
			// System.out.println("Time for lock migration: " + timeForMigration);
		}
		sysout(trxId, "In acquire locks via indirection.");
		// Grab the entire writeBuffer and iterate on the keys in the order of
		// logIds.
		TreeMap<LogId, TreeSet<Action>> writeBuffer = transactionState
				.getWriteBufferWithLogIds();
		boolean allLocked = false;
		long nbDetoursEncountered = 0;
		long nbNetworkHopsInTotal = 0;

		final List<LogId> logs = new LinkedList<LogId>();
		final List<ImmutableBytesWritable> keys = new LinkedList<ImmutableBytesWritable>();
		final List<Boolean> isLockAtMigratedLocation = new LinkedList<Boolean>();
		TreeMap<LogId, List<Pair<ImmutableBytesWritable, Boolean>>> mapOfMigratedLogIdsToKeys= 
			new TreeMap<LogId, List<Pair<ImmutableBytesWritable, Boolean>>>(new DistTxnState.LogIdComparator());
		
		Iterator<LogId> logIdItr = writeBuffer.navigableKeySet().iterator();
		while (logIdItr.hasNext()) {
			LogId logId = logIdItr.next();
			TreeSet<Action> actions = writeBuffer.get(logId);

			for (Action action : actions) {
				// Synchronized pair of key and its logId.
				ImmutableBytesWritable key = new ImmutableBytesWritable(action
						.getAction().getRow());

				
				LogId correspLogId = LockMigrator.afterMigrationKeyMap.get(key);
				// If correspLogId returned null, that means there was no migration info. Could be because
				// migration was turned off completely. In that case correspLogId becomes our own LogId.
				if (correspLogId == null)
					correspLogId = logId;
				
				if (debug)
					sysout(trxId, "For key: " + Bytes.toString(key.get())
						+ ", destination logId is: " + correspLogId.toString());
				
				List<Pair<ImmutableBytesWritable, Boolean>> correspKeysList = 
					mapOfMigratedLogIdsToKeys.get(correspLogId);
				if (correspKeysList == null) {
					correspKeysList = new LinkedList<Pair<ImmutableBytesWritable, Boolean>>();
					// CorrespLogId should never be null;
					mapOfMigratedLogIdsToKeys.put(correspLogId, correspKeysList);
				}
					
				if (correspLogId != null) {
					boolean isLocalLockAtMigratedLocation = false;
					if (!correspLogId.equals(logId)) {
						isLocalLockAtMigratedLocation = true;
					}
					
					Pair<ImmutableBytesWritable, Boolean> pair = new Pair<ImmutableBytesWritable, Boolean>(
							key, isLocalLockAtMigratedLocation);
					correspKeysList.add(pair);
				}
			}
		}
		
		// Traverse the map in the order of migratedLogIds, and fill logs, keys, isLockAtMigratedLocation lists.
		sysout(trxId, "Traversing locks in this order: ");
		for (LogId finalLogId: mapOfMigratedLogIdsToKeys.keySet()) {
			List<Pair<ImmutableBytesWritable, Boolean>> perLogIdKeysList = 
				mapOfMigratedLogIdsToKeys.get(finalLogId);
			for (Pair<ImmutableBytesWritable, Boolean> p: perLogIdKeysList) {
				keys.add(p.getFirst());
				logs.add(finalLogId);
				isLockAtMigratedLocation.add(p.getSecond());
				
				if (debug)
					sysout(trxId, "Key: " + Bytes.toString(p.getFirst().get()) + " , " +
						"Log: " + finalLogId.toString() + " , " + "isLockAtMigratedLocation: " + p.getSecond());
			}
		}
		
		// All the keys (along with their logIds) are sent to one coprocessor.
		// If all the keys are not on the same region, the coprocessor uses
		// HTable instances to throw the keys in.
		class AcquireLockCallBack implements
				Batch.Callback<List<ImmutableBytesWritable>> {
			private long nbLocksAcquired;
			private long otherTrxIdWhichHasTheLock;
			private byte[] migratedToKey = null;
			private long flag;

			Long getNbLocksAcquired() {
				return nbLocksAcquired;
			}

			Long getOtherTrxIdWhichHasTheLock() {
				return otherTrxIdWhichHasTheLock;
			}

			byte[] getMigratedToKey() {
				return migratedToKey;
			}

			long getFlag() {
				return this.flag;
			}

			// 1. If the result list contains only two values, then all locks that
			// were sent are locked and the value
			// refers to the number of locks acquired. The first value, which is a
			// flag, will have the value
			// as 0.
			// 2. If the result list contains three values, then the third value would
			// be the transactionId that
			// acquired the lock. The client needs to retry at this same key site to
			// acquire the lock.
			// The flag value (first value in the list) will be of value 1.
			// 3. If the result list contains four values, then the fourth value would
			// be the destination key
			// at which this lock can be found. The client needs to directly contact
			// that key to acquire the
			// lock. The flag value (first value in the list) will be of value 2.
			// 4. If the result contains only two values and the flag (first value) is
			// 3, then the detour
			// was deleted and so we need to lookup in our dynamic map to find the
			// original LogId mapping
			// for this key.
			@Override
			public synchronized void update(byte[] region, byte[] row,
					List<ImmutableBytesWritable> result) {
				// Since this coprocessorExec call will only go to one region hosting
				// the keys present in that list, there will be only one Call
				this.flag = Bytes.toLong(result.get(0).get());
				this.nbLocksAcquired = Bytes.toLong(result.get(1).get());
				if (result.size() > 2)
					this.otherTrxIdWhichHasTheLock = Bytes.toLong(result.get(2).get());
				if (result.size() > 3)
					this.migratedToKey = result.get(3).get();

				if (debug) {
					sysout(trxId, "In AcquireLockCallBack for trx: "
						+ transactionState.getTransactionId() + ", for row: "
						+ Bytes.toString(row) + ", flag is :" + flag
						+ ", nbLocksAcquired is: " + nbLocksAcquired
						+ ", and otherTrxIdWhichHasTheLock is: "
						+ otherTrxIdWhichHasTheLock);
					if (this.migratedToKey != null)
						sysout(trxId, "MigratedToKey is: "
							+ Bytes.toString(this.migratedToKey));
				}
			}
		}

		long totalNbLocksToBeAcquired = keys.size();
		long nbLocksAcquiredInThisRound = 0;
		long nbLocksAcquiredSoFar = 0;
		long flag;
		
		while (true) {
			for (int i = 0; i < nbLocksAcquiredInThisRound; i++) {
				logs.remove(0);
				keys.remove(0);
				isLockAtMigratedLocation.remove(0);
			}
			// RequestPriority changes with the number of locks that were acquired so far.
			// As the trx reaches completion, its priority is increased so as to give it
			// exclusive priority to go through uninterrupted.
			RequestPriority requestPriority = new RequestPriority(
					RequestPriority.OCC_ACQUIRE_LOCKS, trxId, 
					(nbLocksAcquiredSoFar / totalNbLocksToBeAcquired));
			requestPriority.setExecRequestPriorityTag();
			
			AcquireLockCallBack aLockCallBack = new AcquireLockCallBack();
			logTable
					.coprocessorExec(
							WALManagerDistTxnProtocol.class,
							logs.get(0).getKey(),
							logs.get(0).getKey(),
							new Batch.Call<WALManagerDistTxnProtocol, List<ImmutableBytesWritable>>() {
								@Override
								public List<ImmutableBytesWritable> call(
										WALManagerDistTxnProtocol instance) throws IOException {
									return instance.commitRequestAcquireLocksViaIndirection(
											transactionState.getTransactionId(), logs, keys,
											isLockAtMigratedLocation);
								}
							}, aLockCallBack);

			// For all locks that were acquired, note their corresponding logIds.
			for (int i = 0; i < aLockCallBack.getNbLocksAcquired(); i++) {
				// Add the key to trxState's lockedKeys set.
				transactionState.addLockedKey(keys.get(i));
			}

			// In any case, we'll traveled once along the network, so increasing its
			// count.
			nbNetworkHopsInTotal++;

			if (aLockCallBack.getNbLocksAcquired() == keys.size()) {
				allLocked = true;
				sysout(trxId, "Acquired all locks!");
				break;
			} else {
				allLocked = false;
				flag = aLockCallBack.getFlag();
				nbLocksAcquiredInThisRound = aLockCallBack.getNbLocksAcquired();
				nbLocksAcquiredSoFar += nbLocksAcquiredInThisRound;
				
				if (debug) {
					sysout(trxId, "Could not acquire all locks! Flag is: " + flag
						+ " , nbLocksAcquired: " + nbLocksAcquiredInThisRound + " , sent keys size: "
						+ keys.size() + "; the next to be acquired: "
						+ Bytes.toString(keys.get((int) nbLocksAcquiredInThisRound).get()));
				}

				// TODO: Have the roll-forward logic here. Once the roll-forward is
				// tried, we need to
				// try to acquire the other remaining locks.
				// Note that the DistTxnMetadata object needs to be properly maintained
				// and put inside
				// the log table for roll-forwarding to work.
				// For now, we don't do roll-forwarding, we just retry acquiring the
				// lock either at the
				// same key or at the destination key.
				// If flag is 1 -- i.e, someone has the lock -- we have to abort if that
				// key is in our
				// readSet. This is because, if someone has the write-lock, then they
				// are going to update it.
				// In that case, we'll anyway abort in the checkReadVersions function
				// after
				// obtaining this lock. To avoid all the other work, we just abort right
				// now.
				// BIG TODO: This is not the case for all benchmarks. Only for TPCC, with the way 
				// we implemented -- stock decrements are not atomic -- we are having to abort
				// even if we found a single lock on the write set.
				if (flag == 1)
					break;

				// If flag == 2 or flag == 3, the migration information we started off with is
				// not accurate. Hence we clear the lock migration cache for those locks and then
				// abort. When this transaction re-executes, "migrateLocks" function will fetch the
				// appropriate finalLockPositions. 
				// BIG TODO: This function is ordering locks based on their original logIds, not
				// the ones you find out after consulting the migration cache. This is terrible and
				// will lead to deadlocks. The whole premise is that migration information observed
				// at any moment gives the global lock order.
				if (flag == 2 || flag == 3) {
					nbDetoursEncountered++;
					// The key in question.
					int indexOfViolatingKey = (int) nbLocksAcquiredInThisRound;
					ImmutableBytesWritable key = keys.get(indexOfViolatingKey);
					LockMigrator.afterMigrationKeyMap.remove(key);
					break;
				}
			}
		}

		transactionState.setNbDetoursEncountered(nbDetoursEncountered);
		transactionState.setNbNetworkHopsInTotal(nbNetworkHopsInTotal);
		if (!allLocked) {
			// we need to abort if we haven't got all our locks and are exiting this
			// function.
			return false;
		}

		return true;
	}
}
