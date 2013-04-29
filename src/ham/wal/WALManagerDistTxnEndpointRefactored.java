package ham.wal;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;

public class WALManagerDistTxnEndpointRefactored extends WALManagerDistTxnEndpoint
		implements WALManagerDistTxnProtocol {
	RemoteWALCommunicator remoteWALCommunicator = null;
	
	public static void sysout(String otp) {
		 //System.out.println(otp);
	}
	
	public void startRemoteWALCommunicator() {
		try {
			remoteWALCommunicator = new RemoteWALCommunicator(this, 200,
					200, this.conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		UncaughtExceptionHandler handler = new UncaughtExceptionHandler() {
			public void uncaughtException(Thread t, Throwable e) {
				System.err.println("Uncaught exception in service thread "
						+ t.getName() + e);
			}
		};

		Threads.setDaemonThreadRunning(this.remoteWALCommunicator.getThread(),
		 ".remoteWALCommunicator", handler);
	}
	
	@Override
	public void start(org.apache.hadoop.hbase.CoprocessorEnvironment env) {
		sysout("Starting coprocessor from refactored code");
		super.start(env);
		RegionCoprocessorEnvironment regionEnv = (RegionCoprocessorEnvironment) env;
		HRegion region = regionEnv.getRegion();
		if (region.getTableDesc().getNameAsString().equalsIgnoreCase(WALTableProperties.WAL_TABLENAME)) {
			// Create the RemoteWALCommunicator if it is null.
			if (remoteWALCommunicator == null) {
				sysout("Starting RemoteWALCommunicator");
				startRemoteWALCommunicator();
			}
		}
	}
	
	@Override
	public void stop(CoprocessorEnvironment env) {
		// TODO Auto-generated method stub
		super.stop(env);
		if (this.remoteWALCommunicator != null) {
			sysout("Stopping remoteWALCommunicator");
			this.remoteWALCommunicator.askToStop();
			this.remoteWALCommunicator.interruptIfNecessary();
		}
	}
	
	//TODO: This function implementation is very crude. Lots of refactoring needed. Reasons are: 
	// 1. In memory processing happens before walEdits -- not safe.
	// 2. Locks in EndpointForKVSpace commit function are given up before the WALEdits -- not safe.
	// 3. After some micro-instrumentation, the inMemOps time is around 4 ms and the flushtime
	// 		is also around 4ms. Total avg commit stage time is 14ms. Around 6ms are being taken
	// 		for EndPoint invocation. This is probably the reason, HBase folks moved from EndPoint
	//		to some CoprocessorService!
	public Boolean commitWritesPerEntityGroupWithShadowsSingleThreaded(
			Long transactionId, List<LogId> logIdList, List<List<Put>> allUpdates,
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

		for (int logIndex = 0; logIndex < logIdList.size(); logIndex++) {
			LogId logId = logIdList.get(logIndex);
			// If the logId does not belong to this region, we just skip.
			if (!HRegion.rowIsInRange(region.getRegionInfo(), logId.getKey()))
				continue;

			ourLogIds.add(logId);
			localIdToGlobalIdMap.put(ourIdCount, logIndex);
			ourIdCount++;
		}

		List<WALEdit> accumulatedWalEdits = new LinkedList<WALEdit>();
		List<Row> firstSetOfCausalLockReleases = new LinkedList<Row>();
		List<Row> secondSetOfCausalLockReleases = new LinkedList<Row>();
		boolean overallCommitStatus = true;

		//long startInMemOpsTime = System.currentTimeMillis();
		// TODO: Ideally, the shadow objects need to be fetched from the store. For
		// now, we don't really
		// use this data as this function is taking in the actual updates (Puts).
		// If another transaction is doing a roll-forward, we won't have those Puts.
		for (int localId = 0; localId < ourLogIds.size(); localId++) {
			// Execute the local transaction on the logId using the fetched snapshot.
			int globalId = localIdToGlobalIdMap.get(localId);
			final LogId logId = logIdList.get(globalId);
			sysout("Inside Commit per entity group, processing logId: "
					+ logId.toString());
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

				// For every KeyValue, we create a write and add it to finalWrites.
				// Typically, we expect only one KeyValue with the value information.
				// TODO: This can be used to have writes to multiple columns; in which
				// case,
				// put.getFamilyMap() would be the starting point for iteration.
				if (!put.isEmpty()) {
					for (Map.Entry<byte[], List<KeyValue>> familyMap : put.getFamilyMap()
							.entrySet()) {
						byte[] family = familyMap.getKey();
						List<KeyValue> kvs = familyMap.getValue();
						for (KeyValue kv : kvs) {
							byte[] itemName = Bytes.toBytes(Bytes
									.toString(WALTableProperties.dataTableName)
									+ Write.nameDelimiter
									+ Bytes.toString(family)
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
			}

			// Prepare a check object and commit the write-set with optimistic
			// checks. In this case, we didn't have any reads, so the check object
			// will be empty. We don't even get a snapshot and stuff since the commit function never
			// stores anything in the WAL snapshot, it always flushes writes down to the region hosting data.
			// -- which in our design resides on the same node hosting the log. 
			// Since we moved to the new commit function in 
			// WALManagerEndpointForMyKVSpaceRefactored, we don't need any meaningful timestamp for check. 
			// We simply mark its timestamp as -1.
			final Check check = new Check();
			check.setTimestamp(-1);

			// Commit the check object and the write-sets.
			// boolean commitResponse = super.commit(logId, check, finalWrites, null);
			boolean commitStatus = this.commitToMemory(logId, check, finalWrites,
					toBeUnlockedKeys, commitTypeInfo, firstSetOfCausalLockReleases,
					secondSetOfCausalLockReleases, accumulatedWalEdits);
			overallCommitStatus = overallCommitStatus && commitStatus;
		}
		//long stopInMemOpsTime = System.currentTimeMillis();
		//System.out.println("InMemOpsTime: " + (stopInMemOpsTime - startInMemOpsTime));
		
		//long startFlushingTime = System.currentTimeMillis();
		// Group the accumulatedWalEdits into one and append them to log.
		WALEdit finalWalEdit = new WALEdit();
		for (WALEdit walEdit : accumulatedWalEdits) {
			for (KeyValue kv : walEdit.getKeyValues()) {
				finalWalEdit.add(kv);
			}
		}

		long now = EnvironmentEdgeManager.currentTimeMillis();
		HLog log = region.getLog();
		HTableDescriptor htd = new HTableDescriptor(
				WALTableProperties.dataTableName);
		log.append(region.getRegionInfo(), WALTableProperties.dataTableName,
				finalWalEdit, now, htd);
		// Flush the firstSetOfCausalLockReleases. In our setting, the data object and the lock object
		// reside on the same region. Thus, we can directly use HRegion function calls instead of going
		// through dataTable. Also we are assuming that all Lock Releases on H-WAL objects are being
		// done through Put operations.
		if (!firstSetOfCausalLockReleases.isEmpty()) {
			Put[] localRegionPuts = new Put[firstSetOfCausalLockReleases.size()];
			for (int i = 0; i < firstSetOfCausalLockReleases.size(); i++) {
				Row r = firstSetOfCausalLockReleases.get(i);
				localRegionPuts[i] = (Put)r;
			}
			region.put(localRegionPuts);
		}
		// Flush the secondSetOfCausalLockReleases. They are lock releases on the
		// M-WALs.
		if (!secondSetOfCausalLockReleases.isEmpty()) { 
			remoteWALCommunicator.addToCausalReleasesQueue(secondSetOfCausalLockReleases);
		}

		//long stopFlushingTime = System.currentTimeMillis();
		//System.out.println("Flushing time: " + (stopFlushingTime - startFlushingTime));
		return overallCommitStatus;
	}
	
	// I'm assuming the destination keys are the same as the original keys, only
	// the logId will change.
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
		// Among the logIds sent to us, not all them will be processed by us. The
		// map will contain
		// the indirection
		// from "our id" to the id present in the sent list.
		int ourKeyId = 0;
		TreeMap<Integer, String> localKeyIdToGlobalKeyIdMap = new TreeMap<Integer, String>();
		TreeMap<Integer, Boolean> migrationInfoIndexedByLocalKey = new TreeMap<Integer, Boolean>();
			
		boolean atLeastOneLockIsAttemptingMigration = false;
		final WALManagerDistTxnEndpointRefactored walManagerDistTxnEndpoint = this;
		for (int logIndex = 0; logIndex < logs.size(); logIndex++) {
			final LogId logId = logs.get(logIndex);
			// If the logId does not belong to this region, we just skip.
			if (!HRegion.rowIsInRange(region.getRegionInfo(), logId.getKey())) {
				// System.err.println("ROW NOT IN REGION: logId: "
				// + Bytes.toString(logId.getKey()) + ", region info: "
				// + region.getRegionInfo().toString());
				returnValList.set(logIndex, new LinkedList<ImmutableBytesWritable>());
				continue;
			}

			List<ImmutableBytesWritable> correspKeys = keys.get(logIndex);
			for (int keyIndex = 0; keyIndex < correspKeys.size(); keyIndex++) {
				localKeyIdToGlobalKeyIdMap.put(ourKeyId, Integer.toString(logIndex)
						+ "=" + Integer.toString(keyIndex));
				final ImmutableBytesWritable key = correspKeys.get(keyIndex);
				// Note that the destination key is th same as the original key,
				// only the
				// LogId changes.
				boolean canMigrate = walManagerDistTxnEndpoint.canMigrateLock(transactionId,
										logId, key, destLogId, key);
				sysout("For key: " + Bytes.toString(key.get()) + ", canMigrate returned: " + canMigrate);
				migrationInfoIndexedByLocalKey.put(ourKeyId, canMigrate);
				// Request to create a lock to be placed at M-WAL.
				if (canMigrate) {
					walManagerDistTxnEndpoint.createLockToPlaceAtMWAL(
							transactionId, logId, key, destLogId, key);
					atLeastOneLockIsAttemptingMigration = true;
				}
				
				ourKeyId++;
			}
		}

		if (atLeastOneLockIsAttemptingMigration) {
			// Flush M-WAL lock creation requests -- i.e, place those locks.
			walManagerDistTxnEndpoint.placeLocksAtMWAL();
		}
		
		// Go through the futures and grab the results.
		for (Integer localKeyId: migrationInfoIndexedByLocalKey.keySet()) {
			boolean didPlaceMigrationInfoAtMWAL = migrationInfoIndexedByLocalKey.get(localKeyId);
			String globalKeyId = localKeyIdToGlobalKeyIdMap.get(localKeyId);
			String[] tokens = globalKeyId.split("=");
			int logIndex = Integer.parseInt(tokens[0]);
			int keyIndex = Integer.parseInt(tokens[1]);
			
			List<ImmutableBytesWritable> finalLockPositionList = returnValList.get(logIndex);
			if (finalLockPositionList == null) {
				finalLockPositionList = new ArrayList<ImmutableBytesWritable>();
				returnValList.set(logIndex, finalLockPositionList);
			}
			
			if (didPlaceMigrationInfoAtMWAL) {
				ImmutableBytesWritable key = keys.get(logIndex).get(keyIndex);
				ImmutableBytesWritable finalLockPosition = walManagerDistTxnEndpoint.addMigrationInfoOnLocalHWALAndReturnFinalLockPosition(
						transactionId, logs.get(logIndex), key,
						destLogId, key);
				finalLockPositionList.add(finalLockPosition);
				sysout("For key: " + Bytes.toString(key.get()) + 
						", finalLockPosition returned: " + Bytes.toString(finalLockPosition.get()));
			} else {
				ImmutableBytesWritable key = keys.get(logIndex).get(keyIndex);
				ImmutableBytesWritable finalLockPosition = walManagerDistTxnEndpoint.readAndReturnFinalLockPosition(
						transactionId, logs.get(logIndex), key,
						destLogId, key);
				finalLockPositionList.add(finalLockPosition);
				sysout("For key: " + Bytes.toString(key.get()) + 
						", finalLockPosition returned: " + Bytes.toString(finalLockPosition.get()));
			}
		}
		return returnValList;
	}	
}
