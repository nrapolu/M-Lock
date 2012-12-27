package ham.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HasThread;

// 1. The endpoint expects a list of locks and their final destinations.
// 2. A function in this class generates the destination keys given the initial 
// 		set of locks.
//		1. The function gathers the partitions in the table from HTable and also keeps track
//			 of their loads -- could be fetched from HMaster.
//    2. Depending on the kind of partitioning -- range/consistent -- optimizations could
//       be made on the choice of destination keys.
//    3. We should create a separate table for the destination keys. Further, since other
//       LogMigrator clients could also generate the same deestiation keys, the key should
//       have the client-id (unique to individual LogMigrators) appended to the destination
//       keys.
// 3.	Given the intital set of keys, the funtion should make sure that a migration is needed
//    and beneficial. Can we skip the migration if the original set of keys are already
//    well placed in terms of locality (on a single partition) -- i.e, the keys returned
//    by the endpoint could be a mixture of source keys and destination keys.
// 4. We are placing the destination keys on a separate table assuming that the latter will
//    always be in-memory either by assigning high-priority to it or through TAM based
//    in-memory table management.
// 5. For the first cut, the destintation table could be the same as the original table,
//    only the destination keys would be different (appended by the specific client-id).
//		1. The source will be HTable column data and destination will be myKVSPace.
//    2. The problem with direct locking of source keys is taht we'll have to go through 
//       WALManager local trx for that. On the tohter hand, destination keys are locked
//			 and managed by the endpoint using in-memory replication and other jazz. Thus, locking
//			 on destination keys might be faster than on the source keys through local trx.
//    3. Thus for the first cut, we'll always choose destination keys for source keys and
//       do in-memory locking through custom endoingt code.
public class LockMigrator extends HasThread implements Runnable {
	// TODO: Need to fetch a unique id from zookeeper whenever we initialize (in the 
	// constructor), or the client can send us a unique id in the constructor.
	Long id;
	AtomicBoolean migrateLocks = new AtomicBoolean(false);
	AtomicBoolean shutdownNotice = new AtomicBoolean(false);
	// TODO: Assuming all the keys we need to migrate are from the same table coded in
	// WALTableProperties. Different tables of the benchmark are encoded in the same table
	// either under difereent column families or as different keyspaces.
	List<List<byte[]>> lockSetsToMigrate = new LinkedList<List<byte[]>>();
	List<String> preferredSaltList = new LinkedList<String>();
	static Map<ImmutableBytesWritable, LogId> afterMigrationKeyMap = 
		new HashMap<ImmutableBytesWritable, LogId>();
	ReentrantLock overallMigrationLock = new ReentrantLock();
	HTable logTable = null;
	
	public static void sysout(String otp) {
		//System.out.println(otp);
	}
	
	public LockMigrator(Configuration conf, Long id) throws IOException {
		super();
		this.id = id;
		this.logTable = new HTable(conf, WALTableProperties.walTableName);
	}

	/*
	@Override
	public void run() {
		while (!shutdownNotice.get()) {
			if (!migrateLocks.get()) {
				synchronized(migrateLocks) {
					try {
						migrateLocks.wait();
					} catch (InterruptedException e) {
						// Fall Through.
					}
				}
			}
			
			overallMigrationLock.lock();
			try {
				migrateLocks();
			} catch (Exception e) {
				e.printStackTrace();
			} catch (Throwable e) {
				e.printStackTrace();
			} finally {
				overallMigrationLock.unlock();
			}
		}
	}
	*/
	
	@Override
	public void run() {
		try {
			migrateLocks();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void migrateLocks() throws IOException, Throwable {
		sysout("Inside MigrateLocks(), startTime: " 
				+ System.currentTimeMillis());
		// Go through the list of locks to be migrated, find their destinations and spawn
		// migration coprocessor requests for all involved logTable regions.
		// The destination logId will be generated using the unique client id of this 
		// LockMigrator.
		// Flush the present migration map, if needed.
		afterMigrationKeyMap.clear();
		final LogId destLogId = new LogId();
		// We salt the key at the end to decrease the possibility of hotspot. This is done by
		// prefixing it with a random number between 0 and 100.
		// We need salting in the first place to avoid collisions with existing keys (which are all only
		// numeric).
		// TODO: In the case of "intelligent" migration, the salt has to be decided by looking
		// at the locks being migrated. For TPC-C, we have to base it on the home-warehouse, which
		// is the warehouse of the maximum number of locks in the lockSet.
		String salt = null;
		if (!preferredSaltList.isEmpty()) {
			salt = preferredSaltList.get(0);
		} else {
			Random rand = new Random();
			salt = Integer.toString(rand.nextInt() % 100);
		}
		destLogId.setKey(Bytes.toBytes(salt + "-" + this.id + "-MigratedLock"));
		destLogId.setName(WALTableProperties.walTableName);
		
		// TODO: Extend this to consider several lockSets at once.
		List<byte[]> lockSet = lockSetsToMigrate.get(0);

		TreeMap<LogId, List<ImmutableBytesWritable>> logIdToKeyMap = new TreeMap<LogId, 
			List<ImmutableBytesWritable>>(new DistTxnState.LogIdComparator());
		// We assume that in the list of Get requests sent to this function, no two
		// of them
		// are for the same row.
		HashMap<ImmutableBytesWritable, Integer> getRowToIndexMap = new HashMap<ImmutableBytesWritable, Integer>();
		for (int index = 0; index < lockSet.size(); index++) {
			byte[] key = lockSet.get(index);
			LogId logId = WALTableProperties.getLogIdForKey(key);
			List<ImmutableBytesWritable> keyList = logIdToKeyMap.get(logId);
			if (keyList == null) {
				keyList = new LinkedList<ImmutableBytesWritable>();
				logIdToKeyMap.put(logId, keyList);
			}
			keyList.add(new ImmutableBytesWritable(key));
			getRowToIndexMap.put(new ImmutableBytesWritable(key), index);
		}

		final List<LogId> logIdList = new LinkedList<LogId>();
		final List<List<ImmutableBytesWritable>> keyLists = new LinkedList<
			List<ImmutableBytesWritable>>(); 
		for (Map.Entry<LogId, List<ImmutableBytesWritable>> entry : logIdToKeyMap.entrySet()) {
			logIdList.add(entry.getKey());
			keyLists.add(entry.getValue());
		}

		// Call migrateLocks endpoint function using a callback.
		class MigrateLocksCallBack implements Batch.Callback<List<List<ImmutableBytesWritable>>> {
			private List<List<ImmutableBytesWritable>> results = null;

			List<List<ImmutableBytesWritable>> getResults() {
				return results;
			}

			@Override
			public synchronized void update(byte[] region, byte[] row,
					List<List<ImmutableBytesWritable>> argResults) {
				// This call goes to multiple regions. Each returns the list of same
				// size (equal to the size of logIdList argument. However, only some of
				// the list elements will have values (others would be empty lists).
				// We collect the non-empty elements and place them in this class's
				// "results" list at the same position.
				if (this.results == null) {
					this.results = new ArrayList<List<ImmutableBytesWritable>>(argResults.size());
					for(int i = 0; i < argResults.size(); i++)
						this.results.add(i, null);
				}

				for (int i = 0; i < argResults.size(); i++) {
					List<ImmutableBytesWritable> boolList = argResults.get(i);
					if (boolList != null && !boolList.isEmpty()) {
						this.results.set(i, boolList);
						//System.out.println("In MigrateLocksCallBack, at index: " + i + " adding boolList. "
					}
				}
			}
		}

		LogId firstLogId = logIdToKeyMap.firstKey();
		LogId lastLogId = logIdToKeyMap.lastKey();

		// Swap first and lastLogId if the later is less than the former. This can happen when the 
		// contentionOrder is changed by the user. Our use of the first and last is to have bounds
		// on the regions, nothing more.
		if (LogId.compareOnlyKey(firstLogId, lastLogId) > 0) {
			LogId tmp = firstLogId;
			firstLogId = lastLogId;
			lastLogId = tmp;
		}
		
		MigrateLocksCallBack migrateLocksCallBack = new MigrateLocksCallBack();
		logTable.coprocessorExec(WALManagerDistTxnProtocol.class, firstLogId.getKey(), lastLogId.getKey(),
				new Batch.Call<WALManagerDistTxnProtocol, List<List<ImmutableBytesWritable>>>() {
					@Override
					public List<List<ImmutableBytesWritable>> call(WALManagerDistTxnProtocol instance)
							throws IOException {
						return instance.migrateLocks(id, logIdList, keyLists, destLogId);
					}
				}, migrateLocksCallBack);
		List<List<ImmutableBytesWritable>> results = migrateLocksCallBack.getResults();
		
		// Gather the migration results and store them in a map. Use this map while acquiring and
		// releasing locks.
		// Traverse the keyLists and results list in sync and form the mapping.
		sysout ("After lock migration, printing the final location of locks");
		for (int i = 0; i < keyLists.size(); i++) {
			List<ImmutableBytesWritable> keyList = keyLists.get(i);
			LogId origLogId = logIdList.get(i);
			List<ImmutableBytesWritable> migratedLockPositions = results.get(i);
			assert(migratedLockPositions.size() == keyList.size());
			for (int j = 0; j < keyList.size(); j++) {
				ImmutableBytesWritable key = keyList.get(j);
				sysout ("For logId: " + origLogId.toString() + ", key: " + Bytes.toString(key.get()));
				ImmutableBytesWritable migratedLockPosition = migratedLockPositions.get(j);
				boolean isMigrated = false;
				LogId migratedLogId = WALTableProperties.getLogIdFromMigratedKey(migratedLockPosition.get());
				if (migratedLogId.equals(destLogId)) {
					isMigrated = true;
				}
				
				if (isMigrated) {
					migratedLogId.setCommitType(LogId.ONLY_DELETE);
					afterMigrationKeyMap.put(key, migratedLogId);
					sysout ("migrated logId is: " + migratedLogId.toString());
				} else {
					migratedLogId.setCommitType(LogId.ONLY_UNLOCK);
					afterMigrationKeyMap.put(key, migratedLogId);
					sysout ("migrated logId is: " + migratedLogId.toString());
				}
			}
		}
		sysout("Inside MigrateLocks(), endTime: " 
				+ System.currentTimeMillis());
	}
	
	public void addLockSetToMigrate(List<byte[]> lockSet) {
		lockSetsToMigrate.add(lockSet);
	}
	
	public void addPreferredSaltForLockTablePartition(String salt) {
		preferredSaltList.add(salt);
	}
	
	public Map<ImmutableBytesWritable, LogId> getAfterMigrationKeyMap() {
		return afterMigrationKeyMap;
	}
	
	public void migrateLocksRequested() {
		synchronized (migrateLocks) {
			migrateLocks.set(true);
			migrateLocks.notifyAll();
		}
	}

	/**
	 * Called by WAL Manager Client to wake up this thread if it sleeping. It is
	 * sleeping if flushLock is not held. This is called when WAL Manager is ready to
	 * quit.
	 */
	public void interruptIfNecessary() {
		try {
			overallMigrationLock.lock();
			this.interrupt();
		} finally {
			overallMigrationLock.unlock();
		}
	}
}
