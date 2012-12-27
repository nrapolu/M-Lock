package ham.wal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class DistTxnState {
	static final Log LOG = LogFactory.getLog(DistTxnState.class);

	private final long transactionId;

	private TreeMap<HRegionLocation, List<LogId>> regionToLogIdMap = new TreeMap<HRegionLocation, List<LogId>>(
			new MyHRegionLocationComparator());
	/**
	 * Regions to ignore in the twoPase commit protocol. They were read only, or
	 * already said to abort.
	 */
	private Set<HRegionLocation> regionsToIgnore = new HashSet<HRegionLocation>();

	private TreeMap<LogId, TreeSet<Action>> writeBufferPerLogId = new TreeMap<LogId, TreeSet<Action>>(
			new LogIdComparator());

	private Map<ImmutableBytesWritable, Action> writeBufferPerKey = new HashMap<ImmutableBytesWritable, Action>();

	private Map<ImmutableBytesWritable, Result> readCache = new HashMap<ImmutableBytesWritable, Result>();

	private HashSet<ImmutableBytesWritable> lockedKeys = new HashSet<ImmutableBytesWritable>();

	private DistTxnMetadata distTxnMetadata = null;

	private TreeMap<LogId, TreeSet<ImmutableBytesWritable>> pessimisticLocksPerLogId = 
		new TreeMap<LogId, TreeSet<ImmutableBytesWritable>>(new LogIdComparator());
	
	private long nbDetoursEncountered = 0;
	private long nbNetworkHopsInTotal = 0;
	private long lockMigrationTime = 0;
	
	public static int contentionOrder = 0;
	
	public long getLockMigrationTime() {
		return lockMigrationTime;
	}

	public void setLockMigrationTime(long lockMigrationTime) {
		this.lockMigrationTime = lockMigrationTime;
	}

	public static void sysout(long trxId, String otp) {
		System.out.println(trxId + " : " + otp);
	}
	
	public DistTxnState(final long transactionId) {
		this.transactionId = transactionId;
		this.distTxnMetadata = new DistTxnMetadata(this.transactionId);
	}

	public void addToWriteBuffer(LogId logId, HRegionLocation regionLocation,
			Action act) {
		TreeSet<Action> multiAction = writeBufferPerLogId.get(logId);
		if (multiAction == null) {
			multiAction = new TreeSet<Action>();
			writeBufferPerLogId.put(logId, multiAction);
		}
		multiAction.add(act);

		// Add this logId to its region mapping.
		List<LogId> logIdList = regionToLogIdMap.get(regionLocation);
		if (logIdList == null) {
			logIdList = new LinkedList<LogId>();
			regionToLogIdMap.put(regionLocation, logIdList);
		}
		logIdList.add(logId);

		// Similarly add this Action to the writeBufferPerKey.
		writeBufferPerKey.put(new ImmutableBytesWritable(act.getAction().getRow()),
				act);
	}

	public void addToPessimisticLocksPerLogId(LogId logId, ImmutableBytesWritable lock) {
		TreeSet<ImmutableBytesWritable> locks = pessimisticLocksPerLogId.get(logId);
		if (locks == null) {
			locks = new TreeSet<ImmutableBytesWritable>();
			pessimisticLocksPerLogId.put(logId, locks);
		}
		locks.add(lock);
	}
		
	public TreeMap<LogId, TreeSet<ImmutableBytesWritable>> getPessimisticLocksPerLogId() {
		return pessimisticLocksPerLogId;
	}

	public void setPessimisticLocksPerLogId(
			TreeMap<LogId, TreeSet<ImmutableBytesWritable>> pessimisticLocksPerLogId) {
		this.pessimisticLocksPerLogId = pessimisticLocksPerLogId;
	}

	public Result getFromReadCache(ImmutableBytesWritable key) {
		return readCache.get(key);
	}

	public void addReadInfoToDistTxnMetadata(ImmutableBytesWritable key,
			long version) {
		this.distTxnMetadata.addReadInfo(key, version);
	}

	public void addToReadCache(ImmutableBytesWritable key, Result r) {
		readCache.put(key, r);
	}
	
	public boolean isPresentInReadSet(ImmutableBytesWritable key) {
		return (this.readCache.get(key) != null);
	}
	
	public void addWriteInfoToDistTxnMetadata(Put put) {
		// Create a shadow object using the key of the put object. Shadow object
		// should
		// also contain the key of the DistTxnMetadata object, so as to enable
		// roll-forwarding. Also, without the transaction specific information, two
		// different
		// transactions might end up writing to the same shadow object.
		// For now, the key of the shadow object will be the same as the key of the
		// original
		// object, just with an appended ("#" + transactionId) present in the
		// DistTxnMetadata.
		// Appending at the end
		// will not mess up the key structure used to decipher the logId for the
		// key.
		// The transaction responsible for this shadow key can be deciphered from
		// the key itself.
		// Even otherwise, the write lock is anyway going to have the
		// key of the DistTxnMetadata object; thus, we can use it to track the
		// object and
		// roll-forward.
		byte[] key = put.getRow();
		byte[] shadowKey = Bytes.toBytes(Bytes.toString(key)
				+ WALTableProperties.shadowKeySeparator
				+ this.transactionId);
		distTxnMetadata.addWriteInfo(new ImmutableBytesWritable(key),
				new ImmutableBytesWritable(shadowKey));
	}

	// return null if there are no writes to that LogId.
	public TreeSet<Action> getFromWriteBuffer(LogId logId) {
		return writeBufferPerLogId.get(logId);
	}

	public List<Put> getShadowPuts() {
		List<Put> shadowPuts = new LinkedList<Put>();
		List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> putShadowList = this.distTxnMetadata
				.getPutShadowList();
		for (Pair<ImmutableBytesWritable, ImmutableBytesWritable> keyPair : putShadowList) {
			ImmutableBytesWritable originalKey = keyPair.getFirst();
			ImmutableBytesWritable shadowKey = keyPair.getSecond();
			Action action = writeBufferPerKey.get(originalKey);
			// TODO: For the first cut, I'm assuming all actions are of type Put.
			// Functionality for Delete will be added later.
			if (action.getAction() instanceof Put) {
				Put p = (Put) action.getAction();
				Put shadowPut = new Put(shadowKey.get(), p.getTimeStamp());
				//TODO: This way of setting values won't work because the familyMap contains KeyValue
				// objects which in turn have their row values preset to the original rows of Put
				// objects. For now we are just setting dummy value, but you'll later have to iterate
				// on all the KeyValues and create another KeyValue with same contents but different 
				// row.
				//shadowPut.setFamilyMap(p.getFamilyMap());
				shadowPut.add(WALTableProperties.dataFamily, WALTableProperties.dataColumn, 
						WALTableProperties.appTimestamp, Bytes.toBytes("Dummy Shadow Value"));
				shadowPut.setWriteToWAL(true);
				shadowPuts.add(shadowPut);
			}
		}
		return shadowPuts;
	}

	public List<byte[]> getPessimisticLockList() {
		List<byte[]> pessimisticLockList = new ArrayList<byte[]>();
		for (TreeSet<ImmutableBytesWritable> lockSet: pessimisticLocksPerLogId.values()) {
			for (ImmutableBytesWritable lock: lockSet) 
				pessimisticLockList.add(lock.get());
		}
		return pessimisticLockList;
	}
	
	public List<byte[]> getSimplePutKeyList() {
		List<byte[]> simplePutKeyList = new ArrayList<byte[]>();
		for (ImmutableBytesWritable putKey : writeBufferPerKey.keySet()) {
			simplePutKeyList.add(putKey.get());
		}
		return simplePutKeyList;
	}

	public TreeMap<LogId, TreeSet<Action>> getWriteBufferWithLogIds() {
		return this.writeBufferPerLogId;
	}

	public TreeMap<HRegionLocation, List<LogId>> getRegionToLogIdMap() {
		return this.regionToLogIdMap;
	}

	public List<Pair<ImmutableBytesWritable, Long>> getReadVersionList() {
		return distTxnMetadata.getReadVersionList();
	}

	public Map<LogId, List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>>> getLogIdToAllWritesMap() {
		Map<LogId, List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>>> logIdToAllWritesMap = new HashMap<LogId, List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>>>();
		List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> putShadowList = this.distTxnMetadata
				.getPutShadowList();
		for (Pair<ImmutableBytesWritable, ImmutableBytesWritable> keyPair : putShadowList) {
			ImmutableBytesWritable originalKey = keyPair.getFirst();
			LogId logIdForKey = WALTableProperties.getLogIdForKey(originalKey.get());

			List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> allWrites = logIdToAllWritesMap
					.get(logIdForKey);
			if (allWrites == null) {
				allWrites = new LinkedList<Pair<ImmutableBytesWritable, ImmutableBytesWritable>>();
				logIdToAllWritesMap.put(logIdForKey, allWrites);
			}
			allWrites.add(keyPair);
		}
		return logIdToAllWritesMap;
	}

	Set<HRegionLocation> getRegionsToIngore() {
		return regionsToIgnore;
	}

	void addRegionToIgnore(final HRegionLocation region) {
		regionsToIgnore.add(region);
	}

	/**
	 * Get the transactionId.
	 * 
	 * @return Return the transactionId.
	 */
	public long getTransactionId() {
		return transactionId;
	}

	public void addLockedKey(ImmutableBytesWritable lockedKey) {
		this.lockedKeys.add(lockedKey);
	}

	HashSet<ImmutableBytesWritable> getLockedKeys() {
		return this.lockedKeys;
	}
	
	public long getNbDetoursEncountered() {
		return nbDetoursEncountered;
	}

	public void setNbDetoursEncountered(long nbDetoursEncountered) {
		this.nbDetoursEncountered = nbDetoursEncountered;
	}

	public long getNbNetworkHopsInTotal() {
		return nbNetworkHopsInTotal;
	}

	public void setNbNetworkHopsInTotal(long nbNetworksHopsInTotal) {
		this.nbNetworkHopsInTotal = nbNetworksHopsInTotal;
	}

	public static class ActionComparator implements Comparator<Action> {
		@Override
		public int compare(Action act1, Action act2) {
			// TODO Auto-generated method stub
			return Bytes.compareTo(act1.getAction().getRow(), act2.getAction()
					.getRow());
		}
	}

	public static class LogIdComparator implements Comparator<LogId> {
		@Override
		public int compare(LogId o1, LogId o2) {
			// TODO Auto-generated method stub
			int result;
			if (o1.getName() == null && o2.getName() == null)
				result = 0;
			else
				result = Bytes.compareTo(o1.getName(), o2.getName());
			if (result == 0)
				result = Bytes.compareTo(o1.getKey(), o2.getKey());
			if (contentionOrder == 1)
				result = -1 * result;
			
			return result;
		}
	}

	public static class MyHRegionLocationComparator implements
			Comparator<HRegionLocation> {
		@Override
		public int compare(HRegionLocation arg0, HRegionLocation arg1) {
			// TODO Auto-generated method stub
			return arg0.getRegionInfo().compareTo(arg1.getRegionInfo());
		}
	}
}
