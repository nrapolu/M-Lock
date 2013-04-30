package ham.wal.regionobserver;

import ham.wal.TPCCTableProperties;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class CumulativeInMemoryStateWithSingleHashMapAndMigrationPolicies {
	private static final Log LOG = LogFactory
			.getLog(CumulativeInMemoryStateWithSingleHashMap.class);
	private final HRegionInfo regionInfo;

	private static final String SEPARATOR = "@";
	private ConcurrentHashMap<String, KeyValue> myInMemStore = new ConcurrentHashMap<String, KeyValue>(
			2000000, (float) 0.75, 200);
	// private ConcurrentNavigableMap<String, ConcurrentHashMap<String,
	// ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>>>>
	// myInMemStore = new ConcurrentSkipListMap<String, ConcurrentHashMap<String,
	// ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>>>>();

	private List<Delete> deletes = new LinkedList<Delete>();

	private boolean debug = false;
	private void sysout(String line) {
		// System.out.println(line);
	}

	public CumulativeInMemoryStateWithSingleHashMapAndMigrationPolicies(
			HRegionInfo regionInfo) {
		super();
		this.regionInfo = regionInfo;
	}

	public List<Delete> getDeletes() {
		return deletes;
	}

	// Timestamp is taken as an argument as it could be different to what is
	// present inside kv.
	// Rest of the information can anyway be got from kv.
	private void addToDeletes(Long timestamp, KeyValue kv) {
		switch (KeyValue.Type.codeToType(kv.getType())) {
		case DeleteFamily: {
			Delete del = new Delete(kv.getRow());
			del.deleteFamily(kv.getFamily());
			deletes.add(del);
			break;
		}
		case Delete: {
			// Deleting a specific column for a specific timestamp.
			Delete del = new Delete(kv.getRow());
			del.deleteColumn(kv.getFamily(), kv.getQualifier(), timestamp);
			deletes.add(del);
			break;
		}
		case DeleteColumn: {
			// Here we delete all versions of the column. However, the
			// deleteColumns() allows
			// for deletion of all cells with timestamps less than the give one.
			// Since,
			// we don't expect to use such high funda functionality, we just
			// delete all
			// versions of that column.
			Delete del = new Delete(kv.getRow());
			del.deleteColumns(kv.getFamily(), kv.getQualifier());
			deletes.add(del);
			break;
		}
		}
	}

	public void addPut(final Put write) {
		if (debug)
			sysout("In addPut, about to insert Put: " + write.toString());
		// Timestamps inside KeyValues will be updated only when they are marked as
		// LATEST_TIMESTAMP.
		updateLatestTimestamp(write.getFamilyMap().values(), EnvironmentEdgeManager
				.currentTimeMillis());
		String row = Bytes.toString(write.getRow());
		Map<byte[], List<KeyValue>> familyMap = write.getFamilyMap();
		for (byte[] family : familyMap.keySet()) {
			String familyStr = Bytes.toString(family);
			List<KeyValue> kvList = familyMap.get(family);
			for (KeyValue kv : kvList) {
				String colStr = Bytes.toString(kv.getQualifier());
				StringBuffer keyStrBuffer = new StringBuffer();
				keyStrBuffer.append(row).append(SEPARATOR).append(familyStr).append(SEPARATOR).append(
						colStr);
				keyStrBuffer.append(SEPARATOR).append(kv.getTimestamp());
				String key = keyStrBuffer.toString();

				boolean isLTorDT = false;
				long currentCountOfDTFreq = 0;
				long currentCountOfLTFreq = 0;
				if (colStr.startsWith(TPCCTableProperties.dtFreqColumnStr)) {
					KeyValue fromStoreKV = myInMemStore.get(key);
					if (fromStoreKV != null) {
						isLTorDT = true;
						// Note that we added +1 to account for this trx.
						currentCountOfDTFreq = Bytes.toLong(fromStoreKV.getValue()) + 1;

						// Create key to fetch ltFreq. Use it to calculate new runningAvg if
						// needed.
						StringBuffer ltFreqKeyStrBuffer = new StringBuffer();
						ltFreqKeyStrBuffer.append(row).append(SEPARATOR).append(familyStr)
								.append(TPCCTableProperties.ltFreqColumnStr);
						ltFreqKeyStrBuffer.append(SEPARATOR).append(kv.getTimestamp());
						currentCountOfLTFreq = Bytes.toLong(myInMemStore.get(
								ltFreqKeyStrBuffer.toString()).getValue());

						if (currentCountOfLTFreq + currentCountOfDTFreq
								% TPCCTableProperties.trxSumThresholdForRunningAvgEvaluation != 0) {
							KeyValue newKeyWithUpdatedDTFreq = new KeyValue(kv.getRow(), kv
									.getFamily(), kv.getQualifier(), Bytes
									.toBytes(currentCountOfDTFreq));
							myInMemStore.put(key, newKeyWithUpdatedDTFreq);
							continue;
						}
					}
				} else if (colStr.startsWith(TPCCTableProperties.ltFreqColumnStr)) {
					KeyValue fromStoreKV = myInMemStore.get(key);
					if (fromStoreKV != null) {
						isLTorDT = true;
						// Note that we added +1 to account for this trx.
						currentCountOfLTFreq = Bytes.toLong(fromStoreKV
								.getValue()) + 1;

						// Create key to fetch dtFreq. Use it to calculate new runningAvg if
						// needed.
						StringBuffer dtFreqKeyStrBuffer = new StringBuffer();
						dtFreqKeyStrBuffer.append(row).append(SEPARATOR).append(familyStr)
								.append(TPCCTableProperties.dtFreqColumnStr);
						dtFreqKeyStrBuffer.append(SEPARATOR).append(kv.getTimestamp());
						currentCountOfDTFreq = Bytes.toLong(myInMemStore.get(
								dtFreqKeyStrBuffer.toString()).getValue());

						if (currentCountOfLTFreq + currentCountOfDTFreq
								% TPCCTableProperties.trxSumThresholdForRunningAvgEvaluation != 0) {
							KeyValue newKeyWithUpdatedLTFreq = new KeyValue(kv.getRow(), kv
									.getFamily(), kv.getQualifier(), Bytes
									.toBytes(currentCountOfLTFreq));
							myInMemStore.put(key, newKeyWithUpdatedLTFreq);
							continue;
						}
					}
				}

				// If code reached here and isLTorDT, then it is time to change the
				// runningAvg.
				if (isLTorDT
						&& currentCountOfLTFreq + currentCountOfDTFreq
								% TPCCTableProperties.trxSumThresholdForRunningAvgEvaluation == 0) {
					// create key for runningAvg. Fetch it and update it
					StringBuffer runningAvgKeyStrBuffer = new StringBuffer();
					runningAvgKeyStrBuffer.append(row).append(SEPARATOR)
							.append(familyStr)
							.append(TPCCTableProperties.runningAvgColumnStr);
					runningAvgKeyStrBuffer.append(SEPARATOR).append(kv.getTimestamp());
					double currentRunningAvg = Bytes.toDouble(myInMemStore.get(
							runningAvgKeyStrBuffer.toString()).getValue());

					double newRunningAvg = TPCCTableProperties.weightForOldRunningAvg
							* currentRunningAvg
							+ TPCCTableProperties.weightForNewRunningAvg
							* (currentCountOfDTFreq / (currentCountOfDTFreq + currentCountOfLTFreq));

					KeyValue newKeyWithUpdatedDTFreq = new KeyValue(kv.getRow(), kv
							.getFamily(), TPCCTableProperties.dtFrequencyColumn, Bytes
							.toBytes(TPCCTableProperties.zero));
					KeyValue newKeyWithUpdatedLTFreq = new KeyValue(kv.getRow(), kv
							.getFamily(), TPCCTableProperties.ltFrequencyColumn, Bytes
							.toBytes(TPCCTableProperties.zero));
					KeyValue newKeyWithUpdatedRunningAvg = new KeyValue(kv.getRow(), kv
							.getFamily(),
							TPCCTableProperties.runningAvgForDTProportionColumn, Bytes
									.toBytes(newRunningAvg));

					myInMemStore.put(key, newKeyWithUpdatedDTFreq);
					myInMemStore.put(key, newKeyWithUpdatedLTFreq);
					myInMemStore.put(key, newKeyWithUpdatedRunningAvg);
					continue;
				}

				if (debug)
					sysout("InMemStore##: Putting at timestamp: " + kv.getTimestamp()
						+ ", is this kv: " + kv.toString());
				myInMemStore.put(key, kv);
			}
		}
	}

	boolean addDelete(final Delete delete) {
		String row = Bytes.toString(delete.getRow());

		boolean retVal = true;
		Map<byte[], List<KeyValue>> delFamilyMap = delete.getFamilyMap();
		for (byte[] family : delFamilyMap.keySet()) {
			String familyStr = Bytes.toString(family);
			List<KeyValue> kvList = delFamilyMap.get(family);
			if (kvList == null)
				continue;

			for (KeyValue kv : kvList) {
				switch (KeyValue.Type.codeToType(kv.getType())) {
				case DeleteFamily: {
					// #NAR: TODO: Here we are deleting all the columns for all timestamps
					// in this
					// family. However, the deleteFamily() call specifies the timestamp
					// below
					// which all columns should be deleted. We are assuming we'll never
					// need such
					// high funda functionality. Also, while creating the Delete object,
					// deleteFamily()
					// overrides other delete calls such as deleteColumn() [version to
					// delete
					// a column with a specific timestamp] and deleteColumns() [version to
					// delete
					// cells with timestamps less than the one provided].

					// First we clear the Timestamp TreeMap for all columns in this
					// family, and then
					// place the argument "Delete KV object" with the latest timestamp in
					// all
					// columns. This is to make sure that, when a scan is done on this
					// column,
					// the cache will have a delete entry at this timestamp, which will
					// let the
					// scan know that it should ignore the value sent by the region, and
					// essentially
					// send nothing to the client.
					// BIG TODO: We need to iterate over keys of inMemState to capture
					// keys starting
					// with the family given by this Delete object. For now we just ignore
					// this call,
					// as it is not used by our use-case.
					// ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>>
					// localColMap = famMap
					// .get(familyStr);
					// for (TreeMap<Long, KeyValue> timestampMap : localColMap
					// .values()) {
					// timestampMap.clear();
					// long latestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
					// timestampMap.put(latestTimestamp, kv);
					// }
					break;
				}
				case Delete: {
					// Deleting a specific column for a specific timestamp.
					String delCol = Bytes.toString(kv.getQualifier());
					Long timestamp = kv.getTimestamp();
					String key = row + SEPARATOR + familyStr + SEPARATOR + delCol
							+ SEPARATOR + timestamp;

					if (myInMemStore.get(key) == null) {
						// some one deleted the entire column, or probably there was never a
						// column.
						// Push this delete to the HRegion, so that the delete's effects are
						// visible everywhere.
						retVal = retVal && false;
						break;
					}

					// We place the argument "Delete KV object" at the position where we
					// deleted
					// something. This is to make sure that, when a scan is done on this
					// column,
					// the cache will have a delete entry at this timestamp, which will
					// let the
					// scan know that it should ignore the value sent by the region, and
					// essentially
					// send nothing to the client.
					myInMemStore.put(key, kv);
					break;
				}
				case DeleteColumn: {
					// Here we delete all versions of the column. However, the
					// deleteColumns() allows
					// for deletion of all cells with timestamps less than the give one.
					// Since,
					// we don't expect to use such high funda functionality, we just
					// delete all
					// versions of that column.
					// String delCol = Bytes.toString(kv.getQualifier());
					// First we clear the Timestamp TreeMap present at that column, and
					// then
					// place the argument "Delete KV object" at the position where we
					// deleted
					// something. This is to make sure that, when a scan is done on this
					// column,
					// the cache will have a delete entry at this timestamp, which will
					// let the
					// scan know that it should ignore the value sent by the region, and
					// essentially
					// send nothing to the client.

					// String colKey = familyStr + SEPARATOR + delCol;
					// TreeMap<Long, KeyValue> timestampMap = colMap.get(colKey);
					// if (timestampMap == null) {
					// // some one deleted the entire column, or probably there was never
					// a
					// // column.
					// // Push this delete to the HRegion, so that the delete's effects
					// are
					// // visible everywhere.
					// retVal = retVal && false;
					// break;
					// }
					//
					// timestampMap.clear();
					// long latestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
					// timestampMap.put(latestTimestamp, kv);
					break;
				}
				}
			}
		}

		return retVal;
	}

	static void updateLatestTimestamp(
			final Collection<List<KeyValue>> kvsCollection, final long time) {
		byte[] timeBytes = Bytes.toBytes(time);
		// HAVE to manually set the KV timestamps
		for (List<KeyValue> kvs : kvsCollection) {
			for (KeyValue kv : kvs) {
				if (kv.isLatestTimestamp()) {
					kv.updateLatestStamp(timeBytes);
				}
			}
		}
	}

	public List<KeyValue> getAllKVsForASingleGet(final Scan scan) {
		List<KeyValue> kvList = new ArrayList<KeyValue>();
		if (myInMemStore.isEmpty())
			return kvList;

		String startRow = Bytes.toString(scan.getStartRow());

		Map<byte[], NavigableSet<byte[]>> scanFamMap = scan.getFamilyMap();
		for (Map.Entry<byte[], NavigableSet<byte[]>> scanFamMapEntry : scanFamMap
				.entrySet()) {
			String famStr = Bytes.toString(scanFamMapEntry.getKey());

			// If the scan had any particular columns mentioned.
			if (scanFamMapEntry.getValue() != null
					&& scanFamMapEntry.getValue().size() > 0) {
				long startTimestamp = scan.getTimeRange().getMin();
				long stopTimestamp = scan.getTimeRange().getMax();
				boolean takeAllVersions = (stopTimestamp == Long.MAX_VALUE)
						&& (startTimestamp == 0L);
				int maxVersions = scan.getMaxVersions();

				for (byte[] col : scanFamMapEntry.getValue()) {
					if (takeAllVersions) {
						// myInMemStore will always contain only one version. By default the
						// version number will be 1.
						sysout("TAKING ALL VERSIONS!");
						long defaultTimestamp = 1;
						StringBuffer strBuf = new StringBuffer();
						strBuf.append(startRow).append(SEPARATOR).append(famStr).append(SEPARATOR);
						strBuf.append(Bytes.toString(col)).append(SEPARATOR).append(defaultTimestamp);
						
						KeyValue kv = myInMemStore.get(strBuf.toString());
						if (kv != null) {
							if (debug)
								sysout("Adding to kvList, timestamp: " + defaultTimestamp + ", kv: "
										+ kv.toString());
							
							kvList.add(kv);
						}
					} else {// Go with the timestamp range.
						for (long i = startTimestamp; i < stopTimestamp
								&& i < maxVersions + startTimestamp; i++) {
							StringBuffer strBuf = new StringBuffer();
							strBuf.append(startRow).append(SEPARATOR).append(famStr).append(SEPARATOR);
							strBuf.append(Bytes.toString(col)).append(SEPARATOR).append(i);
							
							KeyValue kv = myInMemStore.get(strBuf.toString());
							if (kv != null) {
								if (debug)
									sysout("Adding to kvList, timestamp: " + i + ", kv: "
										+ kv.toString());
								
								kvList.add(kv);
							}
						}
					}
				}
				// If no columns are mentioned, we need to grab all columns.
			} else {
				// BIG TODO: We need to iterate over the keys in myInMemState and choose
				// keys starting
				// with the given family. We don't need this case as of now, so
				// ignoring.
				// long startTimestamp = scan.getTimeRange().getMin();
				// long stopTimestamp = scan.getTimeRange().getMax();
				// boolean takeAllVersions = (stopTimestamp == Long.MAX_VALUE)
				// && (startTimestamp == 0L);
				//
				// int maxVersions = scan.getMaxVersions();
				//
				// for (ConcurrentNavigableMap<Long, KeyValue> timestampVal : colVal
				// .values()) {
				// // If there is no timestamp range limit, we go with maxVersions.
				// if (takeAllVersions) {
				// NavigableMap<Long, KeyValue> descendingMap = timestampVal
				// .descendingMap();
				// int count = 0;
				// for (Map.Entry<Long, KeyValue> timestampEntry : descendingMap
				// .entrySet()) {
				// kvList.add(timestampEntry.getValue());
				// sysout("Adding to kvList, timestamp: " + timestampEntry.getKey()
				// + ", kv: " + timestampEntry.getValue().toString());
				//
				// count++;
				// if (count >= maxVersions)
				// break;
				// }
				// } else {
				// // Go with the timestamp range.
				// for (long i = startTimestamp; i < stopTimestamp; i++) {
				// KeyValue kv = timestampVal.get(i);
				// if (kv == null) {
				// sysout("No kv for timestamp: " + i);
				// continue;
				// }
				//
				// sysout("Adding to kvList, timestamp: " + i + ", kv: "
				// + kv.toString());
				// kvList.add(kv);
				// }
				// }
				// }
			}
		}
		return kvList;
	}
}