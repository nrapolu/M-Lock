package ham.wal.regionobserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class CumulativeInMemoryState {

	private static final Log LOG = LogFactory
			.getLog(CumulativeInMemoryState.class);
	private final HRegionInfo regionInfo;

	private ConcurrentNavigableMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>>>> myInMemStore = new ConcurrentSkipListMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>>>>();

	private List<Delete> deletes = new LinkedList<Delete>();

	private void sysout(String line) {
		//System.out.println(line);
	}
	
	public CumulativeInMemoryState(HRegionInfo regionInfo) {
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

	public Put[] getAllUpdates() throws IOException {
		// Since we store the entire KeyValue in the myInMemStore, and since we only
		// store those
		// KeyValues which changed due to updates, we can scan the entire store,
		// stack these Puts
		// and send them over to HRegion for flushing to disk.
		List<Put> puts = new LinkedList<Put>();
		for (Map.Entry<String, ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>>>> rowEntry : myInMemStore
				.entrySet()) {
			Put p = new Put(Bytes.toBytes(rowEntry.getKey()));
			for (ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>> colVal : rowEntry
					.getValue().values()) {
				for (ConcurrentNavigableMap<Long, KeyValue> timestampVal : colVal
						.values()) {
					for (Map.Entry<Long, KeyValue> kvEntry : timestampVal.entrySet()) {
						KeyValue kv = kvEntry.getValue();
						if (KeyValue.Type.codeToType(kv.getType()) != KeyValue.Type.Put) {
							addToDeletes(kvEntry.getKey(), kv);
							continue;
						}

						p.add(kv);
					}
				}
			}
			puts.add(p);
		}
		myInMemStore.clear();
		return puts.toArray(new Put[0]);
	}

	public void addPut(final Put write) {
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
				ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>>> famMap = myInMemStore
						.get(row);
				if (famMap == null) {
					famMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>>>();
					myInMemStore.put(row, famMap);
				}

				ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>> colMap = famMap
						.get(familyStr);
				if (colMap == null) {
					colMap = new ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>>();
					famMap.put(familyStr, colMap);
				}

				ConcurrentNavigableMap<Long, KeyValue> timestampMap = colMap
						.get(colStr);
				if (timestampMap == null) {
					timestampMap = new ConcurrentSkipListMap<Long, KeyValue>();
					colMap.put(colStr, timestampMap);
				}
				sysout("InMemStore##: Putting at timestamp: " + kv.getTimestamp()
						+ ", is this kv: " + kv.toString());
				timestampMap.put(kv.getTimestamp(), kv);
			}
		}
	}

	boolean addDelete(final Delete delete) {
		String row = Bytes.toString(delete.getRow());
		ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>>> famMap = myInMemStore
				.get(row);
		if (famMap == null)
			return false;

		boolean retVal = true;
		Map<byte[], List<KeyValue>> delFamilyMap = delete.getFamilyMap();
		for (byte[] family : delFamilyMap.keySet()) {
			String familyStr = Bytes.toString(family);
			List<KeyValue> kvList = delFamilyMap.get(family);
			if (kvList == null)
				continue;

			ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>> colMap = famMap
					.get(familyStr);
			if (colMap == null) {
				// Push this delete to the HRegion, so that the delete's effects are
				// visible everywhere.
				retVal = retVal && false;
				continue;
			}
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
					ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>> localColMap = famMap
							.get(familyStr);
					for (ConcurrentNavigableMap<Long, KeyValue> timestampMap : localColMap
							.values()) {
						timestampMap.clear();
						long latestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
						timestampMap.put(latestTimestamp, kv);
					}
					break;
				}
				case Delete: {
					// Deleting a specific column for a specific timestamp.
					String delCol = Bytes.toString(kv.getQualifier());
					Long timestamp = kv.getTimestamp();
					ConcurrentNavigableMap<Long, KeyValue> timestampMap = colMap
							.get(delCol);
					if (timestampMap == null) {
						// some one deleted the entire column, or probably there was never a
						// column.
						retVal = retVal && false;
						break;
					}

					if (timestamp == HConstants.LATEST_TIMESTAMP) {
						Map.Entry<Long, KeyValue> delKVEntry = timestampMap.pollLastEntry();
						timestamp = delKVEntry.getKey();
					} else {
						timestampMap.remove(timestamp);
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
					timestampMap.put(timestamp, kv);

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
					String delCol = Bytes.toString(kv.getQualifier());
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
					colMap.get(delCol).clear();
					long latestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
					colMap.get(delCol).put(latestTimestamp, kv);
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

	/*
	 * void addDelete(final Delete delete) { long now =
	 * EnvironmentEdgeManager.currentTimeMillis();
	 * updateLatestTimestamp(delete.getFamilyMap().values(), now); if
	 * (delete.getTimeStamp() == HConstants.LATEST_TIMESTAMP) {
	 * delete.setTimestamp(now); } deletes.add(delete); writeOrdering.add(new
	 * WriteAction(delete)); }
	 */
	/*
	 * void applyDeletes(final List<KeyValue> input, final long minTime, final
	 * long maxTime) { if (deletes.isEmpty()) { return; } for (Iterator<KeyValue>
	 * itr = input.iterator(); itr.hasNext();) { KeyValue included =
	 * applyDeletes(itr.next(), minTime, maxTime); if (null == included) {
	 * itr.remove(); } } }
	 * 
	 * KeyValue applyDeletes(final KeyValue kv, final long minTime, final long
	 * maxTime) { if (deletes.isEmpty()) { return kv; }
	 * 
	 * for (Delete delete : deletes) { // Skip if delete should not apply if
	 * (!Bytes.equals(kv.getRow(), delete.getRow()) || kv.getTimestamp() >
	 * delete.getTimeStamp() || delete.getTimeStamp() > maxTime ||
	 * delete.getTimeStamp() < minTime) { continue; }
	 * 
	 * // Whole-row delete if (delete.isEmpty()) { return null; }
	 * 
	 * for (Entry<byte[], List<KeyValue>> deleteEntry : delete.getFamilyMap()
	 * .entrySet()) { byte[] family = deleteEntry.getKey(); if
	 * (!Bytes.equals(kv.getFamily(), family)) { continue; } List<KeyValue>
	 * familyDeletes = deleteEntry.getValue(); if (familyDeletes == null) { return
	 * null; } for (KeyValue keyDeletes : familyDeletes) { byte[] deleteQualifier
	 * = keyDeletes.getQualifier(); byte[] kvQualifier = kv.getQualifier(); if
	 * (keyDeletes.getTimestamp() > kv.getTimestamp() &&
	 * Bytes.equals(deleteQualifier, kvQualifier)) { return null; } } } }
	 * 
	 * return kv; }
	 */

	public List<KeyValue> getAllKVsForASingleGet(final Scan scan) {
		List<KeyValue> kvList = new ArrayList<KeyValue>();
		if (myInMemStore.isEmpty())
			return kvList;

		String startRow = Bytes.toString(scan.getStartRow());
		ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>>> famVal = myInMemStore
				.get(startRow);

		if(famVal == null)
			return kvList;
		
		Map<byte[], NavigableSet<byte[]>> scanFamMap = scan.getFamilyMap();
		for (Map.Entry<byte[], NavigableSet<byte[]>> scanFamMapEntry : scanFamMap
				.entrySet()) {
			String famStr = Bytes.toString(scanFamMapEntry.getKey());
			ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>> colVal = famVal
					.get(famStr);
			if (colVal == null)
				// The In-Memory store does not have this family -- probably it has a
				// static state.
				continue;

			// If the scan had any particular columns mentioned.
			if (scanFamMapEntry.getValue() != null
					&& scanFamMapEntry.getValue().size() > 0) {
				long startTimestamp = scan.getTimeRange().getMin();
				long stopTimestamp = scan.getTimeRange().getMax();
				boolean takeAllVersions = (stopTimestamp == Long.MAX_VALUE)
				 && (startTimestamp == 0L);
				int maxVersions = scan.getMaxVersions();

				for (byte[] col : scanFamMapEntry.getValue()) {
					ConcurrentNavigableMap<Long, KeyValue> timestampVal = colVal
							.get(Bytes.toString(col));
					if (timestampVal == null || timestampVal.isEmpty())
						continue;

					// If there is no timestamp range limit, we go with maxVersions.
					if (takeAllVersions) {
						ConcurrentNavigableMap<Long, KeyValue> descendingMap = timestampVal
								.descendingMap();
						int count = 0;
						for (Map.Entry<Long, KeyValue> timestampEntry : descendingMap
								.entrySet()) {
							kvList.add(timestampEntry.getValue());
							sysout("Adding to kvList, timestamp: "
									+ timestampEntry.getKey() + ", kv: "
									+ timestampEntry.getValue().toString());

							count++;
							if (count >= maxVersions)
								break;
						}
					} else {
						// Go with the timestamp range.
						for (long i = startTimestamp; i < stopTimestamp; i++) {
							KeyValue kv = timestampVal.get(i);
							sysout("Adding to kvList, timestamp: " + i + ", kv: "
									+ kv.toString());
							kvList.add(kv);
						}						
					}
				}
				// If no columns are mentioned, we need to grab all columns.
			} else {
				long startTimestamp = scan.getTimeRange().getMin();
				long stopTimestamp = scan.getTimeRange().getMax();
				boolean takeAllVersions = (stopTimestamp == Long.MAX_VALUE)
					&& (startTimestamp == 0L);

				int maxVersions = scan.getMaxVersions();

				for (ConcurrentNavigableMap<Long, KeyValue> timestampVal : colVal
						.values()) {
					// If there is no timestamp range limit, we go with maxVersions.
					if (takeAllVersions) {
						NavigableMap<Long, KeyValue> descendingMap = timestampVal
								.descendingMap();
						int count = 0;
						for (Map.Entry<Long, KeyValue> timestampEntry : descendingMap
								.entrySet()) {
							kvList.add(timestampEntry.getValue());
							sysout("Adding to kvList, timestamp: "
									+ timestampEntry.getKey() + ", kv: "
									+ timestampEntry.getValue().toString());

							count++;
							if (count >= maxVersions)
								break;
						}
					} else {
						// Go with the timestamp range.
						for (long i = startTimestamp; i < stopTimestamp; i++) {
							KeyValue kv = timestampVal.get(i);
							if (kv == null) {
								sysout("No kv for timestamp: " + i);
								continue;
							}

							sysout("Adding to kvList, timestamp: " + i + ", kv: "
									+ kv.toString());
							kvList.add(kv);
						}
					}
				}
			}
		}
		return kvList;
	}

	private KeyValue[] getAllKVs(final Scan scan) {
		List<KeyValue> kvList = new ArrayList<KeyValue>();
		if (myInMemStore.isEmpty())
			return kvList.toArray(new KeyValue[0]);

		String startRow = Bytes.toString(scan.getStartRow());
		String stopRow = Bytes.toString(scan.getStopRow());
		if (Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW))
			startRow = myInMemStore.firstKey();

		if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW))
			stopRow = myInMemStore.lastKey();

		ConcurrentNavigableMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>>>> subMap = myInMemStore
				.subMap(startRow, true, stopRow, true);
		for (ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>>> famVal : subMap
				.values()) {

			Map<byte[], NavigableSet<byte[]>> scanFamMap = scan.getFamilyMap();

			for (Map.Entry<byte[], NavigableSet<byte[]>> scanFamMapEntry : scanFamMap
					.entrySet()) {

				String famStr = Bytes.toString(scanFamMapEntry.getKey());
				ConcurrentHashMap<String, ConcurrentNavigableMap<Long, KeyValue>> colVal = famVal
						.get(famStr);
				if (colVal == null)
					// The In-Memory store does not have this family -- probably it has a
					// static state.
					continue;

				// If the scan had any particular columns mentioned.
				if (scanFamMapEntry.getValue() != null
						&& scanFamMapEntry.getValue().size() > 0) {
					long startTimestamp = scan.getTimeRange().getMin();
					long stopTimestamp = scan.getTimeRange().getMax();
					boolean allTrue = true;
					// boolean allTrue = (stopTimestamp == Long.MAX_VALUE)
					// && (startTimestamp == 0);
					int maxVersions = scan.getMaxVersions();

					for (byte[] col : scanFamMapEntry.getValue()) {
						ConcurrentNavigableMap<Long, KeyValue> timestampVal = colVal
								.get(Bytes.toString(col));
						if (timestampVal == null || timestampVal.isEmpty())
							continue;

						// If there is no timestamp range limit, we go with maxVersions.
						if (allTrue) {
							NavigableMap<Long, KeyValue> descendingMap = timestampVal
									.descendingMap();
							int count = 0;
							for (Map.Entry<Long, KeyValue> timestampEntry : descendingMap
									.entrySet()) {
								kvList.add(timestampEntry.getValue());
								sysout("Adding to kvList, timestamp: "
										+ timestampEntry.getKey() + ", kv: "
										+ timestampEntry.getValue().toString());

								count++;
								if (count >= maxVersions)
									break;
							}
						} else {
							// Go with the timestamp range.
							for (long i = startTimestamp; i < stopTimestamp; i++) {
								KeyValue kv = timestampVal.get(i);
								sysout("Adding to kvList, timestamp: " + i + ", kv: "
										+ kv.toString());
								kvList.add(kv);
							}
						}
					}
					// If no columns are mentioned, we need to grab all columns.
				} else {
					long startTimestamp = scan.getTimeRange().getMin();
					long stopTimestamp = scan.getTimeRange().getMax();
					// boolean allTrue = (stopTimestamp == Long.MAX_VALUE)
					// && (startTimestamp == 0);
					boolean allTrue = true;
					int maxVersions = scan.getMaxVersions();

					for (ConcurrentNavigableMap<Long, KeyValue> timestampVal : colVal
							.values()) {
						// If there is no timestamp range limit, we go with maxVersions.
						if (allTrue) {
							NavigableMap<Long, KeyValue> descendingMap = timestampVal
									.descendingMap();
							int count = 0;
							for (Map.Entry<Long, KeyValue> timestampEntry : descendingMap
									.entrySet()) {
								kvList.add(timestampEntry.getValue());
								sysout("Adding to kvList, timestamp: "
										+ timestampEntry.getKey() + ", kv: "
										+ timestampEntry.getValue().toString());

								count++;
								if (count >= maxVersions)
									break;
							}
						} else {
							// Go with the timestamp range.
							for (long i = startTimestamp; i < stopTimestamp; i++) {
								KeyValue kv = timestampVal.get(i);
								if (kv == null) {
									sysout("No kv for timestamp: " + i);
									continue;
								}

								sysout("Adding to kvList, timestamp: " + i + ", kv: "
										+ kv.toString());
								kvList.add(kv);
							}
						}
					}
				}
			}
		}
		return kvList.toArray(new KeyValue[0]);
	}

	/*
	 * private KeyValue[] getAllKVs(final Scan scan) { List<KeyValue> kvList = new
	 * ArrayList<KeyValue>();
	 * 
	 * for (WriteAction action : writeOrdering) { byte[] row = action.getRow();
	 * List<KeyValue> kvs = action.getKeyValues();
	 * 
	 * if (scan.getStartRow() != null && !Bytes.equals(scan.getStartRow(),
	 * HConstants.EMPTY_START_ROW) && Bytes.compareTo(row, scan.getStartRow()) <
	 * 0) { continue; }
	 * 
	 * if (scan.getStopRow() != null && !Bytes.equals(scan.getStopRow(),
	 * HConstants.EMPTY_END_ROW) && Bytes.compareTo(row, scan.getStopRow()) > 0) {
	 * continue; }
	 * 
	 * kvList.addAll(kvs); }
	 * 
	 * return kvList.toArray(new KeyValue[kvList.size()]); }
	 */

	/**
	 * Get a scanner to go through the puts and deletes from this transaction.
	 * Used to weave together the local trx puts with the global state.
	 * 
	 * @return scanner
	 */
	KeyValueScanner getScanner(final Scan scan) {
		return new InMemoryStoreScanner(scan, this);
	}

	/**
	 * Scanner of the puts and deletes that occur during this transaction.
	 * 
	 * @author clint.morgan
	 */
	public static class InMemoryStoreScanner extends KeyValueListScanner
			implements InternalScanner {
		private ScanQueryMatcher matcher;

		InMemoryStoreScanner(final Scan scan, CumulativeInMemoryState inMemState) {
			super(KeyValue.COMPARATOR, inMemState.getAllKVs(scan));

			// We want transaction scanner to always take priority over store
			// scanners. Further, the in-memory store should take priority over the
			// on-disk stores.
			// Since transaction scanner will have the ID Long.MAX_VALUE, we can give
			// this the priority
			// Long.MAX_VALUE - 1.
			setSequenceID(Long.MAX_VALUE);

			// Store.ScanInfo scanInfo = new Store.ScanInfo(arg0, arg1, arg2, arg3,
			// arg4, arg5, arg6)
			//matcher = new ScanQueryMatcher(scan, null, null, HConstants.FOREVER,
			//		KeyValue.KEY_COMPARATOR, scan.getMaxVersions());
		}

		InMemoryStoreScanner(final Scan scan, List<KeyValue> kvs, long sequenceId) {
			super(KeyValue.COMPARATOR, kvs.toArray(new KeyValue[kvs.size()]));
			setSequenceID(sequenceId);
			//matcher = new ScanQueryMatcher(scan, null, null, HConstants.FOREVER,
			//		KeyValue.KEY_COMPARATOR, scan.getMaxVersions());
		}

		/**
		 * Get the next row of values from this transaction.
		 * 
		 * @param outResult
		 * @param limit
		 * @return true if there are more rows, false if scanner is done
		 */
		@Override
		public synchronized boolean next(final List<KeyValue> outResult,
				final int limit) throws IOException {
			KeyValue peeked = this.peek();
			if (peeked == null) {
				close();
				return false;
			}
			matcher.setRow(peeked.getRow());
			KeyValue kv;
			List<KeyValue> results = new ArrayList<KeyValue>();
			LOOP: while ((kv = this.peek()) != null) {
				ScanQueryMatcher.MatchCode qcode = matcher.match(kv);
				switch (qcode) {
				case INCLUDE:
					KeyValue next = this.next();
					results.add(next);
					if (limit > 0 && results.size() == limit) {
						break LOOP;
					}
					continue;

				case DONE:
					// copy jazz
					outResult.addAll(results);
					return true;

				case DONE_SCAN:
					close();

					// copy jazz
					outResult.addAll(results);

					return false;

				case SEEK_NEXT_ROW:
					this.next();
					break;

				case SEEK_NEXT_COL:
					this.next();
					break;

				case SKIP:
					this.next();
					break;

				default:
					throw new RuntimeException("UNEXPECTED");
				}
			}

			if (!results.isEmpty()) {
				// copy jazz
				outResult.addAll(results);
				return true;
			}

			// No more keys
			close();
			return false;
		}

		@Override
		public boolean next(final List<KeyValue> results) throws IOException {
			return next(results, -1);
		}
	}
}
