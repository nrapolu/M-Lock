package ham.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class WALManagerDistTxnClient extends WALManagerClient {
	private static final Log LOG = LogFactory
			.getLog(WALManagerDistTxnClient.class);
	private HBaseBackedTransactionLogger txnLogger = null;

	byte[] dataFamily = WALTableProperties.dataFamily;
	byte[] dataColumn = WALTableProperties.dataColumn;
	byte[] versionColumn = WALTableProperties.versionColumn;
	byte[] writeLockColumn = WALTableProperties.writeLockColumn;
	byte[] walTableName = WALTableProperties.walTableName;
	byte[] logFamily = WALTableProperties.WAL_FAMILY;
	long appTimestamp = WALTableProperties.appTimestamp;

	ExecutorService pool = Executors.newCachedThreadPool();
	List<Future> futures = new LinkedList<Future>();
	long startTimeForMigration;

	public WALManagerDistTxnClient() throws IOException {
		this.txnLogger = new HBaseBackedTransactionLogger();
	}

	public static void sysout(long trxId, String otp) {
		System.out.println(trxId + " : " + otp);
	}

	public DistTxnState beginTransaction() throws IOException {
		long transactionId = txnLogger.createNewTransactionLog();
		// LOG.debug("Begining transaction " + transactionId);
		return new DistTxnState(transactionId);
	}

	public Result get(final HTable table, final DistTxnState transactionState,
			final Get get) throws IOException {
		// First we check if the object to be read is already present in cache. If
		// so, we
		// just return that object.
		// TODO: Here we are just checking in the read-cache. Ideally, we'll have to
		// maintain
		// a write cache and check in it too.
		ImmutableBytesWritable key = new ImmutableBytesWritable(get.getRow());
		Result result = transactionState.getFromReadCache(key);
		if (result != null)
			return result;

		// Otherwise, do a submarine read. The read could be stale. The snapshot at
		// the WAL
		// might contain the latest value of the key. We take a chance and read it
		// directly
		// from the store.
		result = table.get(get);
		// Store the version number of the read in DistTxnMetadata's
		// getWithVersionList
		long version = WALTableProperties.getVersion(result);
		transactionState.addReadInfoToDistTxnMetadata(key, version);
		return result;
	}

	public List<Result> get(final HTable logTable, final HTable dataTable,
			final DistTxnState transactionState, final List<Get> gets,
			boolean shouldMigrateLocks) throws Throwable {
		long trxId = transactionState.getTransactionId();
		// First we check if the object to be read is already present in cache. If
		// so, we
		// just return that object. However, only dumb applications would do a Get
		// for a row that they've already read or written. Fortunately, we don't
		// write
		// dumb applications.
		// From the List<Get>, we find out the logIds they correspond to and form
		// their
		// list.
		TreeMap<LogId, List<Get>> logIdToGetMap = new TreeMap<LogId, List<Get>>(
				new DistTxnState.LogIdComparator());
		// We assume that in the list of Get requests sent to this function, no two
		// of them
		// are for the same row.
		HashMap<ImmutableBytesWritable, Integer> getRowToIndexMap = new HashMap<ImmutableBytesWritable, Integer>();
		// The list to be sent to LockMigrator
		List<byte[]> lockSet = new LinkedList<byte[]>();
		for (int index = 0; index < gets.size(); index++) {
			Get g = gets.get(index);
			LogId logId = WALTableProperties.getLogIdForKey(g.getRow());
			sysout(trxId, "For row: " + Bytes.toString(g.getRow()) + ", logId is: "
					+ logId.toString());
			List<Get> getList = logIdToGetMap.get(logId);
			if (getList == null) {
				getList = new LinkedList<Get>();
				logIdToGetMap.put(logId, getList);
			}
			getList.add(g);
			lockSet.add(g.getRow());
			getRowToIndexMap.put(new ImmutableBytesWritable(g.getRow()), index);
		}

		// Initiate LockMigrator and put it in the pool.
		// TODO: We should make this a global variable, as it will be used several
		// times.
		// Also, we should initiate this client with some random unique id. Send
		// that id
		// as the key for the destination LogId where the keys should be placed.
		if (shouldMigrateLocks) {
			LockMigrator lockMigrator = new LockMigrator(logTable.getConfiguration(),
					transactionState.getTransactionId());
			lockMigrator.addLockSetToMigrate(lockSet);
			Future future = pool.submit(lockMigrator);
			futures.add(future);
			startTimeForMigration = System.currentTimeMillis();
		}

		final List<LogId> logIdList = new LinkedList<LogId>();
		for (Map.Entry<LogId, List<Get>> entry : logIdToGetMap.entrySet()) {
			logIdList.add(entry.getKey());
		}

		class GetSnapshotCallBack implements Batch.Callback<List<Snapshot>> {
			private List<Snapshot> snapshots;

			List<Snapshot> getSnapshots() {
				return snapshots;
			}

			@Override
			public synchronized void update(byte[] region, byte[] row,
					List<Snapshot> result) {
				// This call goes to multiple regions. Each returns the list of same
				// size (equal to the size of logIdList argument. However, only some of
				// the list elements will have values (others would be "null").
				// We collect the non-null elements and place them in this class's
				// "snapshots" list at the same position.
				if (this.snapshots == null) {
					this.snapshots = new ArrayList<Snapshot>(result.size());
					for (int i = 0; i < result.size(); i++)
						snapshots.add(i, null);
				}

				for (int i = 0; i < result.size(); i++) {
					Snapshot snapshot = result.get(i);
					if (snapshot != null && snapshot.getTimestamp() >= 0) {
						this.snapshots.set(i, snapshot);
						// System.out.println("In GetSnapshotCallBack, at index: " + i +
						// " adding snapshot: "
						// + snapshot.toString());
					}
				}
			}
		}

		LogId firstLogId = logIdToGetMap.firstKey();
		// System.out.println("First logId: " + firstLogId.toString());
		LogId lastLogId = logIdToGetMap.lastKey();
		// System.out.println("Last logId: " + lastLogId.toString());
		GetSnapshotCallBack aGetSnapshotCallBack = new GetSnapshotCallBack();
		logTable.coprocessorExec(WALManagerDistTxnProtocol.class, firstLogId
				.getKey(), lastLogId.getKey(),
				new Batch.Call<WALManagerDistTxnProtocol, List<Snapshot>>() {
					@Override
					public List<Snapshot> call(WALManagerDistTxnProtocol instance)
							throws IOException {
						// We don't need to send the actual list of Gets. The function can
						// return the entire Snapshot for the logId, and then we can sieve
						// through
						// them later to obtain results for our gets.
						return instance.getSnapshotsForLogIds(logIdList);
					}
				}, aGetSnapshotCallBack);

		// From the snapshots obtained, check which gets can be satisfied. For the
		// rest, we'll
		// have to go to the store to fetch their results. Even inside a single Get,
		// there could
		// be multiple columns being requested. FOr these, we'll have to
		// individually search in
		// the snapshot, as they would be present as different Write objects.
		List<Result> results = new ArrayList<Result>(gets.size());
		List<List<KeyValue>> resultKvs = new ArrayList<List<KeyValue>>();
		// Fill the results with nulls as the placement into it will be random.
		sysout(trxId, "Gets size: " + gets.size());
		for (Get g : gets) {
			sysout(trxId, "About to fetch Get: " + g.toString());
		}

		for (int i = 0; i < gets.size(); i++)
			results.add(i, null);
		for (int i = 0; i < gets.size(); i++)
			resultKvs.add(i, null);

		List<Get> toBeFetchedFromStore = new ArrayList<Get>();
		List<Snapshot> snapshots = aGetSnapshotCallBack.getSnapshots();

		for (int logIdIndex = 0; logIdIndex < logIdList.size(); logIdIndex++) {
			LogId logId = logIdList.get(logIdIndex);
			List<Get> getList = logIdToGetMap.get(logId);
			Snapshot snapshot = snapshots.get(logIdIndex);
			Map<String, Write> snapshotWriteMap = snapshot.getWriteMap();
			// For now, we only consider the Get to be requesting dataName
			// (dataColumn),
			// versionName (versionColumn), and writeLockName (writeLockColumn).
			for (Get g : getList) {
				// Get the index of this Get
				int getIndex = getRowToIndexMap.get(new ImmutableBytesWritable(g
						.getRow()));
				// Temporary Get which is needed to fetch stuff from store, if all
				// requests
				// are not satisfied by snapshots.
				Get getFromStore = new Get(g.getRow());
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
						// We have to fetch from the dataastore. Here we just collect them.
						// They can
						// be obtained in a batch.
						sysout(trxId, "For column: " + Bytes.toString(column)
								+ ", to be fetched from store: " + g.toString());
						getFromStore.addColumn(dataFamily, column);
					}
				}
				// Push the kvs for this Get into the resultKvs. This Get request
				// might not
				// have been satisfied through the snapshot. Thus, we wait until all
				// the kvs have
				// been fetched and then create the Result object eventually.
				resultKvs.set(getIndex, kvs);

				if (getFromStore.getFamilyMap() != null
						&& !getFromStore.getFamilyMap().isEmpty())
					toBeFetchedFromStore.add(getFromStore);
			}
		}

		if (!toBeFetchedFromStore.isEmpty()) {
			Object[] storeResults = dataTable.batch(toBeFetchedFromStore);
			// For each storeResult, we find out the index of the original get and
			// place
			// the
			// result in the appropriate location in results array.
			for (int i = 0; i < storeResults.length; i++) {
				Object o = storeResults[i];
				Result r = (Result) o;
				if (!r.isEmpty()) {
					int getIndex = getRowToIndexMap.get(new ImmutableBytesWritable(r
							.getRow()));
					List<KeyValue> kvs = resultKvs.get(getIndex);
					kvs.addAll(r.list());
				} else {
					sysout(trxId, "From store result is empty for Get: "
							+ toBeFetchedFromStore.get(i));
				}
			}
		}

		// Now that resultKvs has been filled up, create individual Result objects
		// with
		// its corresponding kv list.
		for (int i = 0; i < gets.size(); i++) {
			List<KeyValue> kvs = resultKvs.get(i);
			if (kvs == null || kvs.isEmpty())
				sysout(trxId, "Empty kvs for row: " + gets.get(i).toString());
			Result finalResult = new Result(kvs);
			results.set(i, finalResult);
		}

		// For all results, store the version number of the read result in
		// DistTxnMetadata's
		// getWithVersionList
		for (int i = 0; i < results.size(); i++) {
			Result r = results.get(i);
			// System.out.println("Examining result at index: " + i);
			long version = 0;
			try {
				version = WALTableProperties.getVersion(r);
			} catch (Exception e) {
				sysout(trxId, "Exception for row: " + Bytes.toString(r.getRow()));
				e.printStackTrace();
			}
			transactionState.addReadInfoToDistTxnMetadata(new ImmutableBytesWritable(
					r.getRow()), version);
		}
		return results;
	}

	public List<Result> getInParallel(final HTable logTable,
			final HTable dataTable, final DistTxnState transactionState,
			final List<Get> gets, boolean shouldMigrateLocks) throws Throwable {
		long trxId = transactionState.getTransactionId();
		// First we check if the object to be read is already present in cache. If
		// so, we
		// just return that object. However, only dumb applications would do a Get
		// for a row that they've already read or written. Fortunately, we don't
		// write
		// dumb applications.
		// From the List<Get>, we find out the logIds they correspond to and form
		// their
		// list.
		TreeMap<LogId, List<Get>> logIdToGetMap = new TreeMap<LogId, List<Get>>(
				new DistTxnState.LogIdComparator());
		// We assume that in the list of Get requests sent to this function, no two
		// of them
		// are for the same row.
		HashMap<ImmutableBytesWritable, Integer> getRowToIndexMap = new HashMap<ImmutableBytesWritable, Integer>();
		// The list to be sent to LockMigrator
		List<byte[]> lockSet = new LinkedList<byte[]>();
		for (int index = 0; index < gets.size(); index++) {
			Get g = gets.get(index);
			LogId logId = WALTableProperties.getLogIdForKey(g.getRow());
			sysout(trxId, "For row: " + Bytes.toString(g.getRow()) + ", logId is: "
					+ logId.toString());
			List<Get> getList = logIdToGetMap.get(logId);
			if (getList == null) {
				getList = new LinkedList<Get>();
				logIdToGetMap.put(logId, getList);
			}
			getList.add(g);
			lockSet.add(g.getRow());
			getRowToIndexMap.put(new ImmutableBytesWritable(g.getRow()), index);
		}

		// Initiate LockMigrator and put it in the pool.
		// TODO: We should make this a global variable, as it will be used several
		// times.
		// Also, we should initiate this client with some random unique id. Send
		// that id
		// as the key for the destination LogId where the keys should be placed.
		if (shouldMigrateLocks) {
			LockMigrator lockMigrator = new LockMigrator(logTable.getConfiguration(),
					transactionState.getTransactionId());
			lockMigrator.addLockSetToMigrate(lockSet);
			Future future = pool.submit(lockMigrator);
			futures.add(future);
			startTimeForMigration = System.currentTimeMillis();
		}

		final List<LogId> logIdList = new LinkedList<LogId>();
		for (Map.Entry<LogId, List<Get>> entry : logIdToGetMap.entrySet()) {
			logIdList.add(entry.getKey());
		}

		class GetSnapshotCallBack implements Batch.Callback<List<Snapshot>> {
			private List<Snapshot> snapshots;

			List<Snapshot> getSnapshots() {
				return snapshots;
			}

			@Override
			public synchronized void update(byte[] region, byte[] row,
					List<Snapshot> result) {
				// This call goes to multiple regions. Each returns the list of same
				// size (equal to the size of logIdList argument. However, only some of
				// the list elements will have values (others would be "null").
				// We collect the non-null elements and place them in this class's
				// "snapshots" list at the same position.
				if (this.snapshots == null) {
					this.snapshots = new ArrayList<Snapshot>(result.size());
					for (int i = 0; i < result.size(); i++)
						snapshots.add(i, null);
				}

				for (int i = 0; i < result.size(); i++) {
					Snapshot snapshot = result.get(i);
					if (snapshot != null && snapshot.getTimestamp() >= 0) {
						this.snapshots.set(i, snapshot);
						// System.out.println("In GetSnapshotCallBack, at index: " + i +
						// " adding snapshot: "
						// + snapshot.toString());
					}
				}
			}
		}

		class TableGetsCall implements Callable<Object[]> {
			HTable localDataTable = null;
			List<Get> gets = null;

			TableGetsCall(HTable dataTable, List<Get> gets) {
				this.localDataTable = dataTable;
				this.gets = gets;
			}

			@Override
			public Object[] call() throws Exception {
				// TODO Auto-generated method stub
				return localDataTable.batch(gets);
			}
		}

		// We issue the Gets for all the keys from the table in another thread.
		// In the main thread, we get the snapshots from WALs. Later, these two are
		// merged
		// picking the one from snapshot wherever a common key is found.
		// Ideally, we'll have to push this merging business to the server-side.
		// After reading
		// the snapshot, the server-side get should fetch the values from table
		// (either present
		// on the local node, as is the case for entity-groups, or fetch from a
		// remote node, as
		// is the case for micro-shards.
		// For now, we do this parallel fetch (which has the correctness issue),
		// since we don't
		// flush our snapshots. Also, the merge function which we write can be
		// clearly copied to
		// the server side.
		TableGetsCall tableGets = new TableGetsCall(dataTable, gets);
		Future<Object[]> futureFromTable = pool.submit(tableGets);

		// Issue reads from snapshots (write-ahead-logs).
		LogId firstLogId = logIdToGetMap.firstKey();
		// System.out.println("First logId: " + firstLogId.toString());
		LogId lastLogId = logIdToGetMap.lastKey();
		// System.out.println("Last logId: " + lastLogId.toString());
		GetSnapshotCallBack aGetSnapshotCallBack = new GetSnapshotCallBack();
		logTable.coprocessorExec(WALManagerDistTxnProtocol.class, firstLogId
				.getKey(), lastLogId.getKey(),
				new Batch.Call<WALManagerDistTxnProtocol, List<Snapshot>>() {
					@Override
					public List<Snapshot> call(WALManagerDistTxnProtocol instance)
							throws IOException {
						// We don't need to send the actual list of Gets. The function can
						// return the entire Snapshot for the logId, and then we can sieve
						// through
						// them later to obtain results for our gets.
						return instance.getSnapshotsForLogIds(logIdList);
					}
				}, aGetSnapshotCallBack);

		// From the snapshots obtained, check which gets can be satisfied. For the
		// rest, we'll
		// have to go to the store to fetch their results. Even inside a single Get,
		// there could
		// be multiple columns being requested. FOr these, we'll have to
		// individually search in
		// the snapshot, as they would be present as different Write objects.
		List<Result> results = new ArrayList<Result>(gets.size());
		List<List<KeyValue>> resultKvs = new ArrayList<List<KeyValue>>();
		// Fill the results with nulls as the placement into it will be random.
		sysout(trxId, "Gets size: " + gets.size());
		for (Get g : gets) {
			sysout(trxId, "About to fetch Get: " + g.toString());
		}

		for (int i = 0; i < gets.size(); i++)
			results.add(i, null);
		for (int i = 0; i < gets.size(); i++)
			resultKvs.add(i, null);

		List<Snapshot> snapshots = aGetSnapshotCallBack.getSnapshots();
		Object[] resultsFromStore = futureFromTable.get();

		for (int logIdIndex = 0; logIdIndex < logIdList.size(); logIdIndex++) {
			LogId logId = logIdList.get(logIdIndex);
			List<Get> getList = logIdToGetMap.get(logId);
			Snapshot snapshot = snapshots.get(logIdIndex);
			Map<String, Write> snapshotWriteMap = snapshot.getWriteMap();
			// For now, we only consider the Get to be requesting dataName
			// (dataColumn),
			// versionName (versionColumn), and writeLockName (writeLockColumn).
			for (Get g : getList) {
				// Get the index of this Get
				int getIndex = getRowToIndexMap.get(new ImmutableBytesWritable(g
						.getRow()));
				Result resultFromStore = (Result) resultsFromStore[getIndex];
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
						kvs.addAll(resultFromStore.getColumn(dataFamily, column));
					}
				}
				// Push the kvs for this Get into the resultKvs. This Get request
				// might not
				// have been satisfied through the snapshot. Thus, we wait until all
				// the kvs have
				// been fetched and then create the Result object eventually.
				resultKvs.set(getIndex, kvs);
			}
		}

		// Now that resultKvs has been filled up, create individual Result objects
		// with
		// its corresponding kv list.
		for (int i = 0; i < gets.size(); i++) {
			List<KeyValue> kvs = resultKvs.get(i);
			if (kvs == null || kvs.isEmpty())
				sysout(trxId, "Empty kvs for row: " + gets.get(i).toString());
			Result finalResult = new Result(kvs);
			results.set(i, finalResult);
		}

		// For all results, store the version number of the read result in
		// DistTxnMetadata's
		// getWithVersionList
		for (int i = 0; i < results.size(); i++) {
			Result r = results.get(i);
			// System.out.println("Examining result at index: " + i);
			long version = 0;
			try {
				version = WALTableProperties.getVersion(r);
			} catch (Exception e) {
				sysout(trxId, "Exception for row: " + Bytes.toString(r.getRow()));
				e.printStackTrace();
			}
			transactionState.addReadInfoToDistTxnMetadata(new ImmutableBytesWritable(
					r.getRow()), version);
		}
		return results;
	}

	public List<Result> getWithServerSideMerge(final HTable logTable,
			final HTable dataTable, final DistTxnState transactionState,
			final List<Get> gets) throws Throwable {
		long trxId = transactionState.getTransactionId();
		// First we check if the object to be read is already present in cache. If
		// so, we
		// just return that object. However, only dumb applications would do a Get
		// for a row that they've already read or written. Fortunately, we don't
		// write
		// dumb applications.
		// From the List<Get>, we find out the logIds they correspond to and form
		// their
		// list.
		TreeMap<LogId, List<Get>> logIdToGetMap = new TreeMap<LogId, List<Get>>(
				new DistTxnState.LogIdComparator());
		// We assume that in the list of Get requests sent to this function, no two
		// of them
		// are for the same row.
		HashMap<ImmutableBytesWritable, Integer> getRowToIndexMap = new HashMap<ImmutableBytesWritable, Integer>();
		for (int index = 0; index < gets.size(); index++) {
			Get g = gets.get(index);
			LogId logId = WALTableProperties.getLogIdForKey(g.getRow());
			List<Get> getList = logIdToGetMap.get(logId);
			if (getList == null) {
				getList = new LinkedList<Get>();
				logIdToGetMap.put(logId, getList);
			}
			getList.add(g);
			getRowToIndexMap.put(new ImmutableBytesWritable(g.getRow()), index);
		}

		final List<LogId> logIdList = new LinkedList<LogId>();
		final List<List<Get>> getsList = new LinkedList<List<Get>>();
		for (Map.Entry<LogId, List<Get>> entry : logIdToGetMap.entrySet()) {
			logIdList.add(entry.getKey());
			getsList.add(entry.getValue());
		}

		class GetWithServerSideMergeCallBack implements
				Batch.Callback<List<List<Result>>> {
			private List<List<Result>> results;

			List<List<Result>> getResults() {
				return results;
			}

			@Override
			public synchronized void update(byte[] region, byte[] row,
					List<List<Result>> resultListList) {
				// This call goes to multiple regions. Each returns the list of same
				// size (equal to the size of logIdList argument. However, only some of
				// the list elements will have values (others would be "null").
				// We collect the non-null elements and place them in this class's
				// "snapshots" list at the same position.
				if (this.results == null) {
					this.results = new ArrayList<List<Result>>(resultListList.size());
					for (int i = 0; i < resultListList.size(); i++)
						results.add(i, null);
				}

				// sysout(transactionState.getTransactionId(),
				// "In GetWithServerSideMergeCallBack, for row: "
				// + Bytes.toString(row) + ", resultListList size is: "
				// + resultListList.size());

				for (int i = 0; i < resultListList.size(); i++) {
					List<Result> resultList = resultListList.get(i);
					if (resultList != null && !resultList.isEmpty()) {
						// sysout(transactionState.getTransactionId(),
						// "Adding resultList at index: " + i + ", which is of size: "
						// + resultList.size());
						this.results.set(i, resultList);
					}
				}
			}
		}

		// Issue reads from snapshots (write-ahead-logs).
		LogId firstLogId = logIdToGetMap.firstKey();
		// System.out.println("First logId: " + firstLogId.toString());
		LogId lastLogId = logIdToGetMap.lastKey();
		// System.out.println("Last logId: " + lastLogId.toString());
		GetWithServerSideMergeCallBack aGetWithServerSideMergeCallBack = new GetWithServerSideMergeCallBack();
		logTable.coprocessorExec(WALManagerDistTxnProtocol.class,
				HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
				new Batch.Call<WALManagerDistTxnProtocol, List<List<Result>>>() {
					@Override
					public List<List<Result>> call(WALManagerDistTxnProtocol instance)
							throws IOException {
						// We don't need to send the actual list of Gets. The function can
						// return the entire Snapshot for the logId, and then we can sieve
						// through
						// them later to obtain results for our gets.
						return instance.getAfterServerSideMerge(logIdList, getsList);
					}
				}, aGetWithServerSideMergeCallBack);

		List<List<Result>> resultListList = aGetWithServerSideMergeCallBack
				.getResults();
		List<Result> finalResults = new LinkedList<Result>();
		for (int k = 0; k < gets.size(); k++) {
			finalResults.add(k, null);
		}

		for (List<Result> resultList : resultListList) {
			if (resultList != null && !resultList.isEmpty()) {
				for (Result r : resultList) {
					int index = getRowToIndexMap.get(new ImmutableBytesWritable(r
							.getRow()));
					finalResults.set(index, r);
				}
			}
		}

		// For all results, store the version number of the read result in
		// DistTxnMetadata's
		// getWithVersionList
		for (int i = 0; i < finalResults.size(); i++) {
			Result r = finalResults.get(i);
			// sysout(transactionState.getTransactionId(), "Examining result : "
			// + r.toString());
			long version = 0;
			try {
				version = WALTableProperties.getVersion(r);
			} catch (Exception e) {
				sysout(trxId, "Exception for row: " + Bytes.toString(r.getRow()));
				e.printStackTrace();
			}
			transactionState.addReadInfoToDistTxnMetadata(new ImmutableBytesWritable(
					r.getRow()), version);
		}
		return finalResults;
	}

	public long stopForLockMigration() {
		long totalTimeForMigration = 0;
		// There should be only one future.
		assert (futures.size() == 1);
		for (Future f : futures) {
			try {
				f.get();
				long endTimeForMigration = System.currentTimeMillis();
				totalTimeForMigration = endTimeForMigration - startTimeForMigration;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return totalTimeForMigration;
	}

	public void put(final HTable table, final DistTxnState transactionState,
			final Put put) throws IOException {
		LogId logId = WALTableProperties.getLogIdForKey(put.getRow());
		HRegionLocation regionLocation = WALTableProperties
				.getRegionLocationForLogId(table, logId);
		transactionState
				.addToWriteBuffer(logId, regionLocation, new Action(put, 1));
		transactionState.addWriteInfoToDistTxnMetadata(put);
	}

	public void addPessimisticLocks(DistTxnState transactionState,
			List<byte[]> pessimisticLocks) {
		for (byte[] lock : pessimisticLocks) {
			LogId logId = WALTableProperties.getLogIdForKey(lock);
			transactionState.addToPessimisticLocksPerLogId(logId,
					new ImmutableBytesWritable(lock));
		}
	}

	public void putShadowObjects(final HTable logTable, final HTable dataTable,
			final DistTxnState transactionState, boolean shouldMigrateLocks,
			String preferredSalt) throws InterruptedException, IOException {
		long trxId = transactionState.getTransactionId();

		// The list to be sent to LockMigrator
		List<byte[]> lockSet = transactionState.getSimplePutKeyList();
		// Initiate LockMigrator and put it in the pool.
		// TODO: We should make this a global variable, as it will be used several
		// times.
		// Also, we should initiate this client with some random unique id. Send
		// that id
		// as the key for the destination LogId where the keys should be placed.
		if (shouldMigrateLocks) {
			sysout(trxId, "Placing request for migration");
			LockMigrator lockMigrator = new LockMigrator(logTable.getConfiguration(),
					transactionState.getTransactionId());
			lockMigrator.addLockSetToMigrate(lockSet);
			lockMigrator.addPreferredSaltForLockTablePartition(preferredSalt);
			Future future = pool.submit(lockMigrator);
			futures.add(future);
			startTimeForMigration = System.currentTimeMillis();
		}

		// TODO: Storing these shadow objects at the snapshot through local
		// transactions
		// might be helpful, as the entity group commit would do a read of the
		// object before
		// copying it onto the original object, storing it at the snapshot would
		// remove
		// a network read.
		List<Put> putShadowList = transactionState.getShadowPuts();

		// Start the putShadows operation.
		dataTable.batch(putShadowList);
		dataTable.flushCommits();
	}

	// The table for this should be the Global Txn Table; this function
	// delegates identification of the table and persisting, to
	// HBaseBackedTxnLogger.
	public void putDistTxnState(HTable logTable, final DistTxnState txnState,
			boolean shouldMigrateLocks) throws IOException {
		// The list to be sent to LockMigrator
		List<byte[]> lockSet = txnState.getPessimisticLockList();
		// Initiate LockMigrator and put it in the pool.
		// TODO: We should make this a global variable, as it will be used several
		// times.
		// Also, we should initiate this client with some random unique id. Send
		// that id
		// as the key for the destination LogId where the keys should be placed.
		if (shouldMigrateLocks) {
			LockMigrator lockMigrator = new LockMigrator(logTable.getConfiguration(),
					txnState.getTransactionId());
			lockMigrator.addLockSetToMigrate(lockSet);
			Future future = pool.submit(lockMigrator);
			futures.add(future);
			startTimeForMigration = System.currentTimeMillis();
		}

		// TODO: We need to add serialization function to txnState and persist
		// the actual state; very much needed for roll-forwarding. For now,
		// we just send a dummy value.
		byte[] serializedTxnState = Bytes.toBytes("SerializedTxnState");
		this.txnLogger.persistTxnState(txnState.getTransactionId(),
				serializedTxnState);
	}

	public boolean commitRequestAcquireLocks(final HTable table,
			final DistTxnState transactionState) throws Throwable {
		final long trxId = transactionState.getTransactionId();
		// Grab the entire writeBuffer and iterate on the keys in the order of
		// logIds.
		TreeMap<LogId, TreeSet<Action>> writeBuffer = transactionState
				.getWriteBufferWithLogIds();
		boolean allLocked = true;
		// Iterating in the order of LogIds so as to avoid deadlocks.
		Iterator<LogId> logIdItr = writeBuffer.navigableKeySet().iterator();
		while (logIdItr.hasNext()) {
			LogId logId = logIdItr.next();
			TreeSet<Action> actions = writeBuffer.get(logId);
			final List<ImmutableBytesWritable> keys = new LinkedList<ImmutableBytesWritable>();
			for (Action action : actions) {
				keys.add(new ImmutableBytesWritable(action.getAction().getRow()));
			}

			// We can save RPC calls by batching the Puts by region.
			// The code here assumes that all keys under a logId are present in the
			// same region.
			// This does not always hold. What we can do is: for the keys sent, the
			// server-side
			// coprocessor, can use HTable instance to do the locking on other regions
			// too, if needed.
			class AcquireLockCallBack implements Batch.Callback<List<Long>> {
				private long nbLocksAcquired;
				private long otherTrxIdWhichHasTheLock;

				Long getNbLocksAcquired() {
					return nbLocksAcquired;
				}

				Long getOtherTrxIdWhichHasTheLock() {
					return otherTrxIdWhichHasTheLock;
				}

				@Override
				public synchronized void update(byte[] region, byte[] row,
						List<Long> result) {
					// Since this coprocessorExec call will only go to one region hosting
					// the
					// keys present in that list, there will be only one Call
					this.nbLocksAcquired = result.get(0);
					this.otherTrxIdWhichHasTheLock = result.get(1);

					sysout(trxId, "In AcquireLockCallBack, for row: "
							+ Bytes.toString(row) + ", nbLocksAcquired is: "
							+ nbLocksAcquired + ", and otherTrxIdWhichHasTheLock is: "
							+ otherTrxIdWhichHasTheLock);
				}
			}

			AcquireLockCallBack aLockCallBack = new AcquireLockCallBack();
			table.coprocessorExec(WALManagerDistTxnProtocol.class, keys.get(0).get(),
					keys.get(0).get(),
					new Batch.Call<WALManagerDistTxnProtocol, List<Long>>() {
						@Override
						public List<Long> call(WALManagerDistTxnProtocol instance)
								throws IOException {
							return instance.commitRequestAcquireLocks(transactionState
									.getTransactionId(), keys);
						}
					}, aLockCallBack);

			if (aLockCallBack.getNbLocksAcquired() == keys.size())
				allLocked = allLocked && true;
			else {
				allLocked = allLocked && false;
				// TODO: Have the roll-forward logic here. Once the roll-forward is
				// tried, we need to
				// try to acquire the other remaining locks.
				// Note that the DistTxnMetadata object needs to be properly maintained
				// and put inside
				// the log table for roll-forwarding to work.
			}
		}
		return allLocked;
	}

	public boolean commitRequestAcquirePessimisticLocksViaIndirection(
			final HTable logTable, final DistTxnState transactionState,
			boolean stopForLockMigration) throws Throwable {
		final long trxId = transactionState.getTransactionId();
		if (stopForLockMigration) {
			long timeForMigration = stopForLockMigration();
			transactionState.setLockMigrationTime(timeForMigration);
			// System.out.println("Time for lock migration: " + timeForMigration);
		}
		sysout(trxId, "In acquire locks via indirection.");
		// Grab the entire writeBuffer and iterate on the keys in the order of
		// logIds.
		TreeMap<LogId, TreeSet<ImmutableBytesWritable>> pessimisticLocksPerLogId = transactionState
				.getPessimisticLocksPerLogId();
		boolean allLocked = true;
		long nbDetoursEncountered = 0;
		long nbNetworkHopsInTotal = 0;
		// Iterating in the order of LogIds so as to avoid deadlocks.
		final List<LogId> logs = new LinkedList<LogId>();
		final List<ImmutableBytesWritable> keys = new LinkedList<ImmutableBytesWritable>();
		final List<Boolean> isKeyMigrated = new LinkedList<Boolean>();
		Iterator<LogId> logIdItr = pessimisticLocksPerLogId.navigableKeySet()
				.iterator();
		while (logIdItr.hasNext()) {
			LogId logId = logIdItr.next();
			TreeSet<ImmutableBytesWritable> locks = pessimisticLocksPerLogId
					.get(logId);

			for (ImmutableBytesWritable lock : locks) {
				// Synchronized pair of key and its logId.
				keys.add(lock);
				sysout(trxId, "For key: " + Bytes.toString(lock.get())
						+ ", destination logId is: ");
				LogId correspLogId = LockMigrator.afterMigrationKeyMap.get(lock);
				if (correspLogId != null) {
					logs.add(correspLogId);
					if (correspLogId.getCommitType() == LogId.ONLY_DELETE) {
						isKeyMigrated.add(true);
					} else {
						isKeyMigrated.add(false);
					}
					sysout(trxId, correspLogId.toString());
				} else {
					logs.add(logId);
					isKeyMigrated.add(false);
					sysout(trxId, logId.toString());
				}
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

		// Map from original keys to their destination keys as observed when trying
		// to acquire the lock.
		// TODO: This map can also be populated by the migrateLokcs function when it
		// detects a migratedLock by
		// a conflicting client. For now, we just observe and note migration only
		// during locking.
		Map<LogId, LogId> dynamicMigrationMap = new HashMap<LogId, LogId>();
		long nbLocksAcquired = 0;
		long flag;
		while (true) {
			for (int i = 0; i < nbLocksAcquired; i++) {
				logs.remove(0);
				keys.remove(0);
				isKeyMigrated.remove(0);
			}
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
											isKeyMigrated);
								}
							}, aLockCallBack);

			// For all locks that were acquired, note their corresponding logIds.
			for (int i = 0; i < aLockCallBack.getNbLocksAcquired(); i++) {
				ImmutableBytesWritable key = keys.get(i);
				LogId finalLogIdAtWhichKeyWasLocked = logs.get(i);
				// Get the logId already present in afterMigrationKeyMap for this key.
				// If its commitInfoType is ONLY_DELETE, then we don't alter --
				// essentially we've
				// locked a key that we migrated. Otherwise, we place this new logId at
				// which final
				// locking took place, with a commitInfoType as ONLY_UNLOCK.
				LogId alreadyMappedLogId = LockMigrator.afterMigrationKeyMap.get(key);
				// Checks that the key wasn't successfully migrated by us.
				if (alreadyMappedLogId == null
						|| alreadyMappedLogId.getCommitType() != LogId.ONLY_DELETE) {
					finalLogIdAtWhichKeyWasLocked.setCommitType(LogId.ONLY_UNLOCK);
					LockMigrator.afterMigrationKeyMap.put(key,
							finalLogIdAtWhichKeyWasLocked);
				}
				// Add the key to trxState's lockedKeys set.
				transactionState.addLockedKey(key);
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
				nbLocksAcquired = aLockCallBack.getNbLocksAcquired();
				sysout(trxId, "Could not acquire all locks! Flag is: " + flag
						+ " , nbLocksAcquired: " + nbLocksAcquired + " , sent keys size: "
						+ keys.size() + "; the next to be acquired: "
						+ Bytes.toString(keys.get((int) nbLocksAcquired).get()));

				// TODO: Have the roll-forward logic here. Once the roll-forward is
				// tried, we need to
				// try to acquire the other remaining locks.
				// Note that the DistTxnMetadata object needs to be properly maintained
				// and put inside
				// the log table for roll-forwarding to work.
				// For now, we don't do roll-forwarding, we just retry acquiring the
				// lock either at the
				// same key or at the destination key.
				// If flag is 3, then it means the detour was deleted, which means we
				// have to
				// go back.
				int indexOfMigratedKey = (int) nbLocksAcquired;
				if (flag == 3) {
					// The detour is deleted, we lookup the detour we original took and
					// noted.
					nbDetoursEncountered++;
					LogId migratedToLogId = logs.get(indexOfMigratedKey);
					LogId previousLogIdOfMigratedKey = dynamicMigrationMap
							.get(migratedToLogId);
					logs.set(indexOfMigratedKey, previousLogIdOfMigratedKey);
					// Make a note that migration is removed.
					isKeyMigrated.set(indexOfMigratedKey, false);
					continue;
				} else if (flag == 2) {
					// We found a detour. Note it in the dynamicMigrationMap and change
					// your destination
					// for future tries.
					nbDetoursEncountered++;
					LogId previousLogIdOfMigratedKey = logs.get(indexOfMigratedKey);
					byte[] migratedToDestKey = aLockCallBack.getMigratedToKey();
					if (migratedToDestKey != null) {
						LogId migratedToLogId = WALTableProperties
								.getLogIdFromMigratedKey(migratedToDestKey);
						migratedToLogId.setCommitType(LogId.ONLY_UNLOCK);
						dynamicMigrationMap
								.put(migratedToLogId, previousLogIdOfMigratedKey);
						sysout(trxId, "Before changing the route in logs list, size is : "
								+ logs.size());
						logs.set(indexOfMigratedKey, migratedToLogId);
						// Make a note that we are now locking at the migrated destination.
						isKeyMigrated.set(indexOfMigratedKey, true);
						sysout(trxId, "After changing the route in logs list, size is : "
								+ logs.size());
					}
				}
			}
		}

		transactionState.setNbDetoursEncountered(nbDetoursEncountered);
		transactionState.setNbNetworkHopsInTotal(nbNetworkHopsInTotal);
		return true;
	}

	public boolean commitRequestAcquireLocksViaIndirection(final HTable logTable,
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
		boolean allLocked = true;
		long nbDetoursEncountered = 0;
		long nbNetworkHopsInTotal = 0;
		// Iterating in the order of LogIds so as to avoid deadlocks.
		final List<LogId> logs = new LinkedList<LogId>();
		final List<ImmutableBytesWritable> keys = new LinkedList<ImmutableBytesWritable>();
		final List<Boolean> isKeyMigrated = new LinkedList<Boolean>();
		Iterator<LogId> logIdItr = writeBuffer.navigableKeySet().iterator();
		while (logIdItr.hasNext()) {
			LogId logId = logIdItr.next();
			TreeSet<Action> actions = writeBuffer.get(logId);

			for (Action action : actions) {
				// Synchronized pair of key and its logId.
				ImmutableBytesWritable key = new ImmutableBytesWritable(action
						.getAction().getRow());
				keys.add(key);
				sysout(trxId, "For key: " + Bytes.toString(key.get())
						+ ", destination logId is: ");
				LogId correspLogId = LockMigrator.afterMigrationKeyMap.get(key);
				if (correspLogId != null) {
					logs.add(correspLogId);
					if (correspLogId.getCommitType() == LogId.ONLY_DELETE) {
						isKeyMigrated.add(true);
					} else {
						isKeyMigrated.add(false);
					}
					sysout(trxId, correspLogId.toString());
				} else {
					logs.add(logId);
					isKeyMigrated.add(false);
					sysout(trxId, logId.toString());
				}
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

		// Map from original keys to their destination keys as observed when trying
		// to acquire the lock.
		// TODO: This map can also be populated by the migrateLokcs function when it
		// detects a migratedLock by
		// a conflicting client. For now, we just observe and note migration only
		// during locking.
		Map<LogId, LogId> dynamicMigrationMap = new HashMap<LogId, LogId>();
		long nbLocksAcquired = 0;
		long flag;
		while (true) {
			for (int i = 0; i < nbLocksAcquired; i++) {
				logs.remove(0);
				keys.remove(0);
				isKeyMigrated.remove(0);
			}
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
											isKeyMigrated);
								}
							}, aLockCallBack);

			// For all locks that were acquired, note their corresponding logIds.
			for (int i = 0; i < aLockCallBack.getNbLocksAcquired(); i++) {
				ImmutableBytesWritable key = keys.get(i);
				LogId finalLogIdAtWhichKeyWasLocked = logs.get(i);
				// Get the logId already present in afterMigrationKeyMap for this key.
				// If its commitInfoType is ONLY_DELETE, then we don't alter --
				// essentially we've
				// locked a key that we migrated. Otherwise, we place this new logId at
				// which final
				// locking took place, with a commitInfoType as ONLY_UNLOCK.
				LogId alreadyMappedLogId = LockMigrator.afterMigrationKeyMap.get(key);
				// Checks that the key wasn't successfully migrated by us.
				if (alreadyMappedLogId == null
						|| alreadyMappedLogId.getCommitType() != LogId.ONLY_DELETE) {
					finalLogIdAtWhichKeyWasLocked.setCommitType(LogId.ONLY_UNLOCK);
					LockMigrator.afterMigrationKeyMap.put(key,
							finalLogIdAtWhichKeyWasLocked);
				}
				// Add the key to trxState's lockedKeys set.
				transactionState.addLockedKey(key);
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
				nbLocksAcquired = aLockCallBack.getNbLocksAcquired();
				sysout(trxId, "Could not acquire all locks! Flag is: " + flag
						+ " , nbLocksAcquired: " + nbLocksAcquired + " , sent keys size: "
						+ keys.size() + "; the next to be acquired: "
						+ Bytes.toString(keys.get((int) nbLocksAcquired).get()));

				// TODO: Have the roll-forward logic here. Once the roll-forward is
				// tried, we need to
				// try to acquire the other remaining locks.
				// Note that the DistTxnMetadata object needs to be properly maintained
				// and put inside
				// the log table for roll-forwarding to work.
				// For now, we don't do roll-forwarding, we just retry acquiring the
				// lock either at the
				// same key or at the destination key.
				// If flag is 3, then it means the detour was deleted, which means we
				// have to
				// go back.
				int indexOfMigratedKey = (int) nbLocksAcquired;
				if (flag == 3) {
					// The detour is deleted, we lookup the detour we original took and
					// noted.
					nbDetoursEncountered++;
					LogId migratedToLogId = logs.get(indexOfMigratedKey);
					LogId previousLogIdOfMigratedKey = dynamicMigrationMap
							.get(migratedToLogId);
					if (previousLogIdOfMigratedKey == null) {
						previousLogIdOfMigratedKey = WALTableProperties
								.getLogIdFromMigratedKey(keys.get(indexOfMigratedKey).get());
					}
					logs.set(indexOfMigratedKey, previousLogIdOfMigratedKey);
					// Make a note that migration is removed.
					isKeyMigrated.set(indexOfMigratedKey, false);
					continue;
				} else if (flag == 2) {
					// We found a detour. Note it in the dynamicMigrationMap and change
					// your destination
					// for future tries.
					nbDetoursEncountered++;
					LogId previousLogIdOfMigratedKey = logs.get(indexOfMigratedKey);
					byte[] migratedToDestKey = aLockCallBack.getMigratedToKey();
					if (migratedToDestKey != null) {
						LogId migratedToLogId = WALTableProperties
								.getLogIdFromMigratedKey(migratedToDestKey);
						migratedToLogId.setCommitType(LogId.ONLY_UNLOCK);
						dynamicMigrationMap
								.put(migratedToLogId, previousLogIdOfMigratedKey);
						sysout(trxId, "Before changing the route in logs list, size is : "
								+ logs.size());
						logs.set(indexOfMigratedKey, migratedToLogId);
						// Make a note that we are now locking at the migrated destination.
						isKeyMigrated.set(indexOfMigratedKey, true);
						sysout(trxId, "After changing the route in logs list, size is : "
								+ logs.size());
					}
				}
			}
		}

		transactionState.setNbDetoursEncountered(nbDetoursEncountered);
		transactionState.setNbNetworkHopsInTotal(nbNetworkHopsInTotal);
		return true;
	}

	public long commitRequestAcquireLocksFromWAL(final HTable table,
			final DistTxnState transactionState) throws Throwable {
		final long trxId = transactionState.getTransactionId();
		// Grab the entire writeBuffer and iterate on the keys in the order of
		// logIds.
		TreeMap<LogId, TreeSet<Action>> writeBuffer = transactionState
				.getWriteBufferWithLogIds();
		TreeMap<HRegionLocation, List<LogId>> regionToLogIdMap = transactionState
				.getRegionToLogIdMap();
		// We traverse the regionToLogIdMap in the order of HRegionLocation as given
		// by the
		// MyHRegionLocationComparator in DistTxnState. If we want to include
		// contention based
		// ordering, we'll have to modify that comparator. Furthermore, all the lock
		// requests for
		// a single HRegion (grouped by their respective LogIds) are sent to the
		// corresponding
		// coprocessor at once. The list of LogIds should be sorted according to the
		// order specified by the LogIdComparator. The sorted list should be sent as
		// such.
		class AcquireLockFromWALCallBack implements Batch.Callback<Long> {
			private long nbUnsuccessfulAttempts = 0;

			public long getNbUnsuccessfulAttempts() {
				return nbUnsuccessfulAttempts;
			}

			@Override
			public synchronized void update(byte[] region, byte[] row, Long result) {
				// Since this coprocessorExec call will only go to one region
				// hosting the
				// keys present in that list, there will be only one Call
				this.nbUnsuccessfulAttempts += result;

				sysout(trxId, "In AcquireLockCallBack, for row: " + Bytes.toString(row)
						+ ", nbUnsuccessfulAttempts is: " + this.nbUnsuccessfulAttempts);
			}
		}

		long nbUnsuccessfulAttempts = 0;
		Iterator<HRegionLocation> regionLocationItr = regionToLogIdMap
				.navigableKeySet().iterator();
		while (regionLocationItr.hasNext()) {
			HRegionLocation location = regionLocationItr.next();
			final List<LogId> logIdList = regionToLogIdMap.get(location);
			// Sort the list basing on LogIdComparator.
			Collections.sort(logIdList, new DistTxnState.LogIdComparator());
			final List<List<ImmutableBytesWritable>> keysToBeLocked = new LinkedList<List<ImmutableBytesWritable>>();
			// Iterating in the order of LogIds so as to avoid deadlocks.
			for (Iterator<LogId> logIdItr = logIdList.iterator(); logIdItr.hasNext();) {
				LogId logId = logIdItr.next();
				TreeSet<Action> actions = writeBuffer.get(logId);
				final List<ImmutableBytesWritable> keys = new LinkedList<ImmutableBytesWritable>();
				for (Action action : actions) {
					sysout(trxId, "For logId: " + logId.toString() + ", adding key: "
							+ Bytes.toString(action.getAction().getRow()));
					keys.add(new ImmutableBytesWritable(action.getAction().getRow()));
				}

				keysToBeLocked.add(keys);
			}
			// We can save RPC calls by batching the Puts by region.
			// The locking happens through local transactions on the logIds. If the
			// snapshots
			// contain a consistent information of the locks, then the lock acquiring
			// will not need
			// any RPC call over a network. However, some more effort is needed to
			// make WALs maintain
			// consistent lock information. Furthermore, since we are grouping lock
			// requests by the
			// region their respective WALs reside in. Thus, the optimization of
			// placing WALs on a single
			// node gives performance.
			LogId sampleLogId = logIdList.get(0);
			AcquireLockFromWALCallBack aLockCallBack = new AcquireLockFromWALCallBack();
			table.coprocessorExec(WALManagerDistTxnProtocol.class, sampleLogId
					.getKey(), sampleLogId.getKey(),
					new Batch.Call<WALManagerDistTxnProtocol, Long>() {
						@Override
						public Long call(WALManagerDistTxnProtocol instance)
								throws IOException {
							return instance.commitRequestAcquireLocksFromWAL(transactionState
									.getTransactionId(), logIdList, keysToBeLocked);
						}
					}, aLockCallBack);
			nbUnsuccessfulAttempts += aLockCallBack.getNbUnsuccessfulAttempts();
		}
		// Since waiting happens at the server-side, there cannot be a false as
		// return value.
		return nbUnsuccessfulAttempts;
	}

	boolean commitRequestCheckVersions(final HTable table,
			final DistTxnState transactionState) throws IOException,
			InterruptedException {
		long trxId = transactionState.getTransactionId();
		List<Pair<ImmutableBytesWritable, Long>> readVersionList = transactionState
				.getReadVersionList();
		List<Get> actions = new LinkedList<Get>();
		for (Pair<ImmutableBytesWritable, Long> readWithVersion : readVersionList) {
			ImmutableBytesWritable key = readWithVersion.getFirst();
			Get g = new Get(key.get());
			g.addColumn(dataFamily, versionColumn);
			actions.add(g);
		}

		Object[] results = table.batch(actions);
		boolean isFresh = true;
		for (int i = 0; i < results.length; i++) {
			long presentVersion = Bytes.toLong(((Result) results[i]).getValue(
					dataFamily, versionColumn));
			long cacheVersion = readVersionList.get(i).getSecond();
			sysout(trxId, "Present version for key: "
					+ Bytes.toString(((Result) results[i]).getRow()) + ", is: "
					+ presentVersion);
			sysout(trxId, "Cache version is: " + cacheVersion);
			if (cacheVersion != presentVersion) {
				isFresh = isFresh && false;
			}
		}
		return isFresh;
	}

	boolean commitRequestCheckVersionsFromWAL(final HTable logTable,
			final HTable dataTable, final DistTxnState transactionState)
			throws Throwable {
		final long trxId = transactionState.getTransactionId();
		List<Pair<ImmutableBytesWritable, Long>> readVersionList = transactionState
				.getReadVersionList();
		List<Get> actions = new LinkedList<Get>();
		for (Pair<ImmutableBytesWritable, Long> readWithVersion : readVersionList) {
			ImmutableBytesWritable key = readWithVersion.getFirst();
			// TODO: Should also fetch write-lock information for this object. Should abort if
			// we found a lock. Specifically, in our case, we need to fetch isLockPlacedOrMigrated 
			// column and also isLockMigrated column. If the former is set to 1 and the latter is set
			// to zero, then the lock is placed.
			Get g = new Get(key.get());
			g.addColumn(dataFamily, versionColumn);
			g.addColumn(WALTableProperties.WAL_FAMILY, WALTableProperties.regionObserverMarkerColumn);
			actions.add(g);
		}

		// We use the List<Get> gets function in this client. It first checks in the
		// snapshot
		// and if doesn't find it, fetches it from the table.
		List<Result> results = this.getWithServerSideMerge(logTable, dataTable,
				transactionState, actions);
		boolean isFresh = true;
		for (int i = 0; i < results.size(); i++) {
			Result r = results.get(i);
			long presentVersion = Long.parseLong(Bytes.toString(r.getValue(
					dataFamily, versionColumn)));
			long cacheVersion = readVersionList.get(i).getSecond();
			sysout(trxId, "Present version for key: " + Bytes.toString(r.getRow())
					+ ", is: " + presentVersion);
			sysout(trxId, "Cache version is: " + cacheVersion);
			if (cacheVersion != presentVersion) {
				isFresh = isFresh && false;
			}
		}
		return isFresh;
	}

	boolean commitWritesPerEntityGroup(final HTable table,
			final DistTxnState distTxnState) throws Throwable {
		final long trxId = distTxnState.getTransactionId();
		sysout(trxId, "Inside commit writes per entity group");
		// Collect the writeBufferPerLogId and send it off to the server-side
		// coprocessor.
		// The coprocessor should simply remove locks on write-keys, change their
		// versions, and
		// copy the shadow objects as values of the original keys.
		Map<LogId, List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>>> logIdToAllWritesMap = distTxnState
				.getLogIdToAllWritesMap();

		class EntityGroupCommitCallBack implements Batch.Callback<Boolean> {
			private boolean entityGroupCommitResult = true;

			@Override
			public synchronized void update(byte[] region, byte[] row, Boolean result) {
				// There will be multiple call back invocations, as we send the
				// coprocessor exec request
				// to all WAL logs involved in the trx.
				entityGroupCommitResult = entityGroupCommitResult && result;
				sysout(trxId, "In EntityGroupCommitCallBack, result is: " + result);
			}

			boolean getResult() {
				return this.entityGroupCommitResult;
			}
		}

		EntityGroupCommitCallBack entityGroupCommitCallBack = new EntityGroupCommitCallBack();
		for (Map.Entry<LogId, List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>>> entry : logIdToAllWritesMap
				.entrySet()) {
			final LogId logId = entry.getKey();
			final List<ImmutableBytesWritable> keyList = new ArrayList<ImmutableBytesWritable>(
					entry.getValue().size());
			final List<ImmutableBytesWritable> shadowKeyList = new ArrayList<ImmutableBytesWritable>(
					entry.getValue().size());
			// Splitting the list of pairs into two synchronized lists.
			for (Pair<ImmutableBytesWritable, ImmutableBytesWritable> p : entry
					.getValue()) {
				sysout(trxId, "OrigKey: " + Bytes.toString(p.getFirst().get()));
				sysout(trxId, "ShadowKey: " + Bytes.toString(p.getSecond().get()));
				keyList.add(p.getFirst());
				shadowKeyList.add(p.getSecond());
			}

			table.coprocessorExec(WALManagerDistTxnProtocol.class, logId.getKey(),
					logId.getKey(), new Batch.Call<WALManagerDistTxnProtocol, Boolean>() {
						@Override
						public Boolean call(WALManagerDistTxnProtocol instance)
								throws IOException {
							return instance.commitWritesPerEntityGroup(distTxnState
									.getTransactionId(), logId, keyList, shadowKeyList);
						}
					}, entityGroupCommitCallBack);
		}

		return entityGroupCommitCallBack.getResult();
	}

	boolean commitWritesPerEntityGroupWithoutShadows(final HTable table,
			final DistTxnState transactionState) throws Throwable {
		final long trxId = transactionState.getTransactionId();
		sysout(trxId, "Inside commit writes per entity group");
		// Collect the writeBufferPerLogId and send it off to the server-side
		// coprocessor.
		// The coprocessor should simply remove locks on write-keys, change their
		// versions, and transform the Puts into Writes.
		// This function does not copy shadow objects. Infact no shadow objects are
		// ever
		// written. The puts to the original keys (updated values) are sent directly
		// through
		// this function. Furthermore, we make this function batch the entity group
		// commits
		// on per-region basis. This will reduce the RPC calls.
		// Grab the entire writeBuffer and iterate on the keys in the order of
		// logIds.
		TreeMap<LogId, TreeSet<Action>> writeBuffer = transactionState
				.getWriteBufferWithLogIds();
		// We traverse the regionToLogIdMap in the order of HRegionLocation as given
		// by the
		// MyHRegionLocationComparator in DistTxnState. If we want to include
		// contention based
		// ordering, we'll have to modify that comparator. Furthermore, all the lock
		// requests for
		// a single HRegion (grouped by their respective LogIds) are sent to the
		// corresponding
		// coprocessor at once. The list of LogIds should be sorted according to the
		// order specified by the LogIdComparator. The sorted list should be sent as
		// such.
		class EntityGroupCommitCallBack implements Batch.Callback<Boolean> {
			private boolean entityGroupCommitResult = true;

			@Override
			public synchronized void update(byte[] region, byte[] row, Boolean result) {
				// There will be multiple call back invocations, as we send the
				// coprocessor exec request
				// to all WAL logs involved in the trx.
				entityGroupCommitResult = entityGroupCommitResult && result;
				sysout(trxId, "In EntityGroupCommitCallBack, result is: " + result);
			}

			boolean getResult() {
				return this.entityGroupCommitResult;
			}
		}

		boolean allUpdated = true;
		// Instead of collecting LogIds and their corresponding Puts per HRegion,
		// we just send all of them (logIds and Put lists) to all coprocessors and
		// make the
		// server-side seive through the list based on the LogIds presence in its
		// region.
		final List<LogId> logIdList = new LinkedList<LogId>();
		final List<List<Put>> allUpdates = new LinkedList<List<Put>>();
		final List<List<LogId>> toBeUnlockedDestLogIds = new LinkedList<List<LogId>>();
		// Iterating in the order of LogIds so as to avoid deadlocks.
		for (Iterator<LogId> logIdItr = writeBuffer.keySet().iterator(); logIdItr
				.hasNext();) {
			LogId logId = logIdItr.next();
			logIdList.add(logId);
			TreeSet<Action> actions = writeBuffer.get(logId);
			final List<Put> updates = new LinkedList<Put>();
			final List<LogId> destLogIds = new LinkedList<LogId>();
			for (Action action : actions) {
				sysout(trxId, "For logId: " + logId.toString() + ", adding key: "
						+ Bytes.toString(action.getAction().getRow()));
				updates.add((Put) action.getAction());
				LogId destLogId = LockMigrator.afterMigrationKeyMap
						.get(new ImmutableBytesWritable(action.getAction().getRow()));
				if (destLogId != null)
					destLogIds.add(destLogId);
				else {
					logId.setCommitType(LogId.ONLY_UNLOCK);
					destLogIds.add(logId);
				}
			}
			allUpdates.add(updates);
			toBeUnlockedDestLogIds.add(destLogIds);
		}

		// Instead of sending it to a particular server-side coprocessor, we send
		// the Call
		// to all of them. They choose to act if the list of LogIds have any that
		// belong to them.
		// TODO: In this setting, regions which do not have any logIds will also do
		// work.
		// However, the present table.coprocessorExec API does not have a way to
		// specify only
		// particular regions to which the Exec should be sent to.
		EntityGroupCommitCallBack entityGroupCommitCallBack = new EntityGroupCommitCallBack();
		table.coprocessorExec(WALManagerDistTxnProtocol.class,
				HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
				new Batch.Call<WALManagerDistTxnProtocol, Boolean>() {
					@Override
					public Boolean call(WALManagerDistTxnProtocol instance)
							throws IOException {
						return instance.commitWritesPerEntityGroupWithoutShadows(
								transactionState.getTransactionId(), logIdList, allUpdates,
								toBeUnlockedDestLogIds);
					}
				}, entityGroupCommitCallBack);
		allUpdated = allUpdated && entityGroupCommitCallBack.getResult();
		return allUpdated;
	}

	boolean commitWritesPerEntityGroupWithShadowsForPessimisticLocks(
			final HTable table, final DistTxnState transactionState) throws Throwable {
		final long trxId = transactionState.getTransactionId();
		sysout(trxId, "Inside commit writes per entity group");
		// Collect the writeBufferPerLogId and send it off to the server-side
		// coprocessor.
		// The coprocessor should simply remove locks on write-keys, change their
		// versions, and transform the Puts into Writes.
		// This function does not copy shadow objects. Infact no shadow objects are
		// ever
		// written. The puts to the original keys (updated values) are sent directly
		// through
		// this function. Furthermore, we make this function batch the entity group
		// commits
		// on per-region basis. This will reduce the RPC calls.
		// Grab the entire writeBuffer and iterate on the keys in the order of
		// logIds.
		TreeMap<LogId, TreeSet<Action>> writeBuffer = transactionState
				.getWriteBufferWithLogIds();
		// We traverse the regionToLogIdMap in the order of HRegionLocation as given
		// by the
		// MyHRegionLocationComparator in DistTxnState. If we want to include
		// contention based
		// ordering, we'll have to modify that comparator. Furthermore, all the lock
		// requests for
		// a single HRegion (grouped by their respective LogIds) are sent to the
		// corresponding
		// coprocessor at once. The list of LogIds should be sorted according to the
		// order specified by the LogIdComparator. The sorted list should be sent as
		// such.
		class EntityGroupCommitCallBack implements Batch.Callback<Boolean> {
			private boolean entityGroupCommitResult = true;

			@Override
			public synchronized void update(byte[] region, byte[] row, Boolean result) {
				// There will be multiple call back invocations, as we send the
				// coprocessor exec request
				// to all WAL logs involved in the trx.
				entityGroupCommitResult = entityGroupCommitResult && result;
				sysout(trxId, "In EntityGroupCommitCallBack, result is: " + result);
			}

			boolean getResult() {
				return this.entityGroupCommitResult;
			}
		}

		boolean allUpdated = true;
		// Instead of collecting LogIds and their corresponding Puts per HRegion,
		// we just send all of them (logIds and Put lists) to all coprocessors and
		// make the
		// server-side seive through the list based on the LogIds presence in its
		// region.
		final List<LogId> logIdList = new LinkedList<LogId>();
		final List<List<Put>> allUpdates = new LinkedList<List<Put>>();
		final List<List<LogId>> toBeUnlockedDestLogIds = new LinkedList<List<LogId>>();
		Map<ImmutableBytesWritable, ImmutableBytesWritable> writeBufferKeys = new HashMap<ImmutableBytesWritable, ImmutableBytesWritable>();
		Map<LogId, Integer> logIdToIndexInList = new HashMap<LogId, Integer>();
		// Iterating in the order of LogIds so as to avoid deadlocks.
		for (Iterator<LogId> logIdItr = writeBuffer.keySet().iterator(); logIdItr
				.hasNext();) {
			LogId logId = logIdItr.next();
			logIdList.add(logId);
			logIdToIndexInList.put(logId, logIdList.size() - 1);
			TreeSet<Action> actions = writeBuffer.get(logId);
			final List<Put> updates = new LinkedList<Put>();
			final List<LogId> destLogIds = new LinkedList<LogId>();
			for (Action action : actions) {
				sysout(trxId, "For logId: " + logId.toString() + ", adding key: "
						+ Bytes.toString(action.getAction().getRow()));
				updates.add((Put) action.getAction());
				writeBufferKeys
						.put(new ImmutableBytesWritable(action.getAction().getRow()),
								new ImmutableBytesWritable(action.getAction().getRow()));
				LogId destLogId = LockMigrator.afterMigrationKeyMap
						.get(new ImmutableBytesWritable(action.getAction().getRow()));
				if (destLogId != null)
					destLogIds.add(destLogId);
				else {
					logId.setCommitType(LogId.ONLY_UNLOCK);
					destLogIds.add(logId);
				}
			}
			allUpdates.add(updates);
			toBeUnlockedDestLogIds.add(destLogIds);
		}

		// Iterate through the pessimistic locks that were acquired. If we already
		// placed
		// an unlock request above through write-buffer, then skip it; otherwise,
		// add it to the
		// list.
		TreeMap<LogId, TreeSet<ImmutableBytesWritable>> pessimisticLocksPerLogId = transactionState
				.getPessimisticLocksPerLogId();
		for (LogId logId : pessimisticLocksPerLogId.keySet()) {
			TreeSet<ImmutableBytesWritable> locks = pessimisticLocksPerLogId
					.get(logId);
			Integer index = logIdToIndexInList.get(logId);
			List<Put> correspUpdatesList = null;
			List<LogId> correspDestLogIds = null;
			if (index != null) {
				correspUpdatesList = allUpdates.get(index);
				correspDestLogIds = toBeUnlockedDestLogIds.get(index);
			} else {
				correspUpdatesList = new LinkedList<Put>();
				correspDestLogIds = new LinkedList<LogId>();

				logIdList.add(logId);
				allUpdates.add(correspUpdatesList);
				toBeUnlockedDestLogIds.add(correspDestLogIds);
			}

			for (ImmutableBytesWritable lock : locks) {
				Put p = new Put();
				correspUpdatesList.add(new Put(lock.get()));
				LogId destLogId = LockMigrator.afterMigrationKeyMap.get(lock);
				if (destLogId != null)
					correspDestLogIds.add(destLogId);
				else {
					logId.setCommitType(LogId.ONLY_UNLOCK);
					correspDestLogIds.add(logId);
				}
			}
		}

		// Instead of sending it to a particular server-side coprocessor, we send
		// the Call
		// to all of them. They choose to act if the list of LogIds have any that
		// belong to them.
		// TODO: In this setting, regions which do not have any logIds will also do
		// work.
		// However, the present table.coprocessorExec API does not have a way to
		// specify only
		// particular regions to which the Exec should be sent to.
		EntityGroupCommitCallBack entityGroupCommitCallBack = new EntityGroupCommitCallBack();
		table.coprocessorExec(WALManagerDistTxnProtocol.class,
				HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
				new Batch.Call<WALManagerDistTxnProtocol, Boolean>() {
					@Override
					public Boolean call(WALManagerDistTxnProtocol instance)
							throws IOException {
						return instance.commitWritesPerEntityGroupWithShadows(
								transactionState.getTransactionId(), logIdList, allUpdates,
								toBeUnlockedDestLogIds);
					}
				}, entityGroupCommitCallBack);
		allUpdated = allUpdated && entityGroupCommitCallBack.getResult();
		return allUpdated;
	}

	boolean commitWritesPerEntityGroupWithShadows(final HTable table,
			final DistTxnState transactionState) throws Throwable {
		final long trxId = transactionState.getTransactionId();
		// Collect the writeBufferPerLogId and send it off to the server-side
		// coprocessor.
		// The coprocessor should simply remove locks on write-keys, change their
		// versions, and transform the Puts into Writes.
		// This function does not copy shadow objects. Infact no shadow objects are
		// ever
		// written. The puts to the original keys (updated values) are sent directly
		// through
		// this function. Furthermore, we make this function batch the entity group
		// commits
		// on per-region basis. This will reduce the RPC calls.
		// Grab the entire writeBuffer and iterate on the keys in the order of
		// logIds.
		TreeMap<LogId, TreeSet<Action>> writeBuffer = transactionState
				.getWriteBufferWithLogIds();
		// We traverse the regionToLogIdMap in the order of HRegionLocation as given
		// by the
		// MyHRegionLocationComparator in DistTxnState. If we want to include
		// contention based
		// ordering, we'll have to modify that comparator. Furthermore, all the lock
		// requests for
		// a single HRegion (grouped by their respective LogIds) are sent to the
		// corresponding
		// coprocessor at once. The list of LogIds should be sorted according to the
		// order specified by the LogIdComparator. The sorted list should be sent as
		// such.
		class EntityGroupCommitCallBack implements Batch.Callback<Boolean> {
			private boolean entityGroupCommitResult = true;

			@Override
			public synchronized void update(byte[] region, byte[] row, Boolean result) {
				// There will be multiple call back invocations, as we send the
				// coprocessor exec request
				// to all WAL logs involved in the trx.
				entityGroupCommitResult = entityGroupCommitResult && result;
				// sysout(trxId, "In EntityGroupCommitCallBack, result is: " + result);
			}

			boolean getResult() {
				return this.entityGroupCommitResult;
			}
		}

		boolean allUpdated = true;
		// Instead of collecting LogIds and their corresponding Puts per HRegion,
		// we just send all of them (logIds and Put lists) to all coprocessors and
		// make the
		// server-side seive through the list based on the LogIds presence in its
		// region.
		final List<LogId> logIdList = new LinkedList<LogId>();
		final List<List<Put>> allUpdates = new LinkedList<List<Put>>();
		final List<List<LogId>> toBeUnlockedDestLogIds = new LinkedList<List<LogId>>();
		// Iterating in the order of LogIds so as to avoid deadlocks.
		for (Iterator<LogId> logIdItr = writeBuffer.keySet().iterator(); logIdItr
				.hasNext();) {
			LogId logId = logIdItr.next();
			logIdList.add(logId);
			TreeSet<Action> actions = writeBuffer.get(logId);
			final List<Put> updates = new LinkedList<Put>();
			final List<LogId> destLogIds = new LinkedList<LogId>();
			for (Action action : actions) {
				sysout(trxId, "For logId: " + logId.toString() + ", adding key: "
						+ Bytes.toString(action.getAction().getRow()));
				updates.add((Put) action.getAction());
				LogId destLogId = LockMigrator.afterMigrationKeyMap
						.get(new ImmutableBytesWritable(action.getAction().getRow()));
				if (destLogId != null) {
					// BIG NOTE: We are disabling "greedy eviction" for
					// districtNextOrderId lock.
					// We are doing this by making the update to lock in lock-table as
					// ONLY_UNLOCK
					// instead of ONLY_DELETE. This is what will ideally be done by the
					// clustering
					// process on the lock-table. The process won't evict this lock as it
					// will
					// be used frequently. It could evict the other locks, which are
					// mostly
					// for stock table, and also are infrequently re-used. Now, this
					// change here
					// is TPCC specific. We can't be doing this change in
					// WALManagerDistTxnClient
					// as its a very generic class. We need to figure out another way of
					// doing this.
					// Ideally, it should be done by the clustering process on the
					// lock-table.
					if (Bytes.toString(logId.getKey()).startsWith(
							TPCCTableProperties.districtWALPrefix)) {
						// sysout(trxId, "Found the district log Id: "
						// + Bytes.toString(logId.getKey()));
						// sysout(trxId, "GIVING ONLY_UNLOCK attribute for destLogId: "
						// + destLogId.toString());
						destLogId.setCommitType(LogId.ONLY_UNLOCK);
					}
					// BIGNOTE: We are disabling all deletes to debug deadlocks.
					destLogId.setCommitType(LogId.ONLY_UNLOCK);
					sysout(trxId, "Adding this destLogId for commit: "
							+ destLogId.toString() + " with commit type: "
							+ destLogId.getCommitType());
					destLogIds.add(destLogId);
				}
			}
			allUpdates.add(updates);
			toBeUnlockedDestLogIds.add(destLogIds);
		}

		// Instead of sending it to a particular server-side coprocessor, we send
		// the Call
		// to all of them. They choose to act if the list of LogIds have any that
		// belong to them.
		// TODO: In this setting, regions which do not have any logIds will also do
		// work.
		// However, the present table.coprocessorExec API does not have a way to
		// specify only
		// particular regions to which the Exec should be sent to.
		EntityGroupCommitCallBack entityGroupCommitCallBack = new EntityGroupCommitCallBack();
		table.coprocessorExec(WALManagerDistTxnProtocol.class,
				HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
				new Batch.Call<WALManagerDistTxnProtocol, Boolean>() {
					@Override
					public Boolean call(WALManagerDistTxnProtocol instance)
							throws IOException {
						return instance.commitWritesPerEntityGroupWithShadows(
								transactionState.getTransactionId(), logIdList, allUpdates,
								toBeUnlockedDestLogIds);
					}
				}, entityGroupCommitCallBack);
		allUpdated = allUpdated && entityGroupCommitCallBack.getResult();
		return allUpdated;
	}

	boolean abortWithoutShadows(final HTable logTable,
			final DistTxnState transactionState) throws Throwable {
		final long trxId = transactionState.getTransactionId();
		TreeMap<LogId, TreeSet<Action>> writeBuffer = transactionState
				.getWriteBufferWithLogIds();
		class AbortCallBack implements Batch.Callback<Boolean> {
			private boolean abortResult = true;

			@Override
			public synchronized void update(byte[] region, byte[] row, Boolean result) {
				// There will be multiple call back invocations, as we send the
				// coprocessor exec request
				// to all WAL logs involved in the trx.
				abortResult = abortResult && result;
				//sysout(trxId, "In AbortCallBack, result is: " + result);
			}

			boolean getResult() {
				return this.abortResult;
			}
		}

		boolean allAborted = true;
		// Instead of collecting LogIds and their corresponding Puts per HRegion,
		// we just send all of them (logIds and Put lists) to all coprocessors and
		// make the
		// server-side seive through the list based on the LogIds presence in its
		// region.
		final List<LogId> logIdList = new LinkedList<LogId>();
		final List<List<ImmutableBytesWritable>> allKeysToBeUnlocked = new LinkedList<List<ImmutableBytesWritable>>();
		final List<List<LogId>> toBeUnlockedDestLogIds = new LinkedList<List<LogId>>();

		// The set contains only those keys which were locked -- the others weren't
		// locked because
		// a version check failed on a locked key.
		HashSet<ImmutableBytesWritable> lockedKeys = transactionState
				.getLockedKeys();
		// Iterating in the order of LogIds so as to avoid deadlocks.
		for (Iterator<LogId> logIdItr = writeBuffer.keySet().iterator(); logIdItr
				.hasNext();) {
			LogId logId = logIdItr.next();
			logIdList.add(logId);
			TreeSet<Action> actions = writeBuffer.get(logId);
			final List<ImmutableBytesWritable> keysToBeUnlocked = new LinkedList<ImmutableBytesWritable>();
			final List<LogId> destLogIds = new LinkedList<LogId>();
			for (Action action : actions) {
				ImmutableBytesWritable key = new ImmutableBytesWritable(action
						.getAction().getRow());
				if (lockedKeys.contains(key)) {
					sysout(trxId, "In abort, for logId: " + logId.toString()
							+ ", adding key: " + Bytes.toString(action.getAction().getRow()));
					keysToBeUnlocked.add(key);
					LogId destLogId = LockMigrator.afterMigrationKeyMap.get(key);
					if (destLogId != null) {
						// BIG NOTE: We are disabling "greedy eviction" for
						// districtNextOrderId lock.
						// We are doing this by making the update to lock in lock-table as
						// ONLY_UNLOCK
						// instead of ONLY_DELETE. This is what will ideally be done by the
						// clustering
						// process on the lock-table. The process won't evict this lock as
						// it will
						// be used frequently. It could evict the other locks, which are
						// mostly
						// for stock table, and also are infrequently re-used. Now, this
						// change here
						// is TPCC specific. We can't be doing this change in
						// WALManagerDistTxnClient
						// as its a very generic class. We need to figure out another way of
						// doing this.
						// Ideally, it should be done by the clustering process on the
						// lock-table.
						if (Bytes.toString(logId.getKey()).startsWith(
								TPCCTableProperties.districtWALPrefix)) {
							//sysout(trxId, "Found the district log Id: "
							//		+ Bytes.toString(logId.getKey()));
							//sysout(trxId, "GIVING ONLY_UNLOCK attribute for destLogId: "
							//		+ destLogId.toString());
							destLogId.setCommitType(LogId.ONLY_UNLOCK);
						}

						// BIGNOTE: We never delete migration; always simply unlock.
						destLogId.setCommitType(LogId.ONLY_UNLOCK);
						destLogIds.add(destLogId);
					} else {
						sysout(trxId, "In abort, destLogId is null!");
						logId.setCommitType(LogId.ONLY_UNLOCK);
						destLogIds.add(logId);
					}
				}
			}

			allKeysToBeUnlocked.add(keysToBeUnlocked);
			toBeUnlockedDestLogIds.add(destLogIds);
		}

		// Instead of sending it to a particular server-side coprocessor, we send
		// the Call
		// to all of them. They choose to act if the list of LogIds have any that
		// belong to them.
		// TODO: In this setting, regions which do not have any logIds will also do
		// work.
		// However, the present table.coprocessorExec API does not have a way to
		// specify only
		// particular regions to which the Exec should be sent to.
		AbortCallBack abortCallBack = new AbortCallBack();
		logTable.coprocessorExec(WALManagerDistTxnProtocol.class,
				HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
				new Batch.Call<WALManagerDistTxnProtocol, Boolean>() {
					@Override
					public Boolean call(WALManagerDistTxnProtocol instance)
							throws IOException {
						return instance.abortWithoutShadows(transactionState
								.getTransactionId(), logIdList, allKeysToBeUnlocked,
								toBeUnlockedDestLogIds);
					}
				}, abortCallBack);
		allAborted = allAborted && abortCallBack.getResult();
		return allAborted;
	}

	void abort(final HTable table, final DistTxnState transactionState)
			throws Throwable {
		final long trxId = transactionState.getTransactionId();
		// Abort would be called if the version check failed or due to some network
		// failure.
		// TODO: We need to make sure that region split/movement does not lead to
		// aborts.
		// Tasks to be performed in abort:
		// 1. We need to delete the shadow objects
		// 2. Release the locks: since we abort pretty soon after locking, the state
		// of the locks
		// would most probably be at the snapshot. Thus, releasing the locks should
		// also happen
		// at the snapshot through local transactions.
		List<Put> putShadowList = transactionState.getShadowPuts();
		List<Delete> deleteShadowList = new ArrayList<Delete>(putShadowList.size());
		for (Put p : putShadowList) {
			Delete d = new Delete(p.getRow());
			deleteShadowList.add(d);
		}
		table.batch(deleteShadowList);
		table.flushCommits();

		// Release the locks through local transactions.
		// Similar to acquiring the locks, except the zero would be written in the
		// writeLockColumn
		// instead of the transactionId.
		TreeMap<LogId, TreeSet<Action>> writeBuffer = transactionState
				.getWriteBufferWithLogIds();
		TreeMap<HRegionLocation, List<LogId>> regionToLogIdMap = transactionState
				.getRegionToLogIdMap();

		// For releasing locks, we can traverse the logs in any order. For now, we
		// are just
		// sticking to the order present in the structure. Furthermore, all the
		// unlock
		// requests for
		// a single HRegion (grouped by their respective LogIds) are sent to the
		// corresponding
		// coprocessor at once. Once we have the functionality of batching
		// coprocessor requests,
		// we can remove some of our own region-based batching.
		long nbUnsuccessfulAttempts = 0;
		Iterator<HRegionLocation> regionLocationItr = regionToLogIdMap
				.navigableKeySet().iterator();
		while (regionLocationItr.hasNext()) {
			HRegionLocation location = regionLocationItr.next();
			final List<LogId> logIdList = regionToLogIdMap.get(location);
			// No sorting of logIds needed.
			final List<List<ImmutableBytesWritable>> keysToBeUnlocked = new LinkedList<List<ImmutableBytesWritable>>();

			for (Iterator<LogId> logIdItr = logIdList.iterator(); logIdItr.hasNext();) {
				LogId logId = logIdItr.next();
				TreeSet<Action> actions = writeBuffer.get(logId);
				final List<ImmutableBytesWritable> keys = new LinkedList<ImmutableBytesWritable>();
				for (Action action : actions) {
					sysout(trxId, "For logId: " + logId.toString() + ", adding key: "
							+ Bytes.toString(action.getAction().getRow()));
					keys.add(new ImmutableBytesWritable(action.getAction().getRow()));
				}

				keysToBeUnlocked.add(keys);
			}
			// We can save RPC calls by batching the Puts by region.
			// The unlocking happens through local transactions on the logIds.
			// However, some more effort is needed to make WALs maintain
			// consistent lock information. Furthermore, since we are grouping lock
			// requests by the
			// region their respective WALs reside in. Thus, the optimization of
			// placing WALs on a single node gives performance.
			class ReleaseLockFromWALCallBack implements Batch.Callback<Long> {
				private long nbUnsuccessfulAttempts = 0;

				public long getNbUnsuccessfulAttempts() {
					return nbUnsuccessfulAttempts;
				}

				@Override
				public synchronized void update(byte[] region, byte[] row, Long result) {
					// Since this coprocessorExec call will only go to one region
					// hosting the
					// keys present in that list, there will be only one Call
					this.nbUnsuccessfulAttempts += result;

					sysout(trxId, "In ReleaseLockCallBack, for row: "
							+ Bytes.toString(row) + ", nbUnsuccessfulAttempts is: "
							+ this.nbUnsuccessfulAttempts);
				}
			}

			LogId sampleLogId = logIdList.get(0);
			ReleaseLockFromWALCallBack unlockCallBack = new ReleaseLockFromWALCallBack();
			table.coprocessorExec(WALManagerDistTxnProtocol.class, sampleLogId
					.getKey(), sampleLogId.getKey(),
					new Batch.Call<WALManagerDistTxnProtocol, Long>() {
						@Override
						public Long call(WALManagerDistTxnProtocol instance)
								throws IOException {
							return instance.abortRequestReleaseLocksFromWAL(transactionState
									.getTransactionId(), logIdList, keysToBeUnlocked);
						}
					}, unlockCallBack);
			nbUnsuccessfulAttempts += unlockCallBack.getNbUnsuccessfulAttempts();
		}
		// Since waiting happens at the server-side, there cannot be a false as
		// return value.
		// return nbUnsuccessfulAttempts;
	}
}
