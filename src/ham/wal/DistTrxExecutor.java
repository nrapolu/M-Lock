package ham.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class DistTrxExecutor implements Callable<DistTrxExecutorReturnVal> {
	String[] tokens = null;
	HTablePool tablePool = null;
	HTable dataTable = null;
	HTable logTable = null;
	int thinkingTime;
	int lenOfTrx;
	int id;
	int contentionOrder;
	int numOfLockGroups;
	WALManagerDistTxnClient walManagerDistTxnClient = null;
	boolean migrateLocks = false;

	byte[] dataFamily = WALTableProperties.dataFamily;
	byte[] dataColumn = WALTableProperties.dataColumn;
	byte[] versionColumn = WALTableProperties.versionColumn;
	byte[] writeLockColumn = WALTableProperties.writeLockColumn;
	byte[] blobColumn = WALTableProperties.blobColumn;

	public DistTrxExecutor(String[] tokens, byte[] dataTableName,
			byte[] logTableName, HTablePool tablePool,
			WALManagerDistTxnClient walManagerDistTxnClient, int thinkingTime,
			int lenOfTrx, int contentionOrder, boolean migrateLocks)
			throws IOException {
		this.tokens = tokens;
		this.tablePool = tablePool;
		this.dataTable = (HTable) this.tablePool.getTable(dataTableName);
		this.logTable = (HTable) this.tablePool.getTable(logTableName);
		this.thinkingTime = thinkingTime;
		this.lenOfTrx = lenOfTrx;
		this.contentionOrder = contentionOrder;
		this.walManagerDistTxnClient = walManagerDistTxnClient;
		this.migrateLocks = migrateLocks;
	}

	public static Map<String, List<String>> getLogsToDataKeysMap(String[] keys) {
		Map<String, List<String>> logsToDataKeys = new HashMap<String, List<String>>();
		for (int i = 0; i < keys.length; i++) {
			String[] keySplits = keys[i].split("["
					+ WALTableProperties.logAndKeySeparator + "]+");
			List<String> dataKeys = logsToDataKeys.get(keySplits[0]);
			if (dataKeys == null) {
				dataKeys = new LinkedList<String>();
				logsToDataKeys.put(keySplits[0], dataKeys);
			}

			dataKeys.add(keySplits[1]);
		}

		return logsToDataKeys;
	}

	public DistTrxExecutorReturnVal call() {
		DistTrxExecutorReturnVal retVal = null;
		try {
			// For all tokens (inventory keys), find out the warehouses they are
			// hosted by.
			// Since each warehouse is taken care of by a WAL, if all keys belong to
			// a single
			// warehouse, then its a local transaction.
			Map<String, List<String>> logsToDataKeysMap = getLogsToDataKeysMap(tokens);
			DistTxnState transactionState = walManagerDistTxnClient
					.beginTransaction();
			int countOfAborts = 0;
			List<Get> gets = new ArrayList<Get>();
			for (Map.Entry<String, List<String>> entry : logsToDataKeysMap.entrySet()) {
				// Execute the local transaction on the logId.
				LogId logId = new LogId();
				logId.setKey(Bytes.toBytes(entry.getKey()));
				// In the case of distributed transactions, the WALManagerDistTxnClient
				// takes care of
				// caching reads and maintaining read-write-sets. This function just
				// needs to issue
				// get/put requests to the client. On the other hand, WALManagerClient
				// does not offer
				// much help, the API does not offer to maintain and track
				// read-write-sets and snapshots.
				for (String itemKey : entry.getValue()) {
					// Read the item from the datastore and increment the stock count.
					Get g = new Get(Bytes.toBytes(itemKey));
					g.addColumn(dataFamily, dataColumn);
					g.addColumn(dataFamily, versionColumn);
					g.addColumn(dataFamily, writeLockColumn);
					g.addColumn(dataFamily, blobColumn);
					g.setTimeStamp(WALTableProperties.appTimestamp);
					gets.add(g);
				}
			}

			// Issuing the "gets" in a batch. The Get might be satisfied through the
			// snapshot
			// or through the data store.
			long startReadTime = System.currentTimeMillis();
			List<Result> results = walManagerDistTxnClient.getWithClientSideMerge(logTable, dataTable,
					transactionState, gets, migrateLocks);
			long endReadTime = System.currentTimeMillis();

			for (Result r : results) {
				String itemKey = Bytes.toString(r.getRow());
				long stockCount = Long.parseLong(Bytes.toString(r.getValue(dataFamily,
						dataColumn)));
				stockCount++;

				System.out.println("For itemKey: " + itemKey
						+ "Final to-be-stock-count is: " + stockCount);

				// Create a Put with the new stock count.
				Put p = new Put(Bytes.toBytes(itemKey));
				p.add(dataFamily, dataColumn, WALTableProperties.appTimestamp, Bytes
						.toBytes(Long.toString(stockCount)));
				walManagerDistTxnClient.put(dataTable, transactionState, p);
			}
			// Transaction commits happens in 3 phases:
			// 1. Place shadow puts in the store (remember to have different
			// transactions put shadow
			// objects in two locations even for the same key; this can be done by
			// making its key contain
			// the transactionId).
			// 2. Lock the writes
			// 3. Check staleness of reads.
			// 4. Commit the writes. (Functionality if there is an abort is
			// presently missing)
			boolean commitResponse = true;
			// There can't be aborts while placing shadow objects or acquiring locks.
			// We measure time and attempts for each.
			// For now, we take the direct update route rather than placing shadows
			// and
			// then copying.
			// walManagerDistTxnClient.putShadowObjects(dataTable, transactionState);
			long startLockingTime = System.currentTimeMillis();
			boolean acquireLocksResponse = walManagerDistTxnClient
					.commitRequestAcquireLocksViaIndirection(logTable, transactionState,
							migrateLocks);
			 long nbDetoursEncountered = transactionState.getNbDetoursEncountered();
			 long nbNetworkHopsInTotal = transactionState.getNbNetworkHopsInTotal();
			 long endLockingTime = System.currentTimeMillis();

			long startVersionCheckTime = System.currentTimeMillis();
			commitResponse = commitResponse
					&& walManagerDistTxnClient.commitRequestCheckVersionsFromWAL(
							logTable, dataTable, transactionState);
			long endVersionCheckTime = System.currentTimeMillis();

			System.out.println("CommitResponse after checkVersions: "
					+ commitResponse);
			if (!commitResponse) {
				// We abort here.
				countOfAborts++;
				walManagerDistTxnClient.abortWithoutShadows(logTable, transactionState);
				return new DistTrxExecutorReturnVal(tokens, countOfAborts,
						tokens.length);
			}

			long startCommitTime = System.currentTimeMillis();
			commitResponse = commitResponse
					&& walManagerDistTxnClient.commitWritesPerEntityGroupWithoutShadows(
							logTable, transactionState);
			System.out.println("CommitResponse after commitWritesPerEntityGroup: "
					+ commitResponse);
			if (!commitResponse) {
				// We abort here.
				countOfAborts++;
				return new DistTrxExecutorReturnVal(tokens, countOfAborts,
						tokens.length);
			}
			long endCommitTime = System.currentTimeMillis();
			long commitTime = (endCommitTime - startCommitTime);
			long readTime = (endReadTime - startReadTime);
			long lockingTime = (endLockingTime - startLockingTime);
			long versionCheckTime = (endVersionCheckTime - startVersionCheckTime);

			retVal = new DistTrxExecutorReturnVal(null, 0, tokens.length);
			retVal.setNumOflogsAcquired(tokens.length);
			retVal.setReadTime(readTime);
			retVal.setLockingTime(lockingTime);
			retVal.setVersionCheckTime(versionCheckTime);
			retVal.setCommitTime(commitTime);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				this.dataTable.close();
				this.logTable.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return retVal;
	}
}
