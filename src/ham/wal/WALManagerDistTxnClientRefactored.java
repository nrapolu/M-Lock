package ham.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class WALManagerDistTxnClientRefactored extends WALManagerDistTxnClient {

	public WALManagerDistTxnClientRefactored() throws IOException {
		super();
		// TODO Auto-generated constructor stub
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
			sysout(trxId, "Present version for key: " + Bytes.toString(r.getRow())
					+ ", is: " + presentVersion);
			sysout(trxId, "Cache version is: " + cacheVersion);
			if (cacheVersion != presentVersion) {
				isFresh = isFresh && false;
			}
		}
		return isFresh;
	}	
}
