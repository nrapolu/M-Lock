package ham.wal;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalTrxExecutor implements Callable<LocalTrxExecutorReturnVal> {
	String[] tokens = null;
	Hashtable<String, String> logsToBeAcquired = new Hashtable<String, String>();
	Vector<String> logsToBeAcquiredAsAList = new Vector<String>();
	Configuration dataTableConfig = null;
	Configuration logTableConfig = null;
	HTable dataTable = null;
	HTable logTable = null;
	int thinkingTime;
	int lenOfTrx;
	byte[] dataFamily = WALTableProperties.dataFamily;
	byte[] dataColumn = WALTableProperties.dataColumn;
	
	public LocalTrxExecutor(String[] tokens, byte[] dataTableName,
			Configuration dataTableConfig, byte[] logTableName,
			Configuration logTableConfig, int thinkingTime, int lenOfTrx)
			throws IOException {
		this.tokens = tokens;
		this.dataTableConfig = dataTableConfig;
		this.logTableConfig = logTableConfig;
		this.dataTable = new HTable(dataTableConfig, dataTableName);
		this.logTable = new HTable(logTableConfig, logTableName);
		this.thinkingTime = thinkingTime;
		this.lenOfTrx = lenOfTrx;
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


	@Override
	public LocalTrxExecutorReturnVal call() throws Exception {
		LocalTrxExecutorReturnVal retVal = null;
		try {
			// For all tokens (inventory keys), find out the warehouses they are
			// hosted by.
			// Since each warehouse is taken care of by a WAL, if all keys belong to
			// a single
			// warehouse, then its a local transaction.
			Map<String, List<String>> logsToDataKeysMap = getLogsToDataKeysMap(tokens);

			// For now we don't consider distributed transactions spawning multiple
			// logs.
			// Assuming only one WAL id is present in the map.
			// TODO: the code which processes the results of the futures should be
			// changed
			// to take care of this.
			if (logsToDataKeysMap.size() > 1)
				return new LocalTrxExecutorReturnVal(0, 0, 0);

			WALManagerClient walManagerClient = new WALManagerClient();
			int countOfAborts = 0;
			long timeForCommits = 0;
			for (Map.Entry<String, List<String>> entry : logsToDataKeysMap
					.entrySet()) {
				// Execute the local transaction on the logId.
				LogId logId = new LogId();
				logId.setKey(Bytes.toBytes(entry.getKey()));
				// Grab the snapshot from WAL.
				Snapshot snapshot = walManagerClient.start(logTable, logId);
				System.out.println("Thread id: "
						+ ManagementFactory.getRuntimeMXBean().getName()
						+ ", Fetched a snapshot with currentTs: "
						+ snapshot.getTimestamp());
				Map<String, Write> snapshotWriteMap = snapshot.getWriteMap();
				// Final readSet to be "checked" and final writeSet to be committed.
				Map<byte[], Set<ImmutableBytesWritable>> readSets = new TreeMap<byte[], Set<ImmutableBytesWritable>>(
						Bytes.BYTES_COMPARATOR);
				List<Write> finalWrites = new LinkedList<Write>();

				// Read the inventory stock count from the data-store. If the snapshot
				// contains it,
				// we don't need to go to the data-store.
				for (String itemKey : entry.getValue()) {
					// Updating the read-set.
					byte[] itemName = Bytes.toBytes(Bytes.toString(dataTable.getTableName())
							+ Write.nameDelimiter + Bytes.toString(dataFamily)
							+ Write.nameDelimiter + Bytes.toString(dataColumn));
					Set<ImmutableBytesWritable> readSet = readSets.get(itemName);
					if (readSet == null) {
						readSet = new HashSet<ImmutableBytesWritable>();
						readSets.put(itemName, readSet);
					}
					readSet.add(new ImmutableBytesWritable(Bytes.toBytes(itemKey)));

					Write w = new Write();
					w.setName(itemName);
					w.setKey(Bytes.toBytes(itemKey));

					if (!snapshot.isEmpty()) {
						// If present, read the item from the snapshot and update its
						// value.
						Write writeFromSnapshot = null;
						if ((writeFromSnapshot = snapshotWriteMap.get(w.getNameAndKey())) != null) {
							Long stockCount = Long.parseLong(Bytes
									.toString(writeFromSnapshot.getValue()));
							stockCount++;
							writeFromSnapshot.setValue(Bytes.toBytes(Long
									.toString(stockCount)));
							finalWrites.add(writeFromSnapshot);
							System.out
									.println("Final to-be-committed Write using data from "
											+ "snapshot: " + writeFromSnapshot.toString());
							continue;
						}
					}

					// Read the item from the datastore and increment the stock count.
					Get g = new Get(Bytes.toBytes(itemKey));
					g.addColumn(dataFamily, dataColumn);
					g.setTimeStamp(WALTableProperties.appTimestamp);
					Result r = dataTable.get(g);
					long stockCount = Long.parseLong(Bytes.toString(r.getValue(
							dataFamily, dataColumn)));
					stockCount++;
					w.setValue(Bytes.toBytes(Long.toString(stockCount)));
					finalWrites.add(w);
					System.out.println("Final to-be-committed Write using data from "
							+ "store: " + w.toString());
				}

				// Prepare a check object and commit the write-set with optimistic
				// checks.
				Check check = new Check();
				check.setTimestamp(snapshot.getTimestamp());
				List<ReadSet> readSetsList = new LinkedList<ReadSet>();
				for (Map.Entry<byte[], Set<ImmutableBytesWritable>> readSetEntry : readSets
						.entrySet()) {
					ReadSet r = new ReadSet();
					r.setName(readSetEntry.getKey());
					r.setKeys(readSetEntry.getValue());
					readSetsList.add(r);
				}
				check.setReadSets(readSetsList);
				System.out.println("Thread id: "
						+ ManagementFactory.getRuntimeMXBean().getName()
						+ ", Check object being sent: " + check.toString());

				long startCommitTime = System.currentTimeMillis();
				// Commit the check object and the write-sets.
				boolean commitResponse;
				commitResponse = walManagerClient.commit(logTable, logId, check,
						finalWrites, null, null);
				System.out.println("Thread id: "
						+ ManagementFactory.getRuntimeMXBean().getName()
						+ ", Commit response: " + commitResponse + ", for this check: "
						+ check.toString());
				long endCommitTime = System.currentTimeMillis();

				timeForCommits += (endCommitTime - startCommitTime);

				if (!commitResponse) {
					// We abort here.
					countOfAborts++;
					return new LocalTrxExecutorReturnVal(tokens, countOfAborts,
							tokens.length);
				}
			}

			retVal = new LocalTrxExecutorReturnVal(countOfAborts, tokens.length,
					timeForCommits);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retVal;
	}
}

class LocalTrxExecutorReturnVal {
	public long abortCount;
	public long numOflogsAcquired;
	public long logingTime = 0;
	public String[] tokens = null;

	public LocalTrxExecutorReturnVal(long abortCount, long numOflogsAcquired) {
		this.abortCount = abortCount;
		this.numOflogsAcquired = numOflogsAcquired;
	}

	public LocalTrxExecutorReturnVal(long abortCount, long numOflogsAcquired,
			long logingTime) {
		this.abortCount = abortCount;
		this.numOflogsAcquired = numOflogsAcquired;
		this.logingTime = logingTime;
	}

	public LocalTrxExecutorReturnVal(String[] tokens, long abortCount,
			long numOflogsAcquired) {
		this.tokens = tokens;
		this.abortCount = abortCount;
		this.numOflogsAcquired = numOflogsAcquired;
	}

	public void setTokens(String[] tokens) {
		this.tokens = tokens;
	}
}

