package ham.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class TPCCLocalTrxExecutorClusteredPartitioning extends
		TPCCTablePropertiesClusteredPartitioning implements
		Callable<DistTrxExecutorReturnVal> {
	String[] tokens = null;
	HTableInterface dataTable = null;
	HTableInterface logTable = null;
	int thinkingTime;
	int lenOfTrx;
	long trxId;
	int contentionOrder;
	int numOfLockGroups;
	WALManagerDistTxnClient walManagerDistTxnClient = null;
	boolean migrateLocks = false;

	// Class variables for the transaction.
	long homeWarehouseId;
	long districtId;
	long customerId;

	public TPCCLocalTrxExecutorClusteredPartitioning(String[] tokens,
			HTable dataTable, HTable logTable,
			WALManagerDistTxnClient walManagerDistTxnClient, int thinkingTime,
			int lenOfTrx, int contentionOrder, boolean migrateLocks)
			throws IOException {
		this.tokens = tokens;
		this.dataTable = dataTable;
		this.logTable = logTable;
		this.thinkingTime = thinkingTime;
		this.lenOfTrx = lenOfTrx;
		this.contentionOrder = contentionOrder;
		this.migrateLocks = migrateLocks;
		this.walManagerDistTxnClient = walManagerDistTxnClient;
	}

	public static void sysout(long trxId, String otp) {
		//System.out.println(trxId + " : " + otp);
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

	List<Get> parseInputTokensAndIssueReadSet() {
		List<Get> gets = new ArrayList<Get>();
		// 1st element in the tokens list will be "HomeWarehouse"
		this.homeWarehouseId = Long.parseLong(tokens[0].split(":")[0]);
		String homeWarehouseKey = createWarehouseTableKey(this.homeWarehouseId);

		Get g = new Get(Bytes.toBytes(homeWarehouseKey));
		g.addColumn(dataFamily, warehouseTaxRateColumn);
		g.addColumn(dataFamily, versionColumn);
		g.addColumn(WAL_FAMILY, regionObserverMarkerColumn);
		g.setTimeStamp(appTimestamp);
		gets.add(g);

		// 2nd element in the tokens list will be "DistrictId"
		this.districtId = Long.parseLong(tokens[1].split(":")[1]);
		String districtKey = createDistrictTableKey(this.homeWarehouseId,
				this.districtId);
		g = new Get(Bytes.toBytes(districtKey));
		g.addColumn(dataFamily, districtTaxRateColumn);
		g.addColumn(dataFamily, districtNextOrderIdColumn);
		g.addColumn(dataFamily, versionColumn);
		g.addColumn(WAL_FAMILY, regionObserverMarkerColumn);
		g.setTimeStamp(appTimestamp);
		gets.add(g);

		// 3rd element in the tokens list will be "CustomerId"
		// BIGNOTE: Customer keys should start with district and then warehouse, so
		// that
		// the requests are load balanced on the servers. However, the generated trx
		// files
		// have it in the reverse order. Until that is fixed, we don't issue a Get
		// for
		// customer info and correspondingly ignore the customerDiscount.

		this.customerId = Long.parseLong(tokens[2].split(":")[2]);
		// g = new Get(customerKey);
		// g.addColumn(dataFamily, customerDiscountColumn);
		// g.addColumn(dataFamily, customerLastNameColumn);
		// g.addColumn(dataFamily, customerCreditColumn);
		// g.addColumn(dataFamily, versionColumn);
		// g.addColumn(WAL_FAMILY, regionObserverMarkerColumn);
		// g.setTimestamp(appTimestamp);
		// gets.add(g);

		// From 4th element in the tokens list, every alternate element points to an
		// item id, and its next element denotes the warehouse from which it needs
		// to be
		// bought.
		for (int i = 3; i < tokens.length; i = i + 2) {
			Long itemId = Long.parseLong(tokens[i]);
			Long correspWarehouse = Long.parseLong(tokens[i + 1]);
			// Prepare a Get for the item from Item table (we need its price).
			String itemKey = createItemTableKey(itemId);
			g = new Get(Bytes.toBytes(itemKey));
			g.addColumn(dataFamily, itemPriceColumn);
			g.addColumn(dataFamily, itemNameColumn);
			g.addColumn(dataFamily, versionColumn);
			g.addColumn(WAL_FAMILY, regionObserverMarkerColumn);
			g.setTimeStamp(appTimestamp);
			gets.add(g);

			// Prepare a Get to fetch information from Stock table for this item.
			String keyForStockTable = createStockTableKey(correspWarehouse, itemId);
			sysout(trxId, "Sending request for this stock key: " + keyForStockTable);
			g = new Get(Bytes.toBytes(keyForStockTable));
			g.addColumn(dataFamily, stockQuantityColumn);
			g.addColumn(dataFamily, versionColumn);
			g.addColumn(WAL_FAMILY, regionObserverMarkerColumn);
			g.setTimeStamp(appTimestamp);
			gets.add(g);
		}
		return gets;
	}

	public DistTrxExecutorReturnVal call() {
		DistTrxExecutorReturnVal retVal = null;
		try {
			DistTxnState transactionState = walManagerDistTxnClient
					.beginTransaction();
			this.trxId = transactionState.getTransactionId();
			int countOfAborts = 0;
			
			Long itemId = Long.parseLong(tokens[0]);
			Long correspWarehouse = Long.parseLong(tokens[1]);

			// Prepare a Put to update stock information for this item in the Stock table.
			String keyForStockTable = createStockTableKey(correspWarehouse, itemId);
			sysout(trxId, "Sending request for this stock key: " + keyForStockTable);
			// For now, we don't update any stock value, as we don't have a checkAndIncrement
			// operation on the server side. We simply check for absence of lock on that row, 
			// "virtually" lock the row, and then practically unlock the row by placing a zero in it
			// again. Since the operation happens as a critical section, we won't have race conditions.
			// For every data object, there is a lock object (denoted by LogId#dataId).
			// Since we aren't really dealing with the data object, we just check for lock, place lock
			// and unlock, all on the lock object.
			LogId logId = getLogIdForKey(Bytes.toBytes(keyForStockTable));
			byte[] finalKeyForStockTable = Bytes.toBytes(Bytes.toString(logId.getKey())
					+ logAndKeySeparator + keyForStockTable);
			Put put = new Put(finalKeyForStockTable);
			put.add(WALTableProperties.WAL_FAMILY, writeLockColumn, appTimestamp, Bytes.toBytes(zero));
			put.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.regionObserverMarkerColumn, appTimestamp,
					WALTableProperties.randomValue);
			put.setWriteToWAL(true);
			
			long startCommitTime = System.currentTimeMillis();
			boolean commitResponse = walManagerDistTxnClient.checkAndPut(dataTable, logTable,
					finalKeyForStockTable, WALTableProperties.WAL_FAMILY,
					writeLockColumn, Bytes.toBytes(zero), put);
			if (!commitResponse) {
				// We abort here.
				countOfAborts++;
				return new DistTrxExecutorReturnVal(tokens, countOfAborts,
						tokens.length);
			}
			long endCommitTime = System.currentTimeMillis();
			long commitTime = (endCommitTime - startCommitTime);
			
			retVal = new DistTrxExecutorReturnVal(null, 0, tokens.length);
			retVal.setNumOflogsAcquired(tokens.length);
			retVal.setCommitTime(commitTime);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retVal;
	}
}
