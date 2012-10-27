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
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class TPCCNewOrderTrxExecutor extends TPCCTableProperties implements
		Callable<DistTrxExecutorReturnVal> {
	String[] tokens = null;
	HTablePool tablePool = null;
	HTable dataTable = null;
	HTable logTable = null;
	int thinkingTime;
	int lenOfTrx;
	long trxId;
	int contentionOrder;
	int numOfLockGroups;
	WALManagerDistTxnClient walManagerDistTxnClient = null;
	boolean migrateLocks = false;

	// Class variables for the transaction.
	byte[] homeWarehouseId = null;
	byte[] districtId = null;
	byte[] customerId = null;

	public TPCCNewOrderTrxExecutor(String[] tokens, byte[] dataTableName,
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

	public static void sysout(long trxId, String otp) {
		// System.out.println(trxId + " : " + otp);
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
		byte[] homeWarehouseKey = Bytes.toBytes(tokens[0]);
		this.homeWarehouseId = Bytes.toBytes(tokens[0].split(":")[0]);
		Get g = new Get(homeWarehouseKey);
		g.addColumn(dataFamily, warehouseTaxRateColumn);
		g.addColumn(dataFamily, versionColumn);
		gets.add(g);

		// 2nd element in the tokens list will be "DistrictId"
		byte[] districtKey = Bytes.toBytes(districtWALPrefix + tokens[1]);
		this.districtId = Bytes.toBytes(tokens[1].split(":")[1]);
		g = new Get(districtKey);
		g.addColumn(dataFamily, districtTaxRateColumn);
		g.addColumn(dataFamily, districtNextOrderIdColumn);
		g.addColumn(dataFamily, versionColumn);
		gets.add(g);

		// 3rd element in the tokens list will be "CustomerId"
		byte[] customerKey = Bytes.toBytes(tokens[2]);
		this.customerId = Bytes.toBytes(tokens[2].split(":")[2]);
		g = new Get(customerKey);
		g.addColumn(dataFamily, customerDiscountColumn);
		g.addColumn(dataFamily, customerLastNameColumn);
		g.addColumn(dataFamily, customerCreditColumn);
		g.addColumn(dataFamily, versionColumn);
		gets.add(g);

		// From 4th element in the tokens list, every alternate element points to an
		// item id, and its next element denotes the warehouse from which it needs
		// to be
		// bought.
		for (int i = 3; i < tokens.length; i = i + 2) {
			String itemId = tokens[i];
			String correspWarehouse = tokens[i + 1];
			// Prepare a Get for the item from Item table (we need its price).
			g = new Get(Bytes.toBytes(itemId + ":" + "item"));
			g.addColumn(dataFamily, itemPriceColumn);
			g.addColumn(dataFamily, itemNameColumn);
			g.addColumn(dataFamily, versionColumn);
			gets.add(g);

			// Prepare a Get to fetch information from Stock table for this item.
			String keyForStockTable = itemId + ":" + correspWarehouse + ":" + "stock";
			sysout(trxId, "Sending request for this stock key: " + keyForStockTable);
			g = new Get(Bytes.toBytes(keyForStockTable));
			g.addColumn(dataFamily, stockQuantityColumn);
			g.addColumn(dataFamily, versionColumn);
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
			List<Get> gets = new ArrayList<Get>();
			gets = parseInputTokensAndIssueReadSet();

			// In the case of distributed transactions, the WALManagerDistTxnClient
			// takes care of
			// caching reads and maintaining read-write-sets. This function just
			// needs to issue
			// get/put requests to the client. On the other hand, WALManagerClient
			// does not offer
			// much help, the API does not offer to maintain and track
			// read-write-sets and snapshots.

			// Issuing the "gets" in a batch. The Get might be satisfied through the
			// snapshot
			// or through the data store.
			long startReadTime = System.currentTimeMillis();
			List<Result> results = walManagerDistTxnClient.getWithServerSideMerge(logTable,
					dataTable, transactionState, gets);
			long endReadTime = System.currentTimeMillis();

			// Grab the information we need from the results.
			Result r = results.get(0);
			int warehouseTaxRate = Integer.parseInt(Bytes.toString(r.getValue(
					dataFamily, warehouseTaxRateColumn)));

			r = results.get(1);
			long districtNextOrderId = Long.parseLong(Bytes.toString(r.getValue(
					dataFamily, districtNextOrderIdColumn)));
			// int districtTaxRate = Integer.parseInt(Bytes.toString(r.getValue(
			// dataFamily, districtTaxRateColumn)));

			r = results.get(2);
			int customerDiscount = Integer.parseInt(Bytes.toString(r.getValue(
					dataFamily, customerDiscountColumn)));

			// Create a Put to increase the districtNextOrder.
			String districtKey = districtWALPrefix + Bytes.toString(homeWarehouseId) + ":"
					+ Bytes.toString(districtId) + ":" + "district";

			Put updatedDistrictNextOrderId = new Put(Bytes.toBytes(districtKey));
			updatedDistrictNextOrderId.add(dataFamily, districtNextOrderIdColumn,
					appTimestamp, Bytes.toBytes(Long.toString(districtNextOrderId + 1)));
			updatedDistrictNextOrderId.add(dataFamily, versionColumn, appTimestamp,
					Bytes.toBytes(Long.toString(trxId)));
			walManagerDistTxnClient.put(dataTable, transactionState,
					updatedDistrictNextOrderId);

			// Create a Put to enter info into the Order table.
			String orderKey = orderWALPrefix + Bytes.toString(homeWarehouseId) + ":"
					+ Bytes.toString(districtId) + ":"
					+ Long.toString(districtNextOrderId) + ":" + "order"
					+ WALTableProperties.logAndKeySeparator
					+ Bytes.toString(homeWarehouseId) + ":" + Bytes.toString(districtId)
					+ ":" + Long.toString(districtNextOrderId) + ":" + "order";
			Put order = new Put(Bytes.toBytes(orderKey));
			order.add(dataFamily, orderIdColumn, appTimestamp, Bytes.toBytes(Long
					.toString(districtNextOrderId)));
			order.add(dataFamily, orderDistrictIdColumn, appTimestamp, districtId);
			order.add(dataFamily, orderWarehouseIdColumn, appTimestamp,
					homeWarehouseId);
			order.add(dataFamily, orderCustomerIdColumn, appTimestamp, customerId);
			order.add(dataFamily, orderOrderLineCountColumn, appTimestamp, Bytes
					.toBytes("" + 10));
			order.add(dataFamily, orderAllLocalColumn, appTimestamp, Bytes
					.toBytes("" + 1));
			order.add(dataFamily, versionColumn, appTimestamp, Bytes.toBytes(Long
					.toString(trxId)));
			walManagerDistTxnClient.put(dataTable, transactionState, order);

			// Create a Put to add info into the NewOrder table.
			String newOrderKey = orderWALPrefix + Bytes.toString(homeWarehouseId) + ":"
					+ Bytes.toString(districtId) + ":"
					+ Long.toString(districtNextOrderId) + ":" + "order"
					+ WALTableProperties.logAndKeySeparator
					+ Bytes.toString(homeWarehouseId) + ":" + Bytes.toString(districtId)
					+ ":" + Long.toString(districtNextOrderId) + ":" + "newOrder";
			Put newOrder = new Put(Bytes.toBytes(orderKey));
			newOrder.add(dataFamily, newOrderIdColumn, appTimestamp, Bytes
					.toBytes(Long.toString(districtNextOrderId)));
			newOrder.add(dataFamily, newOrderDistrictIdColumn, appTimestamp,
					districtId);
			newOrder.add(dataFamily, newOrderWarehouseIdColumn, appTimestamp,
					homeWarehouseId);
			newOrder.add(dataFamily, versionColumn, appTimestamp, Bytes.toBytes(Long
					.toString(trxId)));
			walManagerDistTxnClient.put(dataTable, transactionState, newOrder);

			for (int i = 3; i < results.size(); i = i + 2) {
				long orderLineAmount;
				Result itemResult = results.get(i);
				String itemKey = Bytes.toString(itemResult.getRow());
				long itemPrice = Long.parseLong(Bytes.toString(itemResult.getValue(
						dataFamily, itemPriceColumn)));
				// We assume that only one piece of each item is being purchased.
				// (OL_QUANTITY = 1).
				orderLineAmount = itemPrice;

				Result stockResult = results.get(i + 1);
				String itemStockKey = Bytes.toString(stockResult.getRow());
				sysout(trxId, "Retrieving result for this stock key: " + itemStockKey);
				long stockCount = Long.parseLong(Bytes.toString(stockResult.getValue(
						dataFamily, stockQuantityColumn)));
				stockCount++;

				//System.out.println("For itemKey: " + itemStockKey
				//		+ "Final to-be-stock-count is: " + stockCount);

				// Create a Put with the new stock count.
				Put p = new Put(Bytes.toBytes(itemStockKey));
				p.add(dataFamily, stockQuantityColumn, WALTableProperties.appTimestamp,
						Bytes.toBytes(Long.toString(stockCount)));
				p.add(dataFamily, versionColumn, appTimestamp, Bytes.toBytes(Long
						.toString(trxId)));
				walManagerDistTxnClient.put(dataTable, transactionState, p);

				// Create a Put to update the orderline entry for this item.
				String newOrderLineKey = orderWALPrefix + Bytes.toString(homeWarehouseId) + ":"
						+ Bytes.toString(districtId) + ":"
						+ Long.toString(districtNextOrderId) + ":" + "order"
						+ WALTableProperties.logAndKeySeparator
						+ Bytes.toString(homeWarehouseId) + ":"
						+ Bytes.toString(districtId) + ":"
						+ Long.toString(districtNextOrderId) + ":" + itemKey + ":"
						+ "orderLine";

				Put orderLineEntry = new Put(Bytes.toBytes(newOrderLineKey));
				orderLineEntry.add(dataFamily, orderLineOrderIdColumn, appTimestamp,
						Bytes.toBytes(Long.toString(districtNextOrderId)));
				orderLineEntry.add(dataFamily, orderLineDistrictIdColumn, appTimestamp,
						districtId);
				orderLineEntry.add(dataFamily, orderLineWarehouseIdColumn,
						appTimestamp, homeWarehouseId);
				orderLineEntry.add(dataFamily, orderLineNumberColumn, appTimestamp,
						Bytes.toBytes(itemKey));
				orderLineEntry.add(dataFamily, orderLineItemIdColumn, appTimestamp,
						Bytes.toBytes(itemKey));
				// TODO: writing homeWarehouse as the supplying warehouse is an
				// approximation.
				orderLineEntry.add(dataFamily, orderLineSupplyWarehouseIdColumn,
						appTimestamp, homeWarehouseId);
				orderLineEntry.add(dataFamily, orderLineQuantityColumn, appTimestamp,
						Bytes.toBytes("" + 1));
				orderLineEntry.add(dataFamily, orderLineAmountColumn, appTimestamp,
						Bytes.toBytes("" + itemPrice));
				orderLineEntry.add(dataFamily, versionColumn, appTimestamp, Bytes
						.toBytes(Long.toString(trxId)));
				walManagerDistTxnClient
						.put(dataTable, transactionState, orderLineEntry);
			}
			// Transaction commits happens in 3 phases:
			// 1. Place shadow puts in the store (remember to have different
			// transactions put shadow
			// objects in two locations even for the same key; this can be done by
			// making its key contain
			// the transactionId).
			// 2. Record the transaction by persisting the DistTxnState object.
			// 3. Lock the writes
			// 4. Check staleness of reads.
			// 5. Commit the writes.
			boolean commitResponse = true;
			// There can't be aborts while placing shadow objects or acquiring locks.
			// We measure time and attempts for each.
			long startPutShadowTime = System.currentTimeMillis();
			//System.out.println("MigrateLocks request status: " + migrateLocks);
			walManagerDistTxnClient.putShadowObjects(logTable, dataTable,
					transactionState, migrateLocks, Bytes.toString(homeWarehouseId));
			long endPutShadowTime = System.currentTimeMillis();

			long startPutTxnStateTime = System.currentTimeMillis();
			walManagerDistTxnClient.putDistTxnState(logTable, transactionState, false);
			long endPutTxnStateTime = System.currentTimeMillis();

			long startLockingTime = System.currentTimeMillis();
			boolean acquireLocksResponse = walManagerDistTxnClient
					.commitRequestAcquireLocksViaIndirection(logTable, transactionState,
							migrateLocks);
			long endLockingTime = System.currentTimeMillis();
			long nbDetoursEncountered = transactionState.getNbDetoursEncountered();
			long nbNetworkHopsInTotal = transactionState.getNbNetworkHopsInTotal();
			long lockMigrationTime = transactionState.getLockMigrationTime();

			long startVersionCheckTime = System.currentTimeMillis();
			commitResponse = commitResponse
					&& walManagerDistTxnClient.commitRequestCheckVersionsFromWAL(
							logTable, dataTable, transactionState);
			long endVersionCheckTime = System.currentTimeMillis();

			//System.out.println("CommitResponse after checkVersions: "
			//		+ commitResponse);
			if (!commitResponse) {
				// We abort here.
				countOfAborts++;
				walManagerDistTxnClient.abortWithoutShadows(logTable, transactionState);
				return new DistTrxExecutorReturnVal(tokens, countOfAborts,
						tokens.length);
			}

			long startCommitTime = System.currentTimeMillis();
			commitResponse = commitResponse
					&& walManagerDistTxnClient.commitWritesPerEntityGroupWithShadows(
							logTable, transactionState);
			//System.out.println("CommitResponse after commitWritesPerEntityGroup: "
			//		+ commitResponse);
			if (!commitResponse) {
				// We abort here.
				countOfAborts++;
				return new DistTrxExecutorReturnVal(tokens, countOfAborts,
						tokens.length);
			}
			long endCommitTime = System.currentTimeMillis();
			long commitTime = (endCommitTime - startCommitTime);
			long readTime = (endReadTime - startReadTime);
			long putShadowTime = (endPutShadowTime - startPutShadowTime);
			long putTxnStateTime = (endPutTxnStateTime - startPutTxnStateTime);
			long lockingTime = (endLockingTime - startLockingTime);
			long versionCheckTime = (endVersionCheckTime - startVersionCheckTime);

			retVal = new DistTrxExecutorReturnVal(null, 0, tokens.length);
			retVal.setNumOflogsAcquired(tokens.length);
			retVal.setNbDetoursEncountered(nbDetoursEncountered);
			retVal.setNbNetworkRoundTripsInTotalForLocking(nbNetworkHopsInTotal);
			retVal.setLockMigrationTime(lockMigrationTime);
			retVal.setReadTime(readTime);
			retVal.setPutShadowTime(putShadowTime);
			retVal.setPutTxnStateTime(putTxnStateTime);
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
