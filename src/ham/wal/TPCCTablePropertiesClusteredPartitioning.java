package ham.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TPCCTablePropertiesClusteredPartitioning extends
		TPCCTableProperties {
	private static boolean isStockTableRangePartitioned = true;
	static HashMap<Long, String> warehouseKeyMap = new HashMap<Long, String>();
	static HashMap<Long, String> toBePrependedStringMapForMigratedLocks = new HashMap<Long, String>();

	{
		// Also populate the hashMap in this class which maps the warehouse id to
		// the actual key
		// to be used.
		warehouseKeyMap.put(new Long(1), "0001");
		warehouseKeyMap.put(new Long(2), "0502");
		warehouseKeyMap.put(new Long(3), "1003");
		warehouseKeyMap.put(new Long(4), "1504");
		warehouseKeyMap.put(new Long(5), "2005");
		warehouseKeyMap.put(new Long(6), "2506");
		warehouseKeyMap.put(new Long(7), "3007");
		warehouseKeyMap.put(new Long(8), "3508");
		warehouseKeyMap.put(new Long(9), "4009");
		warehouseKeyMap.put(new Long(10), "4510");
		warehouseKeyMap.put(new Long(11), "5011");
		warehouseKeyMap.put(new Long(12), "5512");
		warehouseKeyMap.put(new Long(13), "6013");
		warehouseKeyMap.put(new Long(14), "6514");
		warehouseKeyMap.put(new Long(15), "7015");
		warehouseKeyMap.put(new Long(16), "7516");
		warehouseKeyMap.put(new Long(17), "8017");
		warehouseKeyMap.put(new Long(18), "8518");
		warehouseKeyMap.put(new Long(19), "9019");
		warehouseKeyMap.put(new Long(20), "9520");

		// Fill in the toBePrependedStringForMigratedLocks map. Assumption is that
		// the last
		// 5 nodes (demarcated by the last 4 split keys) will hold the migrated
		// locks.
		toBePrependedStringMapForMigratedLocks.put(new Long(1), "75");
		toBePrependedStringMapForMigratedLocks.put(new Long(2), "80");
		toBePrependedStringMapForMigratedLocks.put(new Long(3), "85");
		toBePrependedStringMapForMigratedLocks.put(new Long(4), "90");
		toBePrependedStringMapForMigratedLocks.put(new Long(5), "95");
	}

	public TPCCTablePropertiesClusteredPartitioning(Configuration conf,
			HBaseAdmin admin) {
		super(conf, admin);
		// TODO Auto-generated constructor stub
	}

	public void sysout(String strOut) {
		System.out.println(strOut);
	}
	
	public TPCCTablePropertiesClusteredPartitioning() {
	}

	public void createAndPopulateTable(long numEntries, long numSplits)
			throws IOException {
		// Lets assume the numSplits will give the number of warehouses in the
		// inventory
		// table. For each warehouse, there will be a WAL through which all updates
		// to the
		// stock in that warehouse will happen as a local transaction
		// We can make the keys for the inventory start with the warehouse number
		// (e.g., 1#1 implies
		// it is item id: 1 in warehouse: 1). This is similar to the graph
		// partitioning. Any
		// reads and writes to inventory in the same warehouse can happen in a
		// batch.
		// Further, the data table can be split into regions based on the warehouse
		// prefix.
		// The walTable may also be split using the warehouse-id
		List<byte[]> splitKeys = new ArrayList<byte[]>();
		if (numSplits == 5) {
			splitKeys.add(Bytes.toBytes("00:"));
			splitKeys.add(Bytes.toBytes("05:"));
			splitKeys.add(Bytes.toBytes("10:"));
			splitKeys.add(Bytes.toBytes("15:"));
		} else if (numSplits == 15) {
			for (int i = 1; i <= 7; i++) {
				String baseStr = new Integer(i).toString();
				splitKeys.add(Bytes.toBytes(baseStr + "5"));
				splitKeys.add(Bytes.toBytes(baseStr + ":"));
			}
			splitKeys.add(Bytes.toBytes(":"));
		} else if (numSplits == 20) {
			// We need the following splits: 00:, 05:, 10:, 15:, 20:, 25:, 30:, 35:,
			// 40:, 45:,
			// 50:, 55:, 60:, 65:, 70:, 75:, 80:, 85:, 90:, 95:
			splitKeys.add(Bytes.toBytes("00:"));
			splitKeys.add(Bytes.toBytes("05:"));
			splitKeys.add(Bytes.toBytes("10:"));
			splitKeys.add(Bytes.toBytes("15:"));
			splitKeys.add(Bytes.toBytes("20:"));
			splitKeys.add(Bytes.toBytes("25:"));
			splitKeys.add(Bytes.toBytes("30:"));
			splitKeys.add(Bytes.toBytes("35:"));
			splitKeys.add(Bytes.toBytes("40:"));
			splitKeys.add(Bytes.toBytes("45:"));
			splitKeys.add(Bytes.toBytes("50:"));
			splitKeys.add(Bytes.toBytes("55:"));
			splitKeys.add(Bytes.toBytes("60:"));
			splitKeys.add(Bytes.toBytes("65:"));
			splitKeys.add(Bytes.toBytes("70:"));
			splitKeys.add(Bytes.toBytes("75:"));
			splitKeys.add(Bytes.toBytes("80:"));
			splitKeys.add(Bytes.toBytes("85:"));
			splitKeys.add(Bytes.toBytes("90:"));
		}

		HTableDescriptor dataTableDesc = new HTableDescriptor(dataTableName);
		HColumnDescriptor dataFamilyDesc = new HColumnDescriptor(dataFamily);
		dataFamilyDesc.setMaxVersions(1);
		dataTableDesc.addFamily(dataFamilyDesc);

		// Since we merged the two tables, logFamily should also be present in
		// data table.
		HColumnDescriptor logFamilyDesc = new HColumnDescriptor(logFamily);
		logFamilyDesc.setMaxVersions(Integer.MAX_VALUE);
		logFamilyDesc.setInMemory(true);
		dataTableDesc.addFamily(logFamilyDesc);

		if (admin.tableExists(dataTableName)) {
			if (admin.isTableEnabled(dataTableName)) {
				admin.disableTable(dataTableName);
			}
			admin.deleteTable(dataTableName);
		}
		if (!splitKeys.isEmpty())
			admin.createTable(dataTableDesc, splitKeys.toArray(new byte[0][0]));
		else
			admin.createTable(dataTableDesc);

		// As the two tables are merged, we don't delete this table.
		/*
		 * HTableDescriptor logTableDesc = new HTableDescriptor(walTableName);
		 * HColumnDescriptor logFamilyDesc = new HColumnDescriptor(logFamily);
		 * logFamilyDesc.setMaxVersions(Integer.MAX_VALUE);
		 * logFamilyDesc.setInMemory(true); logTableDesc.addFamily(logFamilyDesc);
		 * 
		 * // testTableDesc.addFamily(new HColumnDescriptor(placementFamily)); //
		 * testTableDesc.addFamily(new HColumnDescriptor(isPutFamily)); //
		 * testTableDesc.addFamily(new HColumnDescriptor(isGetFamily)); if
		 * (admin.tableExists(walTableName)) { if
		 * (admin.isTableEnabled(walTableName)) { admin.disableTable(walTableName);
		 * } admin.deleteTable(walTableName); } if (!splitKeys.isEmpty())
		 * admin.createTable(logTableDesc, splitKeys.toArray(new byte[0][0])); else
		 * admin.createTable(logTableDesc);
		 */

		// Create and initialize the HBaseBackedTransactionLogger to store the
		// DistTxnMetadata
		// objects.
		HBaseBackedTransactionLogger.createTable(conf);
	}

	public String createWarehouseTableKey(long warehouseId) {
		return warehouseKeyMap.get(warehouseId) + ":" + "warehouse";
	}

	public String createItemTableKey(long itemId) {
		return Long.toString(itemId) + ":" + "item";
	}

	public String createDistrictTableKey(long warehouseId, long districtId) {
		return warehouseKeyMap.get(warehouseId) + ":" + Long.toString(districtId)
				+ ":" + "district";
	}

	public String createCustomerTableKey(long warehouseId, long districtId,
			long customerId) {
		return warehouseKeyMap.get(warehouseId) + ":" + Long.toString(districtId)
				+ ":" + Long.toString(customerId) + ":" + "customer";
	}

	public String createStockTableKey(long warehouseId, long itemId) {
		if (isStockTableRangePartitioned)
			return createStockTableKeyForRangePartitioning(warehouseId, itemId);
		return warehouseKeyMap.get(warehouseId) + ":" + Long.toString(itemId) + ":"
				+ "stock";
	}
	
	public String createStockTableKeyForRangePartitioning(long warehouseId, long itemId) {
		return Long.toString(itemId) + ":" + warehouseKeyMap.get(warehouseId) + ":" 
				+ "stock";
	}

	public String createOrderTableKey(long warehouseId, long districtId,
			long nextOrderId) {
		return warehouseKeyMap.get(warehouseId) + ":" + districtId + ":"
				+ nextOrderId + ":" + "order" + WALTableProperties.logAndKeySeparator
				+ warehouseKeyMap.get(warehouseId) + ":" + districtId + ":"
				+ nextOrderId + ":" + "order";
	}

	public String createOrderLineTableKey(long warehouseId, long districtId,
			long districtNextOrderId, long itemId) {
		return warehouseKeyMap.get(warehouseId) + ":" + districtId + ":"
				+ Long.toString(districtNextOrderId) + ":" + "order"
				+ WALTableProperties.logAndKeySeparator
				+ warehouseKeyMap.get(warehouseId) + ":" + districtId + ":"
				+ Long.toString(districtNextOrderId) + ":" + itemId + ":" + "orderLine";
	}

	public String createToBeMigratedNodeHint(long homeWarehouseId) {
		// Assumption: There are 15 warehouses sitting on 15 different nodes. 5
		// nodes will host the
		// migrated locks. TtoBePrependedStringForMigratedLocks map holds the String
		// to be prepended
		// for the 5 different lock-servers.
		long numLockServers = 5;
		long hashedHomeWarehouse = (homeWarehouseId % numLockServers) + 1;
		String toBePrependedString = toBePrependedStringMapForMigratedLocks
				.get(hashedHomeWarehouse);
		// Note that we aren't using any separator.
		String finalMigratedNodeHint = toBePrependedString + homeWarehouseId;
		return finalMigratedNodeHint;
	}

	// This makes the data table already present and load balanced in the cluster.
	// The execution of transactions would only lead to overwriting of values.
	public void populateDataTableEntries(long numWarehouses, boolean writeBlob)
			throws IOException, InterruptedException {
		HTable hDataTable = new HTable(conf, dataTableName);
		hDataTable.setAutoFlush(false);
		hDataTable.setWriteBufferSize(numWarehouses * 1000 * 1000);
		byte[] blob = new byte[BLOB_SIZE];
		// Populate the WAREHOUSE table.
		for (long i = 1; i <= numWarehouses; i++) {
			String key = createWarehouseTableKey(i);
			Put p = new Put(Bytes.toBytes(key));
			p.add(dataFamily, warehouseTaxRateColumn, appTimestamp, Bytes
					.toBytes(Integer.toString(constantTaxRate)));
			p.add(dataFamily, dataColumn, appTimestamp, Bytes.toBytes(Long
					.toString(0)));
			p.add(dataFamily, versionColumn, appTimestamp, Bytes.toBytes(Long
					.toString(zero)));
			p.add(dataFamily, writeLockColumn, appTimestamp, Bytes.toBytes(zero));
			if (writeBlob) {
				p.add(dataFamily, blobColumn, appTimestamp, blob);
			}

			p.setWriteToWAL(false);
			hDataTable.put(p);
		}

		// constant number of items in each warehouse.
		for (long i = 1; i <= numItemsPerWarehouse; i++) {
			String key = createItemTableKey(i);
			Put p = new Put(Bytes.toBytes(key));
			p.add(dataFamily, itemPriceColumn, appTimestamp, Bytes.toBytes(Integer
					.toString(constantItemPrice)));
			p.add(dataFamily, itemNameColumn, appTimestamp, Bytes.toBytes(Long
					.toString(i)));
			p.add(dataFamily, dataColumn, appTimestamp, Bytes.toBytes(Long
					.toString(0)));
			p.add(dataFamily, versionColumn, appTimestamp, Bytes.toBytes(Long
					.toString(zero)));
			p.add(dataFamily, writeLockColumn, appTimestamp, Bytes.toBytes(zero));
			if (writeBlob) {
				p.add(dataFamily, blobColumn, appTimestamp, blob);
			}

			p.setWriteToWAL(false);
			hDataTable.put(p);
		}

		// Populate the District table. There would be 10 districts per warehouse.
		for (long i = 1; i <= numWarehouses; i++) {
			for (long j = 1; j <= numDistrictsPerWarehouse; j++) {
				String key = createDistrictTableKey(i, j);
				Put p = new Put(Bytes.toBytes(key));
				p.add(dataFamily, districtTaxRateColumn, appTimestamp, Bytes
						.toBytes(Integer.toString(constantTaxRate)));
				p.add(dataFamily, districtNextOrderIdColumn, appTimestamp, Bytes
						.toBytes(Long.toString(one)));
				p.add(dataFamily, dataColumn, appTimestamp, Bytes.toBytes(Long
						.toString(0)));
				p.add(dataFamily, versionColumn, appTimestamp, Bytes.toBytes(Long
						.toString(zero)));
				p.add(dataFamily, writeLockColumn, appTimestamp, Bytes.toBytes(zero));
				if (writeBlob) {
					p.add(dataFamily, blobColumn, appTimestamp, blob);
				}

				p.setWriteToWAL(false);
				hDataTable.put(p);
			}
		}

		// Populate the Customer table. Each district has 3000 customers.
		for (long i = 1; i <= numWarehouses; i++) {
			for (long j = 1; j <= numDistrictsPerWarehouse; j++) {
				for (long k = 1; k <= numCustomersPerDistrict; k++) {
					String key = createCustomerTableKey(i, j, k);
					Put p = new Put(Bytes.toBytes(key));
					p.add(dataFamily, customerDiscountColumn, appTimestamp, Bytes
							.toBytes(Integer.toString(constantDiscount)));
					p.add(dataFamily, customerLastNameColumn, appTimestamp, Bytes
							.toBytes("ABC"));
					p.add(dataFamily, customerCreditColumn, appTimestamp, Bytes
							.toBytes("Y"));
					p.add(dataFamily, dataColumn, appTimestamp, Bytes.toBytes(Long
							.toString(0)));
					p.add(dataFamily, versionColumn, appTimestamp, Bytes.toBytes(Long
							.toString(zero)));
					p.add(dataFamily, writeLockColumn, appTimestamp, Bytes.toBytes(zero));
					if (writeBlob) {
						p.add(dataFamily, blobColumn, appTimestamp, blob);
					}

					p.setWriteToWAL(false);
					hDataTable.put(p);
				}
			}
		}

		// Populate the Stock table. Each warehouse has 100000 items.
		for (long i = 1; i <= numItemsPerWarehouse; i++) {
			for (long j = 1; j <= numWarehouses; j++) {
				String key = createStockTableKey(j, i);
				Put p = new Put(Bytes.toBytes(key));
				p.add(dataFamily, stockQuantityColumn, appTimestamp, Bytes.toBytes(Long
						.toString(zero)));
				p.add(dataFamily, dataColumn, appTimestamp, Bytes.toBytes(Long
						.toString(0)));
				p.add(dataFamily, versionColumn, appTimestamp, Bytes.toBytes(Long
						.toString(zero)));
				p.add(dataFamily, writeLockColumn, appTimestamp, Bytes.toBytes(zero));
				if (writeBlob) {
					p.add(dataFamily, blobColumn, appTimestamp, blob);
				}

				p.setWriteToWAL(false);
				hDataTable.put(p);
			}
		}

		hDataTable.flushCommits();
		hDataTable.close();
		if (writeBlob) {
			admin.flush(dataTableName);
			Thread.sleep(1000);
		}
		System.out.println("Wrote default inventory data!");
	}

	// Populates the locks for all data items in the table. The logId for the data
	// item
	// and the data-item-key are combined to form the final key at which the lock
	// will be stored.
	public void populateLocksForDataTableEntries(long numWarehouses)
			throws IOException, InterruptedException {
		HTable logTable = new HTable(conf, walTableName);
		logTable.setAutoFlush(false);
		logTable.setWriteBufferSize(numWarehouses * 1000 * 1000);

		// Populate the WAREHOUSE table.
		for (long i = 1; i <= numWarehouses; i++) {
			String keyStr = createWarehouseTableKey(i);
			byte[] key = Bytes.toBytes(keyStr);
			LogId logId = getLogIdForKey(key);
			byte[] finalKey = Bytes.toBytes(Bytes.toString(logId.getKey())
					+ logAndKeySeparator + keyStr);
			Put p = new Put(finalKey);
			p.add(WAL_FAMILY, writeLockColumn, appTimestamp, Bytes.toBytes(zero));
			p.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.isLockMigratedColumn,
					WALTableProperties.appTimestamp, Bytes
							.toBytes(WALTableProperties.zero));
			p.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.isLockPlacedOrMigratedColumn,
					WALTableProperties.appTimestamp, Bytes
							.toBytes(WALTableProperties.zero));
			p.add(WAL_FAMILY, versionColumn, appTimestamp, Bytes.toBytes(Long
					.toString(zero)));
			p.add(WAL_FAMILY, regionObserverMarkerColumn, appTimestamp, randomValue);
			p.setWriteToWAL(false);
			logTable.put(p);
		}

		// constant number of items in each warehouse.
		for (long i = 1; i <= numItemsPerWarehouse; i++) {
			String keyStr = createItemTableKey(i);
			byte[] key = Bytes.toBytes(keyStr);
			LogId logId = getLogIdForKey(key);
			byte[] finalKey = Bytes.toBytes(Bytes.toString(logId.getKey())
					+ logAndKeySeparator + keyStr);
			Put p = new Put(finalKey);
			p.add(WAL_FAMILY, writeLockColumn, appTimestamp, Bytes.toBytes(zero));
			p.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.isLockMigratedColumn,
					WALTableProperties.appTimestamp, Bytes
							.toBytes(WALTableProperties.zero));
			p.add(WALTableProperties.WAL_FAMILY,
					WALTableProperties.isLockPlacedOrMigratedColumn,
					WALTableProperties.appTimestamp, Bytes
							.toBytes(WALTableProperties.zero));
			p.add(WAL_FAMILY, versionColumn, appTimestamp, Bytes.toBytes(Long
					.toString(zero)));
			p.add(WAL_FAMILY, regionObserverMarkerColumn, appTimestamp, randomValue);
			p.setWriteToWAL(false);
			logTable.put(p);
		}

		// Populate the District table. There would be 10 districts per warehouse.
		for (long i = 1; i <= numWarehouses; i++) {
			for (long j = 1; j <= numDistrictsPerWarehouse; j++) {
				String keyStr = createDistrictTableKey(i, j);
				byte[] key = Bytes.toBytes(keyStr);
				LogId logId = getLogIdForKey(key);
				byte[] finalKey = Bytes.toBytes(Bytes.toString(logId.getKey())
						+ logAndKeySeparator + keyStr);
				Put p = new Put(finalKey);
				p.add(WAL_FAMILY, writeLockColumn, appTimestamp, Bytes.toBytes(zero));
				p.add(WALTableProperties.WAL_FAMILY,
						WALTableProperties.isLockMigratedColumn,
						WALTableProperties.appTimestamp, Bytes
								.toBytes(WALTableProperties.zero));
				p.add(WALTableProperties.WAL_FAMILY,
						WALTableProperties.isLockPlacedOrMigratedColumn,
						WALTableProperties.appTimestamp, Bytes
								.toBytes(WALTableProperties.zero));
				p.add(WAL_FAMILY, versionColumn, appTimestamp, Bytes.toBytes(Long
						.toString(zero)));
				p
						.add(WAL_FAMILY, regionObserverMarkerColumn, appTimestamp,
								randomValue);
				p.add(WAL_FAMILY, dtFrequencyColumn, appTimestamp, Bytes.toBytes(zero));
				p.add(WAL_FAMILY, ltFrequencyColumn, appTimestamp, Bytes.toBytes(zero));
				p.add(WAL_FAMILY, runningAvgForDTProportionColumn, appTimestamp, Bytes.toBytes(zero_double));
				p.setWriteToWAL(false);
				logTable.put(p);
			}
		}

		// We never need write locks for Customer table.
		// Populate the Customer table. Each district has 3000 customers.
		/*
		 * for (long i = 1; i <= numWarehouses; i++) { for (long j = 1; j <= 10;
		 * j++) { for (long k = 1; k <= numCustomersPerDistrict; k++) { String
		 * keyStr = Long.toString(i) + ":" + Long.toString(j) + ":" +
		 * Long.toString(k) + ":" + "customer"; byte[] key = Bytes.toBytes(keyStr);
		 * LogId logId = getLogIdForKey(key); byte[] finalKey =
		 * Bytes.toBytes(Bytes.toString(logId.getKey()) + logAndKeySeparator +
		 * keyStr); Put p = new Put(finalKey); p.add(WAL_FAMILY, writeLockColumn,
		 * appTimestamp, Bytes.toBytes(zero)); p.add(WALTableProperties.WAL_FAMILY,
		 * WALTableProperties.isLockMigratedColumn, WALTableProperties.appTimestamp,
		 * Bytes .toBytes(WALTableProperties.zero));
		 * p.add(WALTableProperties.WAL_FAMILY,
		 * WALTableProperties.isLockPlacedOrMigratedColumn,
		 * WALTableProperties.appTimestamp, Bytes
		 * .toBytes(WALTableProperties.zero)); p.add(WAL_FAMILY, versionColumn,
		 * appTimestamp, Bytes.toBytes(Long .toString(zero))); p.add(WAL_FAMILY,
		 * regionObserverMarkerColumn, appTimestamp, randomValue);
		 * p.setWriteToWAL(false); logTable.put(p); } } }
		 */

		// Populate the Stock table. Each warehouse has 100000 items.
		for (long i = 1; i <= numItemsPerWarehouse; i++) {
			for (long j = 1; j <= numWarehouses; j++) {
				String keyStr = createStockTableKey(j, i);
				byte[] key = Bytes.toBytes(keyStr);
				LogId logId = getLogIdForKey(key);
				byte[] finalKey = Bytes.toBytes(Bytes.toString(logId.getKey())
						+ logAndKeySeparator + keyStr);
				Put p = new Put(finalKey);
				p.add(WAL_FAMILY, writeLockColumn, appTimestamp, Bytes.toBytes(zero));
				p.add(WALTableProperties.WAL_FAMILY,
						WALTableProperties.isLockMigratedColumn,
						WALTableProperties.appTimestamp, Bytes
								.toBytes(WALTableProperties.zero));
				p.add(WALTableProperties.WAL_FAMILY,
						WALTableProperties.isLockPlacedOrMigratedColumn,
						WALTableProperties.appTimestamp, Bytes
								.toBytes(WALTableProperties.zero));
				p.add(WAL_FAMILY, versionColumn, appTimestamp, Bytes.toBytes(Long
						.toString(zero)));
				p
						.add(WAL_FAMILY, regionObserverMarkerColumn, appTimestamp,
								randomValue);
				p.add(WAL_FAMILY, dtFrequencyColumn, appTimestamp, Bytes.toBytes(zero));
				p.add(WAL_FAMILY, ltFrequencyColumn, appTimestamp, Bytes.toBytes(zero));
				p.add(WAL_FAMILY, runningAvgForDTProportionColumn, appTimestamp,
						Bytes.toBytes(zero_double));
				p.setWriteToWAL(false);
				logTable.put(p);
			}
		}

		logTable.flushCommits();
		logTable.close();
		// Turn the load balancer off.
		admin.balanceSwitch(false);

		System.out.println("Wrote default lock data!");
	}

	private int generateItemFromZipfian() {
		if (this.zipfForItem == null)
			this.zipfForItem = new ZipfDistribution(numItemsPerWarehouse, contentionParam);

		return zipfForItem.sample();
	}
	
	private int generateWarehouseFromZipfian() {
		if (this.zipfForWarehouse == null)
			this.zipfForWarehouse = new ZipfDistribution(warehousesCount, warehouseDistParam);

		return zipfForWarehouse.sample();
	}

	private StringBuffer generateDistTxnClusteredPartitioning(
			StringBuffer txnStrBuf) {
		HashMap<Integer, Integer> generatedIds = new HashMap<Integer, Integer>();
		for (int i = 0; i < trxLen; i++) {
			int itemId = -1;
			int warehouseId = homeWarehouseId;
			do {
				if (contentionParam > 0.1)
					itemId = generateItemFromZipfian();
				else
					itemId = rand.nextInt(numItemsPerWarehouse) + 1;
			} while (generatedIds.get(itemId) != null);
			generatedIds.put(itemId, itemId);
			
			int remoteWarehouseProbInInt = (int) (remoteWarehouseProb * 100);
			//sysout("RemoteWarehouseProbInInt: " + remoteWarehouseProbInInt);
			int randId = rand.nextInt(100);
			if (randId < remoteWarehouseProbInInt) {
				do {
					// The comparison with 0.2 is to eliminate float "equality" comparisons.
					if (warehouseDistParam > 0.02) {
						warehouseId = generateWarehouseFromZipfian();
						// NOTE: If we decided to put zipfian across warehouses, then highest contented 
						// warehouse should be chosen for fulfillment irrespective of whether it is the home
						// warehouse. The ability to choose from this zipfian is forced by high value of
						// remotewarehouse probability. Thus, even if we start same number of clients per
						// warehouse, the high remoteWarehouseProb and zipfian across warehouses will lead
						// to gradual contention across warehouses. The residual part of "remoteWarehouseProb" 
						// (1 - remoteWarehouseProb) will lead to multi-hops. 
						break;
					}	else { 
						warehouseId = rand.nextInt(warehousesCount) + 1;
					}
				} while (warehouseId == homeWarehouseId && warehousesCount > 1);
			}

			txnStrBuf.append(itemId + " " + warehouseId + " ");
		}
		return txnStrBuf;
	}

	@Override
	public String generateNewDistTxn() {
		StringBuffer txnStrBuf = new StringBuffer();
		int districtId = rand.nextInt(numDistrictsPerWarehouse) + 1;
		int randACustomer = rand.nextInt(A_Customer);
		int randXYCustomer = rand.nextInt(numCustomersPerDistrict) + 1;
		int customerId = ((randACustomer | randXYCustomer) + C)
				% numCustomersPerDistrict + 1;

		txnStrBuf.append(homeWarehouseId + ":warehouse ");
		txnStrBuf.append(homeWarehouseId + ":" + districtId + ":district ");
		txnStrBuf.append(homeWarehouseId + ":" + districtId + ":" + customerId
				+ ":customer ");

		txnStrBuf = generateDistTxnClusteredPartitioning(txnStrBuf);
		//System.out.println("Generated txn: " + txnStrBuf);
		return txnStrBuf.toString();
	}
}
