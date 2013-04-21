package ham.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class TPCCTableProperties extends WALTableProperties {
	static byte[] warehouseTaxRateColumn = Bytes.toBytes("W_TAX");
	static byte[] districtTaxRateColumn = Bytes.toBytes("D_TAX");
	static byte[] districtNextOrderIdColumn = Bytes.toBytes("D_NEXT_O_ID");
	static byte[] customerDiscountColumn = Bytes.toBytes("C_DISCOUNT");
	static byte[] customerLastNameColumn = Bytes.toBytes("C_LAST");
	static byte[] customerCreditColumn = Bytes.toBytes("C_CREDIT");
	static byte[] orderAllLocalColumn = Bytes.toBytes("O_ALL_LOCAL");
	static byte[] orderOrderLineCountColumn = Bytes.toBytes("O_OL_CNT");
	static byte[] orderIdColumn = Bytes.toBytes("O_ID");
	static byte[] orderDistrictIdColumn = Bytes.toBytes("O_D_ID");
	static byte[] orderWarehouseIdColumn = Bytes.toBytes("O_W_ID");
	static byte[] orderCustomerIdColumn = Bytes.toBytes("O_C_ID");
	static byte[] itemPriceColumn = Bytes.toBytes("I_PRICE");
	static byte[] itemNameColumn = Bytes.toBytes("I_NAME");
	static byte[] newOrderIdColumn = Bytes.toBytes("NO_O_ID");
	static byte[] newOrderDistrictIdColumn = Bytes.toBytes("NO_D_ID");
	static byte[] newOrderWarehouseIdColumn = Bytes.toBytes("NO_W_ID");
	static byte[] stockQuantityColumn = Bytes.toBytes("S_QUANTITY");
	static byte[] stockOrderCountColumn = Bytes.toBytes("S_ORDER_CNT");
	static byte[] stockSoldYearToDateColumn = Bytes.toBytes("S_YTD");
	static byte[] orderLineOrderIdColumn = Bytes.toBytes("OL_O_ID");
	static byte[] orderLineNumberColumn = Bytes.toBytes("OL_NUMBER");
	static byte[] orderLineDistrictIdColumn = Bytes.toBytes("OL_D_ID");
	static byte[] orderLineWarehouseIdColumn = Bytes.toBytes("OL_W_ID");
	static byte[] orderLineItemIdColumn = Bytes.toBytes("OL_I_ID");
	static byte[] orderLineSupplyWarehouseIdColumn = Bytes
			.toBytes("OL_SUPPLY_W_ID");
	static byte[] orderLineQuantityColumn = Bytes.toBytes("OL_QUANTITY");
	static byte[] orderLineAmountColumn = Bytes.toBytes("OL_AMOUNT");

	static String orderWALPrefix = "!";
	static String districtWALPrefix = "=";

	static int constantTaxRate = 10;
	static int constantItemPrice = 10;
	static int constantDiscount = 10;

	static int numItemsPerWarehouse = 100000;
	static int numCustomersPerDistrict = 3000;
	static int numDistrictsPerWarehouse = 10;

	Random rand = new Random();
	ZipfDistribution zipfForItem = null;
	ZipfDistribution zipfForWarehouse = null;
	static final int A_Item = 8191;
	static final int A_Customer = 1023;
	static final int C = 1;

	public static int warehousesCount = 15;
	public static int homeWarehouseId = 1;
	public static float contentionParam = 0;
	public static float remoteWarehouseProb = (float) 0.1;
	public static float warehouseDistParam = (float) 0;
	public static int trxLen = 10;
	public static int maxHops = 100;
	
	public TPCCTableProperties(Configuration conf, HBaseAdmin admin) {
		super(conf, admin);
		// TODO Auto-generated constructor stub
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
		if (numSplits <= 11 && numSplits > 1) {
			for (int i = 1; i <= numSplits - 2; i++) {
				splitKeys.add(Bytes.toBytes(new Integer(i).toString()));
			}
			// ASCII character just after 9 is ":".
			splitKeys.add(Bytes.toBytes(":"));
		} else if (numSplits == 15) {
			for (int i = 1; i <= 7; i++) {
				String baseStr = new Integer(i).toString();
				splitKeys.add(Bytes.toBytes(baseStr + "5"));
				splitKeys.add(Bytes.toBytes(baseStr + ":"));
			}
			splitKeys.add(Bytes.toBytes(":"));
		} else if (numSplits == 20) {
			// We need the following splits 10000000, 1:, 25, 2:, 35, 3:, ..., 85, 8:,
			// 95, 9:, :
			splitKeys.add(Bytes.toBytes("10000000"));
			for (int i = 1; i <= 9; i++) {
				String baseStr = new Integer(i).toString();
				if (i != 1)
					splitKeys.add(Bytes.toBytes(baseStr + "5"));
				// if (i != 9)
				splitKeys.add(Bytes.toBytes(baseStr + ":"));
			}
			splitKeys.add(Bytes.toBytes(":"));
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

	public TPCCTableProperties() {
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
			String key = Long.toString(i) + ":" + "warehouse";
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
			String key = Long.toString(i) + ":" + "item";
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
				String key = districtWALPrefix + Long.toString(i) + ":"
						+ Long.toString(j) + ":" + "district";
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
					String key = Long.toString(j) + ":" + Long.toString(i) + ":"
							+ Long.toString(k) + ":" + "customer";
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
				String key = Long.toString(i) + ":" + Long.toString(j) + ":" + "stock";
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
			String keyStr = Long.toString(i) + ":" + "warehouse";
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
			String keyStr = Long.toString(i) + ":" + "item";
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
				String keyStr = districtWALPrefix + Long.toString(i) + ":"
						+ Long.toString(j) + ":" + "district";
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
				String keyStr = Long.toString(i) + ":" + Long.toString(j) + ":"
						+ "stock";
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
	
	class GenerateSpecificMultiHopUniform {
		int prevId = 10;
		HashMap<Integer, Integer> generatedIds = new HashMap<Integer, Integer>();
		
		public GenerateSpecificMultiHopUniform() {
			this.prevId = 10;
		}
		
		public void reset() {
			this.prevId = 10;
		}
		
		public int sample() {
			int nextId = prevId + 5;
			if (nextId == 100)
				nextId = 10;
			
			prevId = nextId;
			
			int baseSaltId = 100;
			int saltedId = -1;
			
			do {
				generatedIds.put(saltedId, saltedId);
				int salt = rand.nextInt(899) + baseSaltId;
				String saltedIdStr = Integer.toString(nextId) + Integer.toString(salt);
				int numItemsPerWarehouseStrLen = Integer.toString(numItemsPerWarehouse).length();
				saltedIdStr = saltedIdStr.substring(0, numItemsPerWarehouseStrLen - 1);
				saltedId = Integer.parseInt(saltedIdStr);
			} while (generatedIds.get(saltedId) != null);
			
			return saltedId;
		}
	}

	private StringBuffer generateDistTxn(
			StringBuffer txnStrBuf) {
		HashMap<Integer, Integer> generatedIds = new HashMap<Integer, Integer>();
		GenerateSpecificMultiHopUniform generateSpecificMultiHop = null;
		for (int i = 0; i < trxLen; i++) {
			int itemId = -1;
			int warehouseId = homeWarehouseId;
			do {
				generatedIds.put(itemId, itemId);
				if (contentionParam < 0.1) {
					itemId = rand.nextInt(numItemsPerWarehouse) + 1;
				} else if (contentionParam > 2) {
					if (generateSpecificMultiHop == null)
						generateSpecificMultiHop = new GenerateSpecificMultiHopUniform();
					
					itemId = generateSpecificMultiHop.sample();
					if (trxLen % maxHops == 0)
						generateSpecificMultiHop.reset();
					
				} else {
					itemId = generateItemFromZipfian();
				}
			} while (generatedIds.get(itemId) != null);

			int remoteWarehouseProbInInt = (int) remoteWarehouseProb * 100;
			int randId = rand.nextInt(100);
			if (randId < remoteWarehouseProbInInt) {
				do {
					warehouseId = rand.nextInt(warehousesCount) + 1;
				} while (warehouseId == homeWarehouseId);
			}

			txnStrBuf.append(itemId + " " + warehouseId + " ");
		}
		return txnStrBuf;
	}

	@Override
	public String generateNewDistTxn() {
		StringBuffer txnStrBuf = new StringBuffer();
		int districtId = rand.nextInt(TPCCTableProperties.numDistrictsPerWarehouse) + 1;
		int randACustomer = rand.nextInt(A_Customer);
		int randXYCustomer = rand.nextInt(TPCCTableProperties.numCustomersPerDistrict) + 1;
		int customerId = ((randACustomer | randXYCustomer) + C)
				% TPCCTableProperties.numCustomersPerDistrict + 1;

		txnStrBuf.append(homeWarehouseId + ":warehouse ");
		txnStrBuf.append(homeWarehouseId + ":" + districtId + ":district ");
		txnStrBuf.append(homeWarehouseId + ":" + districtId + ":" + customerId
				+ ":customer ");

		txnStrBuf = generateDistTxn(txnStrBuf);
		//System.out.println("Generated txn: " + txnStrBuf);
		return txnStrBuf.toString();
	}
}
