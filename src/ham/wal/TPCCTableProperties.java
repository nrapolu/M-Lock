package ham.wal;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TPCCTableProperties extends WALTableProperties {
	byte[] warehouseTaxRateColumn = Bytes.toBytes("W_TAX");
	byte[] districtTaxRateColumn = Bytes.toBytes("D_TAX");
	byte[] districtNextOrderIdColumn = Bytes.toBytes("D_NEXT_O_ID");
	byte[] customerDiscountColumn = Bytes.toBytes("C_DISCOUNT");
	byte[] customerLastNameColumn = Bytes.toBytes("C_LAST");
	byte[] customerCreditColumn = Bytes.toBytes("C_CREDIT");
	byte[] orderAllLocalColumn = Bytes.toBytes("O_ALL_LOCAL");
	byte[] orderOrderLineCountColumn = Bytes.toBytes("O_OL_CNT");
	byte[] orderIdColumn = Bytes.toBytes("O_ID");
	byte[] orderDistrictIdColumn = Bytes.toBytes("O_D_ID");
	byte[] orderWarehouseIdColumn = Bytes.toBytes("O_W_ID");
	byte[] orderCustomerIdColumn = Bytes.toBytes("O_C_ID");
	byte[] itemPriceColumn = Bytes.toBytes("I_PRICE");
	byte[] itemNameColumn = Bytes.toBytes("I_NAME");
	byte[] newOrderIdColumn = Bytes.toBytes("NO_O_ID");
	byte[] newOrderDistrictIdColumn = Bytes.toBytes("NO_D_ID");
	byte[] newOrderWarehouseIdColumn = Bytes.toBytes("NO_W_ID");
	byte[] stockQuantityColumn = Bytes.toBytes("S_QUANTITY");
	byte[] stockOrderCountColumn = Bytes.toBytes("S_ORDER_CNT");
	byte[] stockSoldYearToDateColumn = Bytes.toBytes("S_YTD");
	byte[] orderLineOrderIdColumn = Bytes.toBytes("OL_O_ID");
	byte[] orderLineNumberColumn = Bytes.toBytes("OL_NUMBER");
	byte[] orderLineDistrictIdColumn = Bytes.toBytes("OL_D_ID");
	byte[] orderLineWarehouseIdColumn = Bytes.toBytes("OL_W_ID");
	byte[] orderLineItemIdColumn = Bytes.toBytes("OL_I_ID");
	byte[] orderLineSupplyWarehouseIdColumn = Bytes.toBytes("OL_SUPPLY_W_ID");
	byte[] orderLineQuantityColumn = Bytes.toBytes("OL_QUANTITY");
	byte[] orderLineAmountColumn = Bytes.toBytes("OL_AMOUNT");

	static String orderWALPrefix = "!";
	static String districtWALPrefix = "=";
	
	int constantTaxRate = 10;
	int constantItemPrice = 10;
	int constantDiscount = 10;

	int numItemsPerWarehouse = 100;
	int numCustomersPerDistrict = 3000;
	
	public TPCCTableProperties(Configuration conf, HBaseAdmin admin) {
		super(conf, admin);
		// TODO Auto-generated constructor stub
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
			p.add(dataFamily, warehouseTaxRateColumn, appTimestamp, Bytes.toBytes(Integer
					.toString(constantTaxRate)));
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
			p.add(dataFamily, itemNameColumn, appTimestamp, Bytes.toBytes(Long.toString(i)));
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
			for (long j = 1; j <= 10; j++) {
				String key = districtWALPrefix + Long.toString(i) + ":" + Long.toString(j) + ":"
						+ "district";
				Put p = new Put(Bytes.toBytes(key));
				p.add(dataFamily, districtTaxRateColumn, appTimestamp, Bytes.toBytes(Integer
						.toString(constantTaxRate)));
				p.add(dataFamily, districtNextOrderIdColumn, appTimestamp, Bytes.toBytes(Long
						.toString(one)));
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
			for (long j = 1; j <= 10; j++) {
				for (long k = 1; k <= numCustomersPerDistrict; k++) {
					String key = Long.toString(j) + ":" + Long.toString(i) + ":"
							+ Long.toString(k) + ":" + "customer";
					Put p = new Put(Bytes.toBytes(key));
					p.add(dataFamily, customerDiscountColumn, appTimestamp, Bytes.toBytes(Integer
							.toString(constantDiscount)));
					p.add(dataFamily, customerLastNameColumn, appTimestamp, Bytes.toBytes("ABC"));
					p.add(dataFamily, customerCreditColumn, appTimestamp, Bytes.toBytes("Y"));
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
				String key = Long.toString(i) + ":" + Long.toString(j) + ":"
						+ "stock";
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
			for (long j = 1; j <= 10; j++) {
				String keyStr = districtWALPrefix + Long.toString(i) + ":" + Long.toString(j) + ":"
						+ "district";
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
		}

		// Populate the Customer table. Each district has 3000 customers.
		for (long i = 1; i <= numWarehouses; i++) {
			for (long j = 1; j <= 10; j++) {
				for (long k = 1; k <= numCustomersPerDistrict; k++) {
					String keyStr = Long.toString(i) + ":" + Long.toString(j) + ":"
							+ Long.toString(k) + ":" + "customer";
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
			}
		}

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
				p.add(WAL_FAMILY, regionObserverMarkerColumn, appTimestamp, randomValue);
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
}
