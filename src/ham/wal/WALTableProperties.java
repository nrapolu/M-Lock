package ham.wal;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class WALTableProperties {
	public final static String WAL_TABLENAME = "WAL_Table";
	public final static byte[] WAL_FAMILY = Bytes.toBytes("WAL_FAMILY");
	public final static byte[] CURRENT_TS_COL = Bytes.toBytes("CURRENT_TS");
	public final static byte[] SYNC_TS_COL = Bytes.toBytes("SYNC_TS_COL");
	public final static byte[] OLDEST_TS_COL = Bytes.toBytes("OLDEST_TS_COL");
	public final static byte[] WAL_ENTRY_COL = Bytes.toBytes("WAL_ENTRY_COL");
	public final static long GENERIC_TIMESTAMP = 1;

	public final static byte[] dataTableName = Bytes.toBytes("DATA_TABLE");
	// final static byte[] walTableName = Bytes.toBytes(WAL_TABLENAME);
	public final static byte[] walTableName = Bytes.toBytes("DATA_TABLE");
	public final static byte[] dataFamily = Bytes.toBytes("DATA_FAMILY");
	public final static byte[] dataColumn = Bytes.toBytes("DATA_COLUMN");
	public final static byte[] versionColumn = Bytes.toBytes("VERSION_COLUMN");
	public final static byte[] writeLockColumn = Bytes
			.toBytes("WRITE_LOCK_COLUMN");
	public final static byte[] blobColumn = Bytes.toBytes("BLOB_COLUMN");
	public final static byte[] isLockMigratedColumn = Bytes
			.toBytes("IS_LOCK_MIGRATED");
	public final static byte[] isLockPlacedOrMigratedColumn = Bytes
			.toBytes("IS_LOCK_PLACED_OR_MIGRATED");
	public final static byte[] destinationKeyColumn = Bytes
			.toBytes("DESTINATION_KEY");

	public final static byte[] regionObserverMarkerColumn = Bytes
			.toBytes("GO_THROUGH_REGION_OBSERVER");
	public byte[] logFamily = WALTableProperties.WAL_FAMILY;
	public static long appTimestamp = GENERIC_TIMESTAMP;

	static long zero = 0;
	static long one = 1;
	static long two = 2;
	// Random entries to be used inside Put[] based transactions.
	static byte[] randomColumn = Bytes.toBytes("RANDOM_COLUMN");
	static byte[] randomValue = Bytes.toBytes("RANDOM_VALUE");

	final static int BLOB_SIZE = 100;
	static String logAndKeySeparator = "#";
	static String shadowKeySeparator = "^";

	Configuration conf = null;
	HBaseAdmin admin = null;

	public WALTableProperties(Configuration conf, HBaseAdmin admin) {
		this.conf = conf;
		this.admin = admin;
	}

	public WALTableProperties() {
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

	// This makes the data table already present and load balanced in the cluster.
	// The execution of transactions would only lead to overwriting of values.
	public void populateDataTableEntries(long numEntries, boolean writeBlob)
			throws IOException, InterruptedException {
		HTable hDataTable = new HTable(conf, dataTableName);
		hDataTable.setWriteBufferSize(numEntries / 10);
		byte[] blob = new byte[BLOB_SIZE];
		for (long i = 0; i < numEntries; i++) {
			Put p = new Put(Bytes.toBytes(Long.toString(i)));
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
	public void populateLocksForDataTableEntries(long numEntries)
			throws IOException, InterruptedException {
		HTable logTable = new HTable(conf, walTableName);
		logTable.setAutoFlush(false);
		logTable.setWriteBufferSize(numEntries / 10);
		for (long i = 0; i < numEntries; i++) {
			String keyStr = Long.toString(i);
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
			p.setWriteToWAL(false);
			logTable.put(p);
		}
		logTable.flushCommits();
		logTable.close();
		System.out.println("Wrote default lock data!");
	}

	public void turnOffBalancer() throws IOException {
		this.admin.balanceSwitch(false);
	}

	public static long getVersion(Result result) {
		long version = Long.parseLong(Bytes.toString(result.getValue(dataFamily,
				versionColumn)));
		return version;
	}

	public static boolean isWriteLockOn(Result result) {
		if (result.getValue(dataFamily, writeLockColumn) != null)
			return true;
		return false;
	}

	public static LogId getLogIdForKey(byte[] key) {
		// For now, we are assuming the key to contain the logId. They will be
		// separated
		// by the logIdDelimiter.
		String[] splits = Bytes.toString(key)
				.split("[" + logAndKeySeparator + "]+");
		LogId logId = new LogId();
		logId.setKey(Bytes.toBytes(splits[0]));
		logId.setName(walTableName);
		return logId;
	}

	public static HRegionLocation getRegionLocationForLogId(HTableInterface dataTable,
			LogId logId) throws IOException {
		return ((HTable) dataTable).getRegionLocation(logId.getKey(), false);
	}

	public void findCacheHits(String fileName, long nbTrxToExecute)
			throws IOException, InterruptedException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String trx = null;
		int count = 0;
		HTable dataTable = new HTable(conf, dataTableName);
		dataTable.setWriteBufferSize(10);
		while ((trx = br.readLine()) != null && count < nbTrxToExecute) {
			String[] tokens = trx.split("\\s+");
			String[] keys = new String[tokens.length];
			List<Get> gets = new LinkedList<Get>();
			for (int i = 0; i < tokens.length; i++) {
				String[] keySplits = tokens[i].split("["
						+ WALTableProperties.logAndKeySeparator + "]+");
				keys[i] = keySplits[1];
				Get g = new Get(Bytes.toBytes(keys[i]));
				g.addColumn(dataFamily, blobColumn);
				g.addColumn(dataFamily, dataColumn);
				g.addColumn(dataFamily, versionColumn);
				gets.add(g);
			}

			long getStartTime = System.currentTimeMillis();
			Object[] results = dataTable.batch(gets);
			long getEndTime = System.currentTimeMillis();
			System.out.println("Get Time: " + (getEndTime - getStartTime));

			byte[] blob = new byte[BLOB_SIZE];
			List<Put> puts = new LinkedList<Put>();
			for (int i = 0; i < keys.length; i++) {
				Put p = new Put(Bytes.toBytes(keys[i]));
				p.add(dataFamily, dataColumn, appTimestamp, Bytes.toBytes(Long
						.toString(0)));
				p.add(dataFamily, versionColumn, appTimestamp, Bytes.toBytes(zero));
				p.add(dataFamily, writeLockColumn, appTimestamp, Bytes.toBytes(zero));
				p.add(dataFamily, blobColumn, appTimestamp, blob);

				p.setWriteToWAL(false);
				puts.add(p);
			}
			long putStartTime = System.currentTimeMillis();
			dataTable.batch(puts);
			long putEndTime = System.currentTimeMillis();
			System.out.println("Put Time: " + (putEndTime - putStartTime));
			count++;
		}
		dataTable.close();
		br.close();
	}

	static void printUsage() {
		System.out
				.println("Usage: WALTableProperties <numDataEntries> <numSplits> <TrxFileName> "
						+ "<NbTrxToExecute> <Options: 1 -- OnlyWriteToTable, 2 -- OnlyExecuteTrx,"
						+ " 3 -- WriteAndExecute>");
	}

	public static void main(String[] args) {
		if (args.length < 5) {
			printUsage();
			System.exit(-1);
		}
		try {
			long numDataEntries = Long.parseLong(args[0]);
			int numSplits = Integer.parseInt(args[1]);
			String trxFileName = args[2];
			long nbTrxToExecute = Long.parseLong(args[3]);
			int option = Integer.parseInt(args[4]);
			Configuration conf = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(conf);
			WALTableProperties tableProps = new WALTableProperties(conf, admin);
			if (option == 1) {
				tableProps.createAndPopulateTable(numDataEntries, numSplits);
				tableProps.populateDataTableEntries(numDataEntries, true);
			} else if (option == 2)
				tableProps.findCacheHits(trxFileName, nbTrxToExecute);
			else if (option == 3) {
				tableProps.createAndPopulateTable(numDataEntries, numSplits);
				tableProps.populateDataTableEntries(numDataEntries, true);
				tableProps.findCacheHits(trxFileName, nbTrxToExecute);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static LogId getLogIdFromMigratedKey(byte[] migratedKey) {
		String migratedKeyStr = Bytes.toString(migratedKey);
		String[] splits = migratedKeyStr.split(logAndKeySeparator);
		LogId logId = new LogId();
		logId.setKey(Bytes.toBytes(splits[0]));
		logId.setName(walTableName);
		return logId;
	}
	
	public String generateNewDistTxn() {
		return null;
	}
}
