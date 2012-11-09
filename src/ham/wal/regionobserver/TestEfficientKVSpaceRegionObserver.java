package ham.wal.regionobserver;

import ham.wal.WALTableProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class TestEfficientKVSpaceRegionObserver {
	public static byte[] testTableName = Bytes.toBytes("TEST_TABLE");
	private Configuration conf = null;
	private HTable table = null;
	private static byte[] testRowKey = Bytes.toBytes("TEST_ROW_KEY");
	private static byte[] testFamily = Bytes.toBytes("TEST_FAMILY");
	private static byte[] testColumnA = Bytes.toBytes("TEST_COLUMN_A");
	private static byte[] testColumnB = Bytes.toBytes("TEST_COLUMN_B");
	public static byte[] regionObserverMarkerColumn = WALTableProperties.regionObserverMarkerColumn;
	public static byte[] testValA = Bytes.toBytes("TEST_VAL_A");
	public static byte[] testValB = Bytes.toBytes("TEST_VAL_B");
	public static long genericTimestamp = WALTableProperties.appTimestamp;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			TestEfficientKVSpaceRegionObserver testObserver = new TestEfficientKVSpaceRegionObserver();

			testObserver.test();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void test() throws Exception {
		createTable();
		insertPuts();
		verifyWithGets();
		makeDeletes();
		verifyWithGetsAfterDeletes();
	}

	private void verifyWithGetsAfterDeletes() throws Exception {
		Get g = new Get(testRowKey);
		g.addColumn(testFamily, testColumnA);
		g.addColumn(testFamily, regionObserverMarkerColumn);
		g.setTimeStamp(genericTimestamp);
		Result r = this.table.get(g);
		byte[] retrievedValA = r.getValue(testFamily, testColumnA);
		byte[] retrievedValB = r.getValue(testFamily, testColumnB);
		if (retrievedValA == null && retrievedValB == null) {
			System.out.println("Test SUCCESS!");
		} else {
			System.out.println("TEST FAILED!!");
			System.out.println("Retrieved ValA: " + Bytes.toString(retrievedValA) + ", and " +
					"retrieved ValB: " + Bytes.toString(retrievedValB));
		}
	}

	private void makeDeletes() throws Exception {
		// TODO Auto-generated method stub
		Delete d = new Delete(testRowKey);
		d.deleteColumn(testFamily, testColumnA, genericTimestamp);
		d.deleteColumn(testFamily, testColumnB, genericTimestamp);
		d.deleteColumn(testFamily, regionObserverMarkerColumn, genericTimestamp);
		this.table.delete(d);
	}

	private void verifyWithGets() throws Exception {
		Get g = new Get(testRowKey);
		g.addColumn(testFamily, testColumnA);
		g.addColumn(testFamily, testColumnB);
		g.addColumn(testFamily, regionObserverMarkerColumn);
		g.setTimeStamp(genericTimestamp);
		Result r = this.table.get(g);
		byte[] retrievedValA = r.getValue(testFamily, testColumnA);
		byte[] retrievedValB = r.getValue(testFamily, testColumnB);
		if (Bytes.equals(retrievedValA, testValA)
				&& Bytes.equals(retrievedValB, testValB)) {
			System.out.println("Test SUCCESS!");
		} else {
			System.out.println("TEST FAILED!!");
			System.out.println("Retrieved ValA: " + Bytes.toString(retrievedValA)
					+ ", and retrieved ValB is: " + Bytes.toString(retrievedValB));
		}
	}

	private void insertPuts() throws Exception {
		Put p = new Put(testRowKey);
		p.add(testFamily, testColumnA, genericTimestamp, testValA);
		//p.setWriteToWAL(false);
		this.table.put(p);
		this.table.flushCommits();

		p = new Put(testRowKey);
		p.add(testFamily, testColumnB, genericTimestamp, testValB);
		p.add(testFamily, regionObserverMarkerColumn, genericTimestamp, testValA);
		//p.setWriteToWAL(false);
		this.table.put(p);
		this.table.flushCommits();
	}

	private void createTable() throws Exception {
		this.conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor testTableDesc = new HTableDescriptor(testTableName);
		HColumnDescriptor testFamilyDesc = new HColumnDescriptor(testFamily);
		testFamilyDesc.setMaxVersions(1);
		testTableDesc.addFamily(testFamilyDesc);

		if (admin.tableExists(testTableName)) {
			if (admin.isTableEnabled(testTableName)) {
				admin.disableTable(testTableName);
			}
			admin.deleteTable(testTableName);
		}
		admin.createTable(testTableDesc);
		this.table = new HTable(conf, testTableName);
	}
}
