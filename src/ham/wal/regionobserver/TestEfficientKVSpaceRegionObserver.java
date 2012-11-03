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
	private static byte[] testColumn = Bytes.toBytes("TEST_COLUMN");
	public static byte[] regionObserverMarkerColumn = WALTableProperties.regionObserverMarkerColumn;
	public static byte[] testVal = Bytes.toBytes("TEST_VAL");
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
		g.addColumn(testFamily, testColumn);
		g.addColumn(testFamily, regionObserverMarkerColumn);
		g.setTimeStamp(genericTimestamp);
		Result r = this.table.get(g);
		byte[] retrievedVal = r.getValue(testFamily, testColumn);
		if (retrievedVal == null) {
			System.out.println("Test SUCCESS!");
		} else {
			System.out.println("TEST FAILED!!");
			System.out.println("Retrieved Val: " + Bytes.toString(retrievedVal));
		}
	}

	private void makeDeletes() throws Exception {
		// TODO Auto-generated method stub
		Delete d = new Delete(testRowKey);
		d.deleteColumn(testFamily, testColumn, genericTimestamp);
		d.deleteColumn(testFamily, regionObserverMarkerColumn, genericTimestamp);
		this.table.delete(d);
	}

	private void verifyWithGets() throws Exception {
		Get g = new Get(testRowKey);
		g.addColumn(testFamily, testColumn);
		g.addColumn(testFamily, regionObserverMarkerColumn);
		g.setTimeStamp(genericTimestamp);
		Result r = this.table.get(g);
		byte[] retrievedVal = r.getValue(testFamily, testColumn);
		if (Bytes.equals(retrievedVal, testVal)) {
			System.out.println("Test SUCCESS!");
		} else {
			System.out.println("TEST FAILED!!");
			System.out.println("Retrieved Val: " + Bytes.toString(retrievedVal));
		}
	}

	private void insertPuts() throws Exception {
		Put p = new Put(testRowKey);
		p.add(testFamily, testColumn, genericTimestamp, testVal);
		// p.add(testFamily, regionObserverMarkerColumn, genericTimestamp, testVal);
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
