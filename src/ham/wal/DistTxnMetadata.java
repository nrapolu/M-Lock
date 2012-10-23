package ham.wal;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;

public class DistTxnMetadata {
	static int init = 0;
	static int ready = 1;
	static int locked = 2;
	static int checked = 3;
	static int done = 4;
	static int aborting = 5;
	static int aborted = 6;
	
	private List<Pair<ImmutableBytesWritable, Long>> readVersionList = 
		new LinkedList<Pair<ImmutableBytesWritable, Long>>();
	private List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> putShadowList = 
		new LinkedList<Pair<ImmutableBytesWritable, ImmutableBytesWritable>>();
	
	private int mode;
	private long transactionId;
	
	public DistTxnMetadata(long transactionId) {
		this.transactionId = transactionId;
		this.mode = init;
	}
	
	public long getTransactionId() {
		return transactionId;
	}
	
	public void addReadInfo(ImmutableBytesWritable key, long version) {
		readVersionList.add(new Pair<ImmutableBytesWritable, Long>(key, version));
	}
	
	public void addWriteInfo(ImmutableBytesWritable key, ImmutableBytesWritable shadowKey) {
		putShadowList.add(new Pair<ImmutableBytesWritable, ImmutableBytesWritable>(key, shadowKey));
	}
	
	public List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> getPutShadowList() {
		return this.putShadowList;
	}
	
	public List<Pair<ImmutableBytesWritable, Long>> getReadVersionList() {
		return this.readVersionList;
	}
}
