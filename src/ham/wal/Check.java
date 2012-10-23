package ham.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class Check implements Writable {
	private long timestamp;
	private List<ReadSet> readSets = null;
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public List<ReadSet> getReadSets() {
		return readSets;
	}
	public void setReadSets(List<ReadSet> readSets) {
		this.readSets = readSets;
	}
	
	String printReadSets() {
		String str = "";
		if (readSets != null) {
			for (ReadSet r: readSets) {
				str = str + " " + r.toString();
			}
		}
		return str;
	}
	
	@Override
	public String toString() {
		return "Check [readSets=" + printReadSets() + ", timestamp=" + timestamp + "]";
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.timestamp = in.readLong();
		int readSetsLen = in.readInt();
		readSets = new LinkedList<ReadSet>();
		for (int i = 0; i < readSetsLen; i++) {
			ReadSet r = new ReadSet();
			r.readFields(in);
			readSets.add(r);
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(timestamp);
		out.writeInt(readSets.size());
		for (ReadSet r: readSets) {
			r.write(out);
		}
	}
}
