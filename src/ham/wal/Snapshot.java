package ham.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

public class Snapshot implements Writable {
	private static final Log LOG = LogFactory
		.getLog("ham.wal.Snapshot");
	private long timestamp;
	private Map<String, Write> writeMap = null;
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public Map<String, Write> getWriteMap() {
		return writeMap;
	}
	public void setWriteMap(Map<String, Write> writeMap) {
		this.writeMap = writeMap;
	}
	
	public boolean isEmpty() {
		return writeMap == null || writeMap.isEmpty();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.timestamp = in.readLong();
		LOG.debug("In snapshot, read timestamp: " + this.timestamp);
		this.writeMap = new HashMap<String, Write>();
		int writeMapSize = in.readInt();
		LOG.debug("In snapshot, read writeMapSize: " + writeMapSize);
		for (int i = 0; i < writeMapSize; i++) {
			LOG.debug("In snapshot, reading key-values");
			String key = in.readUTF();
			LOG.debug("Key: " + key);
			Write w = new Write();
			w.readFields(in);
			this.writeMap.put(key, w);
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(timestamp);
		LOG.debug("In snapshot, writing timestamp: " + this.timestamp);
		out.writeInt(writeMap.size());
		LOG.debug("In snapshot, writing writeMap size: " + this.writeMap.size());
		for (Map.Entry<String, Write> writeMapEntry: writeMap.entrySet()) {
			LOG.debug("In snapshot, writing entries from writeMap.");
			out.writeUTF(writeMapEntry.getKey());
			writeMapEntry.getValue().write(out);
		}
	}
}
