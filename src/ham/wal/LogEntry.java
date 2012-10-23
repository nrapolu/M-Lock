package ham.wal;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

public class LogEntry {
	private long timestamp;
	private List<Write> writes;
	final static String delimiter = "@";
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public List<Write> getWrites() {
		return writes;
	}
	
	public void setWrites(List<Write> writes) {
		this.writes = writes;
	}
	
	public byte[] toBytes() {
		String allInString = Long.toString(timestamp);
		for (Write w: writes) {
			allInString = allInString + delimiter + w.toString();
		}
		return Bytes.toBytes(allInString);
	}
	
	public static LogEntry fromBytes(byte[] allInBytes) {
		String splitDelimiter = "[" + delimiter + "]+";
		String[] tokens = Bytes.toString(allInBytes).split(splitDelimiter);
		long timestamp = Long.parseLong(tokens[0]);
		List<Write> writes = new LinkedList<Write>();
		for (int i = 1; i < tokens.length; i++) {
			writes.add(Write.fromString(tokens[i]));
		}
		
		LogEntry newLogEntry = new LogEntry();
		newLogEntry.setTimestamp(timestamp);
		newLogEntry.setWrites(writes);
		return newLogEntry;
	}	
}
