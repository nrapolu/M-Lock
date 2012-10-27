package ham.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class LogId implements Writable {
	private byte[] name = null;
	private byte[] key = null;
	private int commitType = 0;
	
	final static int ONLY_DELETE = 1;
	final static int ONLY_UNLOCK = 2;
	final static int UNLOCK_AND_RESET_MIGRATION = 3;
	
	public LogId(LogId otherLogId) {
		this.commitType = otherLogId.commitType;
		this.name = otherLogId.name;
		this.key = otherLogId.key;
	}
	
	public LogId() {
		super();
		// TODO Auto-generated constructor stub
	}

	public byte[] getName() {
		return name;
	}
	public void setName(byte[] name) {
		this.name = name;
	}
	public byte[] getKey() {
		return key;
	}
	public void setKey(byte[] key) {
		this.key = key;
	}
	
	public int getCommitType() {
		return this.commitType;
	}
	
	public void setCommitType(int commitType) {
		this.commitType = commitType;
	}
	
	static LogId findLogId(byte[] entityKey) {
		// Use a map to determine the LogId for the given entity.
		LogId logId = new LogId();
		// TO-DO: fill the logId object.
		return logId;
	}
	
	@Override
	public String toString() {
		return "LogId [key=" + Bytes.toString(key) + ", name="
				+ Bytes.toString(name) + "]";
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = Bytes.readByteArray(in);
		this.key = Bytes.readByteArray(in);
		this.commitType = in.readInt();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		Bytes.writeByteArray(out, name);
		Bytes.writeByteArray(out, key);
		out.writeInt(commitType);
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Bytes.hashCode(key);
		if (name != null)
			result = prime * result + Bytes.hashCode(name);
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LogId other = (LogId) obj;
		if (!Bytes.equals(key, other.key))
			return false;
		if (!Bytes.equals(name, other.name))
			return false;
		if (commitType != other.commitType)
			return false;
		return true;
	}
}
