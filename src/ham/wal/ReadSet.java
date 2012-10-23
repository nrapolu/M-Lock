package ham.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class ReadSet implements Writable {
	private byte[] name = null;
	private Set<ImmutableBytesWritable> keys = null;
	public byte[] getName() {
		return name;
	}
	public void setName(byte[] name) {
		this.name = name;
	}
	public Set<ImmutableBytesWritable> getKeys() {
		return keys;
	}
	public void setKeys(Set<ImmutableBytesWritable> keys) {
		this.keys = keys;
	}
	
	String printKeys() {
		String str = "";
		if (keys != null) {
			for (ImmutableBytesWritable key: keys) {
				str = str + " " + Bytes.toString(key.get());
			}
		} else {
			System.err.println("Keys is null :(");
		}
		return str;
	}
	
	@Override
	public String toString() {
		return "ReadSet [keys=" + printKeys() + ", name=" + Bytes.toString(name) + "]";
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = Bytes.readByteArray(in);
		this.keys = new HashSet<ImmutableBytesWritable>();
		int keysSetSize = in.readInt();
		for (long i = 0; i < keysSetSize; i++) {
			ImmutableBytesWritable key = new ImmutableBytesWritable();
			key.readFields(in);
			keys.add(key);
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		Bytes.writeByteArray(out, name);
		out.writeInt(keys.size());
		for (ImmutableBytesWritable key: keys) {
			key.write(out);
		}
	}	
}
