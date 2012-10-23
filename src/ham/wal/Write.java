package ham.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class Write implements Writable{
	private byte[] name;
	private byte[] key;
	private byte[] value;
	final static String nameDelimiter = "%";
	final static String varDelimiter = "$";
	
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
	
	public byte[] getValue() {
		return value;
	}
	
	public void setValue(byte[] value) {
		this.value = value;
	}
	
	public String getNameAndKey() {
		return Bytes.toString(name) + varDelimiter + Bytes.toString(key);
	}
	
	public static String getNameAndKey(byte[] name, byte[] key) {
		return Bytes.toString(name) + varDelimiter + Bytes.toString(key);
	}
	
	public String toString() {
		return Bytes.toString(name) + varDelimiter + Bytes.toString(key) +
			varDelimiter + Bytes.toString(value);
	}
	
	public static Write fromString(String w) {
		String[] tokens = w.split("[" + varDelimiter + "]+");
		Write newWrite = new Write();
		newWrite.setName(Bytes.toBytes(tokens[0]));
		newWrite.setKey(Bytes.toBytes(tokens[1]));
		newWrite.setValue(Bytes.toBytes(tokens[2]));
		return newWrite;
	}
	
	@Override
	public boolean equals(Object obj) {
		Write w = (Write) obj;
		return Bytes.equals(w.name, this.name) && Bytes.equals(w.key, key);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = Bytes.readByteArray(in);
		this.key = Bytes.readByteArray(in);
		this.value = Bytes.readByteArray(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Bytes.writeByteArray(out, name);
		Bytes.writeByteArray(out, key);
		Bytes.writeByteArray(out, value);
	}	
}
