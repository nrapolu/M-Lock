package ham.wal.scheduler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.client.coprocessor.*;

public class RequestPriority implements Comparable<RequestPriority>, Writable {
	// The following are the possible transaction phase Ids. 
	public static final int OCC_READ_PHASE = 1;
	public static final int OCC_PUT_SHADOWS = 2;
	public static final int OCC_PUT_METADATA = 3;
	public static final int OCC_ACQUIRE_LOCKS = 4;
	public static final int OCC_CHECK_VERSIONS = 5;
	public static final int OCC_COMMIT = 6;
	public static final int OCC_ABORT = 7;
	
	public static final int PCC_PUT_PRE_METADATA = 1;
	public static final int PCC_ACQUIRE_LOCKS = 2;
	public static final int PCC_PUT_POST_METADATA = 3; 
	public static final int PCC_READ_PHASE = 4;
	public static final int PCC_COMMIT = 5; 
	
	int phaseId = 0;
	long trxTimestamp = 0;
	float lockPhaseProgress = 0;
	
	String tag = null;

	public RequestPriority(int phaseId, long trxTimestamp, float lockPhaseProgress) {
		super();
		this.phaseId = phaseId;
		this.trxTimestamp = trxTimestamp;
		this.lockPhaseProgress = lockPhaseProgress;
	}

	public void setExecRequestPriorityTag() {
		Exec exec = new Exec();
		exec.setRequestPriorityTag(getRequestPriorityTag());
	}
	
	public int getPhaseId() {
		return phaseId;
	}

	public void setPhaseId(int phaseId) {
		this.phaseId = phaseId;
	}

	public long getTrxTimestamp() {
		return trxTimestamp;
	}

	public void setTrxTimestamp(long trxTimestamp) {
		this.trxTimestamp = trxTimestamp;
	}

	public float getLockPhaseProgress() {
		return lockPhaseProgress;
	}

	public void setLockPhaseProgress(float lockPhaseProgress) {
		this.lockPhaseProgress = lockPhaseProgress;
	}

	public String getRequestPriorityTag() {
		if (this.tag != null)
			return tag;
		
		String tagSeparator = "&";
		// Do not add present trxTimestamp. Instead the request will be added the arrival 
		// timestamp at the server. In this way, similar requests with similar progress will
		// be serviced in FIFO order by the server.
		this.tag = phaseId + tagSeparator + String.format("%.2f", lockPhaseProgress);
		// + tagSeparator + String.format("%015d", trxTimestamp);
		return tag;
	}
	
	@Override
	public int compareTo(RequestPriority other) {
		// Ordering should be reverse -- a request closer to its deadline (higher phase id)
		// should receive priority.
		return -1 * getRequestPriorityTag().compareToIgnoreCase(other.getRequestPriorityTag());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.phaseId = in.readInt();
		this.lockPhaseProgress = in.readFloat();
		this.trxTimestamp = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.phaseId);
		out.writeFloat(this.lockPhaseProgress);
		out.writeLong(this.trxTimestamp);
	}
}
