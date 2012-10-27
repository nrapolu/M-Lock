package ham.wal;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.regionserver.HRegion;

public interface WALManagerProtocol extends CoprocessorProtocol {
	// Transaction start
	public Snapshot start(LogId id) throws IOException;

	public long startTime(LogId id) throws IOException;

	// Transaction commit
	boolean commit(LogId id, Check check, List<Write> writes,
			List<ImmutableBytesWritable> toBeUnlockedKeys,
			List<Integer> commitTypeInfo) throws IOException;

	// Add a lock to myKVSpace.
	boolean addLock(LogId logId, ImmutableBytesWritable key) throws IOException;

	// Migrate the lock for key from sourceLogId to destKey at destLogId.
	ImmutableBytesWritable migrateLock(final Long transactionId, final LogId logId,
			final ImmutableBytesWritable key, final LogId destLogId,
			final ImmutableBytesWritable destKey) throws IOException;
}
