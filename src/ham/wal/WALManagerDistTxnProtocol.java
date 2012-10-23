package ham.wal;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;

public interface WALManagerDistTxnProtocol extends WALManagerProtocol {
  List<Long> commitRequestAcquireLocks(Long transactionId, List<ImmutableBytesWritable> keys)
		throws IOException;
  
  Long commitRequestAcquireLocksFromWAL(Long transactionId, 
  		List<LogId> logs, List<List<ImmutableBytesWritable>> keys)
		throws IOException;
  
  Long abortRequestReleaseLocksFromWAL(Long transactionId, 
  		List<LogId> logs, List<List<ImmutableBytesWritable>> keys)
		throws IOException;
  
  Boolean commitWritesPerEntityGroup(Long transactionId, LogId logId, 
  		List<ImmutableBytesWritable> keyList, List<ImmutableBytesWritable> shadowKeyList) 
  	throws IOException;
  
  Boolean commitWritesPerEntityGroupWithoutShadows(Long transactionId, 
  		List<LogId> logIdList, List<List<Put>> allUpdates, 
  		List<List<LogId>> toBeUnlockedDestLogIds) 
  	throws IOException;
  
  Boolean commitWritesPerEntityGroupWithShadows(Long transactionId, 
  		List<LogId> logIdList, List<List<Put>> allUpdates, 
  		List<List<LogId>> toBeUnlockedDestLogIds) 
  	throws IOException;
  
  Boolean abortWithoutShadows(Long transactionId, 
  		List<LogId> logIdList, List<List<ImmutableBytesWritable>> allKeysToBeUnlocked, 
  		List<List<LogId>> toBeUnlockedDestLogIds) 
  	throws IOException;
  
  List<Snapshot> getSnapshotsForLogIds(List<LogId> logIdList) throws IOException;
  
  List<List<Boolean>> migrateLocks(final Long transactionId, final List<LogId> logs, 
  		final List<List<ImmutableBytesWritable>> keys, final LogId destLogId) throws IOException;

  List<List<Result>> getAfterServerSideMerge(final List<LogId> logs, final List<List<Get>> gets) 
  	throws IOException;
  
  // 1. If the result list contains only two values, then all locks that were sent are locked and the value
  // 		refers to the number of locks acquired. The first value, which is a flag, will have the value
	//    as 0.
  // 2. If the result list contains three values, then the third value would be the transactionId that
  //    acquired the lock. The client needs to retry at this same key site to acquire the lock.
	//    The flag value (first value in the list) will be of value 1.
  // 3. If the result list contains four values, then the fourth value would be the destination key
  // 		at which this lock can be found. The client needs to directly contact that key to acquire the
  //    lock. The flag value (first value in the list) will be of value 2.
	// 4. If the result contains only two values and the flag (first value) is 3, then the detour
	//    was deleted and so we need to lookup in our dynamic map to find the original LogId mapping
	//    for this key.
  List<ImmutableBytesWritable> commitRequestAcquireLocksViaIndirection(Long transactionId, List<LogId> logs, 
			List<ImmutableBytesWritable> keys, List<Boolean> isKeyMigrated) throws IOException;
}
