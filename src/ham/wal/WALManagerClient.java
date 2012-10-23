package ham.wal;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class WALManagerClient {
  private static final Log log = LogFactory.getLog(WALManagerClient.class);

  public WALManagerClient() {
  }

  public Snapshot start(final HTable table, final LogId id) throws Throwable {
    class StartCallBack implements Batch.Callback<Snapshot> {
    	Snapshot snapshot = null;

      Snapshot getSnapshot() {
        return snapshot;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, Snapshot result) {
      	// Since this coprocessorExec call will only go to one region hosting that LogId,
      	// there will be only one Call
      	this.snapshot = result;
      }
    }
    StartCallBack aStartCallBack = new StartCallBack();
    table.coprocessorExec(WALManagerProtocol.class, id.getKey(), id.getKey(), new Batch.Call<WALManagerProtocol, Snapshot>() {
      @Override
      public Snapshot call(WALManagerProtocol instance) throws IOException {
        return instance.start(id);
      }
    }, aStartCallBack);
    return aStartCallBack.getSnapshot();
  }
  
  public long startTime(final HTable table, final LogId id) throws Throwable {
    class StartCallBack implements Batch.Callback<Long> {
    	Long startTime = null;

      Long getStartTime() {
        return startTime;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, Long result) {
      	// Since this coprocessorExec call will only go to one region hosting that LogId,
      	// there will be only one Call
      	this.startTime = result;
      }
    }
    StartCallBack aStartCallBack = new StartCallBack();
    table.coprocessorExec(WALManagerProtocol.class, id.getKey(), id.getKey(), 
    		new Batch.Call<WALManagerProtocol, Long>() {
      @Override
      public Long call(WALManagerProtocol instance) throws IOException {
        return instance.startTime(id);
      }
    }, aStartCallBack);
    return aStartCallBack.getStartTime();
  }
  
  public boolean commit(final HTable table, final LogId id, final Check check,
  		final List<Write> writes, final List<ImmutableBytesWritable> toBeUnlockedKeys,
  		final List<Integer> commitTypeInfo) throws Throwable {
    class CommitCallBack implements Batch.Callback<Boolean> {
    	Boolean commitResult = null;

      Boolean getCommitResult() {
        return commitResult;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, Boolean result) {
      	// Since this coprocessorExec call will only go to one region hosting that LogId,
      	// there will be only one Call
      	this.commitResult = result;
      }
    }
    CommitCallBack aCommitCallBack = new CommitCallBack();
    table.coprocessorExec(WALManagerProtocol.class, id.getKey(), id.getKey(), 
    		new Batch.Call<WALManagerProtocol, Boolean>() {
      @Override
      public Boolean call(WALManagerProtocol instance) throws IOException {
        return instance.commit(id, check, writes, toBeUnlockedKeys, commitTypeInfo);
      }
    }, aCommitCallBack);
    return aCommitCallBack.getCommitResult();
  }  
}
