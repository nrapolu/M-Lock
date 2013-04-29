package ham.wal;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.HasThread;

public class RemoteWALCommunicator extends HasThread {
	LinkedBlockingQueue<Row> causalLockReleases = new LinkedBlockingQueue<Row>();
	HTable logTable = null;
	HBaseAdmin admin = null;
	boolean stop = false;
	
	private static final Log LOG = LogFactory.getLog("ham.wal.RemoteWALCommunicator");
	private int flushPeriod;
	private WALManagerEndpointForMyKVSpace walManager;
	private AtomicBoolean flushLog = new AtomicBoolean(false);
	private final ReentrantLock flushLock = new ReentrantLock();
	private volatile long lastflushtime = System.currentTimeMillis();
	private final int threadWakeTime;
	private Configuration conf = null;
	private LinkedList<Row> drainContainer = new LinkedList<Row>();
	
	private static void sysout(String otp) {
		//System.out.println(otp);
	}
	
	public RemoteWALCommunicator(WALManagerEndpointForMyKVSpace walManager,
			int flushPeriod, int threadWakeTime, Configuration conf) throws IOException {
		this.walManager = walManager;
		this.flushPeriod = flushPeriod;
		this.threadWakeTime = threadWakeTime;
		this.conf = conf;
		this.admin = new HBaseAdmin(this.conf);
	}

	public void addToCausalReleasesQueue(List<Row> causalLocks) {
		this.causalLockReleases.addAll(causalLocks);
	}

	@Override
	public void run() {
		while (!stop && !walManager.isStopped()) {
			long now = System.currentTimeMillis();
			boolean periodic = false;
			if (!flushLog.get()) {
				periodic = (now - this.lastflushtime) > this.flushPeriod;
				if (!periodic) {
					synchronized (flushLog) {
						try {
							flushLog.wait(this.threadWakeTime);
						} catch (InterruptedException e) {
							// Fall through
						}
					}
					continue;
				}
				// Time for periodic roll
				if (LOG.isDebugEnabled()) {
					LOG.debug("Log flush period " + this.flushPeriod + "ms elapsed");
				}
			} else if (LOG.isDebugEnabled()) {
				LOG.debug("Log flush requested");
			}
			flushLock.lock(); // FindBugs UL_UNRELEASED_LOCK_EXCEPTION_PATH
			try {
				this.lastflushtime = now;
				flushToRemoteWALs();
			} catch (Exception ex) {
				LOG.error("Log flush failed", ex);
			} finally {
				flushLog.set(false);
				flushLock.unlock();
			}
		}
	}

	public void flushToRemoteWALs() throws InterruptedException, IOException {
		if (this.logTable == null) {
			// Check if logTable is created. If it is not, simply return. We can flush once
			// it is created.
			boolean isTableAvailable = admin.isTableAvailable(WALTableProperties.WAL_TABLENAME);
			if (!isTableAvailable) {
				return;
			} else {
				// Create an instance of the table.
				this.logTable = new HTable(this.conf, WALTableProperties.walTableName);
			}
		}
		
		causalLockReleases.drainTo(drainContainer);
		if (!drainContainer.isEmpty()) {
			sysout("Draining Lock releases with size: " + drainContainer.size());
			logTable.batch(drainContainer);
			drainContainer.clear();
		}
	}
	
	public void askToStop() {
		stop = true;
	}
	
	/**
	 * Called by WAL Manager to wake up this thread if it sleeping. It is
	 * sleeping if flushLock is not held.
	 */
	public void interruptIfNecessary() {
		try {
			flushLock.lock();
			this.interrupt();
		} finally {
			flushLock.unlock();
		}
	}

}
