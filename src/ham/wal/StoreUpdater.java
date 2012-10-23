package ham.wal;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.HasThread;

public class StoreUpdater extends HasThread {
	private static final Log LOG = LogFactory.getLog("ham.wal.StoreUpdater");
	private int flushPeriod;
	private WALManagerEndpoint walManager;
	private AtomicBoolean flushLog = new AtomicBoolean(false);
	private final ReentrantLock flushLock = new ReentrantLock();
	private volatile long lastflushtime = System.currentTimeMillis();
	private final int threadWakeFrequency;

	public StoreUpdater(WALManagerEndpoint walManager, int flushPeriod,
			int threadWakeFrequency) {
		this.walManager = walManager;
		this.flushPeriod = flushPeriod;
		this.threadWakeFrequency = threadWakeFrequency;
	}

	@Override
	public void run() {
		while (!walManager.isStopped()) {
			long now = System.currentTimeMillis();
			boolean periodic = false;
			if (!flushLog.get()) {
				periodic = (now - this.lastflushtime) > this.flushPeriod;
				if (!periodic) {
					synchronized (flushLog) {
						try {
							flushLog.wait(this.threadWakeFrequency);
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
				walManager.flushLogs();
			} catch (Exception ex) {
				LOG.error("Log flush failed", ex);
			} finally {
				flushLog.set(false);
				flushLock.unlock();
			}
		}
	}

	public void logFlushRequested() {
		synchronized (flushLog) {
			flushLog.set(true);
			flushLog.notifyAll();
		}
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
