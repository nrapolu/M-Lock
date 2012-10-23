package ham.wal;

public class DistTrxExecutorReturnVal {
	public long abortCount = 0;
	public long numOfLocksAcquired = 0;
	public long numOfSuccessfulLocksAcquired = 0;
	public long numOfWastedLocksAcquired = 0;
	public long readTime = 0;
	public long lockingTime = 0;
	public long versionCheckTime = 0;
	public long commitTime = 0;
	public String[] tokens = null;
	public int returnId;
	public long numOfUnsuccessfulAttempts = 0;
	public long unsuccessfulAttemptsTime = 0;
	public long numOflogsAcquired = 0;
	public long putShadowTime = 0;
	public long putTxnStateTime = 0;
	public long firstPutTxnStateTime = 0;
	public long secondPutTxnStateTime = 0;
	
	public long nbDetoursEncountered = 0;
	public long nbNetworkRoundTripsInTotalForLocking = 0;
	public long lockMigrationTime = 0;
	
	public long getFirstPutTxnStateTime() {
		return firstPutTxnStateTime;
	}

	public void setFirstPutTxnStateTime(long firstPutTxnStateTime) {
		this.firstPutTxnStateTime = firstPutTxnStateTime;
	}

	public long getSecondPutTxnStateTime() {
		return secondPutTxnStateTime;
	}

	public void setSecondPutTxnStateTime(long secondPutTxnStateTime) {
		this.secondPutTxnStateTime = secondPutTxnStateTime;
	}

	public long getLockMigrationTime() {
		return lockMigrationTime;
	}

	public void setLockMigrationTime(long lockMigrationTime) {
		this.lockMigrationTime = lockMigrationTime;
	}

	public long getNbNetworkRoundTripsInTotalForLocking() {
		return nbNetworkRoundTripsInTotalForLocking;
	}

	public void setNbNetworkRoundTripsInTotalForLocking(
			long nbNetworkRoundTripsInTotalForLocking) {
		this.nbNetworkRoundTripsInTotalForLocking = nbNetworkRoundTripsInTotalForLocking;
	}

	public long getNbDetoursEncountered() {
		return nbDetoursEncountered;
	}

	public void setNbDetoursEncountered(long nbDetoursEncountered) {
		this.nbDetoursEncountered = nbDetoursEncountered;
	}

	public DistTrxExecutorReturnVal(long abortCount) {
		this.abortCount = abortCount;
		this.numOfLocksAcquired = 0;
		this.numOfSuccessfulLocksAcquired = 0;
		this.numOfWastedLocksAcquired = 0;
	}

	public DistTrxExecutorReturnVal(long abortCount, long numOfLocksAcquired) {
		this.abortCount = abortCount;
		this.numOfLocksAcquired = numOfLocksAcquired;
	}

	public DistTrxExecutorReturnVal(String[] tokens, long abortCount,
			long numOfLocksAcquired) {
		this.tokens = tokens;
		this.abortCount = abortCount;
		this.numOfLocksAcquired = numOfLocksAcquired;
	}

	public void setNumOflogsAcquired(long numOflogsAcquired) {
		this.numOflogsAcquired = numOflogsAcquired;
	}

	public void setLockingTime(long lockingTime) {
		this.lockingTime = lockingTime;
	}

	public void setTokens(String[] tokens) {
		this.tokens = tokens;
	}

	public void setNumOfSuccessfulLocksAcquired(long numOfSuccessfulLocksAcquired) {
		this.numOfSuccessfulLocksAcquired = numOfSuccessfulLocksAcquired;
	}

	public void setNumOfWastedLocksAcquired(long numOfWastedLocksAcquired) {
		this.numOfWastedLocksAcquired = numOfWastedLocksAcquired;
	}

	public void setNumOfUnsuccessfulAttempts(long numOfUnsuccessfulAttempts) {
		this.numOfUnsuccessfulAttempts = numOfUnsuccessfulAttempts;
	}

	public void setReturnId(int returnId) {
		this.returnId = returnId;
	}

	public long getReadTime() {
		return readTime;
	}

	public void setReadTime(long readTime) {
		this.readTime = readTime;
	}

	public long getVersionCheckTime() {
		return versionCheckTime;
	}

	public void setVersionCheckTime(long versionCheckTime) {
		this.versionCheckTime = versionCheckTime;
	}

	public long getCommitTime() {
		return commitTime;
	}

	public void setCommitTime(long commitTime) {
		this.commitTime = commitTime;
	}

	public long getLockingTime() {
		return lockingTime;
	}

	public void setPutShadowTime(long putShadowTime) {
		// TODO Auto-generated method stub
		this.putShadowTime = putShadowTime;
	}

	public void setPutTxnStateTime(long putTxnStateTime) {
		// TODO Auto-generated method stub
		this.putTxnStateTime = putTxnStateTime;
	}

	public long getPutShadowTime() {
		return putShadowTime;
	}

	public long getPutTxnStateTime() {
		return putTxnStateTime;
	}
}
