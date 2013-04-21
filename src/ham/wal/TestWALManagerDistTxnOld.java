package ham.wal;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class TestWALManagerDistTxnOld {
	static Hashtable<String, String> logRoutingTable = new Hashtable<String, String>();

	WALTableProperties tableProperties = null;
	Configuration conf = null;
	HBaseAdmin admin = null;
	byte[] dataTableName = WALTableProperties.dataTableName;
	byte[] walTableName = WALTableProperties.walTableName;
	byte[] dataFamily = WALTableProperties.dataFamily;
	byte[] dataColumn = WALTableProperties.dataColumn;

	public TestWALManagerDistTxnOld() throws IOException {
		this.conf = (Configuration) HBaseConfiguration.create();
		this.admin = new HBaseAdmin(conf);
	}

	/*
	 * public void populateDefaultlogRoutingEntries(long numEntries) throws
	 * IOException { // In the default case, each log is in its own partition; no
	 * coalescing. HTable hlogTable = new HTable(conf, logTableName);
	 * hlogTable.setWriteBufferSize(numEntries / 10); for (long i = 0; i <
	 * numEntries; i++) { Put p = new Put(Bytes.toBytes(Long.toString(i)));
	 * p.add(logFamily, logColumn, Bytes.toBytes(Long.toString(i)));
	 * p.setWriteToWAL(false); hlogTable.put(p); } hlogTable.close();
	 * System.out.println("Wrote default routing data!"); }
	 * 
	 * public void populatelogRoutingEntriesFromFile(String fileName, long
	 * numEntries) throws IOException { // File format : line number is the log
	 * row key, and the value is the // coalesced // log which need to be acquired
	 * before writing to the row key. // We write this data in the logTable, in
	 * logFamily and logColumn. BufferedReader bin = new BufferedReader(new
	 * FileReader(fileName));
	 * 
	 * HTable hlogTable = new HTable(conf, logTableName);
	 * hlogTable.setWriteBufferSize(numEntries / 10);
	 * 
	 * for (long i = 0; i < numEntries; i++) { String line = bin.readLine(); if
	 * (line == null) break; long mappedTolog = Long.parseLong(line.trim());
	 * 
	 * Put p = new Put(Bytes.toBytes(Long.toString(i))); p.add(logFamily,
	 * logColumn, Bytes.toBytes(Long .toString(mappedTolog)));
	 * p.setWriteToWAL(false); hlogTable.put(p); } hlogTable.close(); bin.close();
	 * }
	 * 
	 * public void populateLocalTableFromCentrallogRoutingTable(long numEntries)
	 * throws IOException { HTable hlogTable = new HTable(conf, logTableName);
	 * ResultScanner scanner = hlogTable.getScanner(logFamily, logColumn);
	 * Iterator<Result> scannerItr = scanner.iterator(); int scannerCount = 0;
	 * while (scannerItr.hasNext()) { Result rowResult = scannerItr.next(); String
	 * rowKey = Bytes.toString(rowResult.getRow()); String rowVal = Bytes
	 * .toString(rowResult.getValue(logFamily, logColumn));
	 * logRoutingTable.put(rowKey, rowVal); scannerCount++; } scanner.close();
	 * 
	 * if (scannerCount != numEntries) {
	 * System.err.println("Scanner couldn`t fetch all entries"); }
	 * System.out.println("Scanned and populated local routing hash table"); }
	 */

	private DistTrxExecutorReturnVal executeDistTrxFromFileUsingSingleThread(
			String fileName, boolean doMigrateLocks, int thinkingTime,
			long countOfTrxToBeExecuted, int lenOfTrx, int contentionOrder)
			throws Exception {
		DistTxnState.contentionOrder = contentionOrder;
		long abortCount = 0;
		long numOfSuccessfulLocksAcquired = 0;
		long numOfWastedLocksAcquired = 0;

		BufferedReader bin = new BufferedReader(new FileReader(fileName));
		String trxLine = null;
		countOfTrxToBeExecuted = (countOfTrxToBeExecuted < 0) ? Long.MAX_VALUE
				: countOfTrxToBeExecuted;
		// globalTrxCount denotes the number of Trx read and executed from file.
		long globalTrxCount = 0;
		long countOfExecutedTrx = 0;
		long readTime = 0;
		long putShadowTime = 0;
		long putTxnStateTime = 0;
		long lockingTime = 0;
		long versionCheckTime = 0;
		long commitTime = 0;
		long numOfUnsuccessfulAttempts = 0;
		long nbDetoursEncountered = 0;
		long nbNetworkRoundTripsInTotalForLocking = 0;
		long lockMigrationTime = 0;
		// Sleeping time should ideally be the same as the time taken to acquire a
		// lock,
		// when conforming to the simulation.
		long sleepingTime = 0;
		int threadId = 0;

		// Irrespective of the contention order, we always generate a
		// randomizedLockMap for load balancing
		// transactional accesses -- YCSB assumes 0 as the highest-contented and 1
		// as the next contented etc.,
		// This leads to heavy hitting on any one node which hosts the region
		// containing all 1's (11 - 19).
		// Randomization spreads all this impact (hopefully :))
		// randomizeLocksForLoadBalancing(numDataEntries);

		// As it is a single thread, we fetch all the Trx, put it in a queue
		// and execute them one by one. No threads, no futures, and no polling.
		LinkedList<String> trxQueue = new LinkedList<String>();
		LinkedList<String[]> waitingQueue = new LinkedList<String[]>();
		int maxWaitingQueueSize = 5;

		// Single HTable instance used multiple times by the thread. No creation/deletion
		// for every transaction.
		HTable dataTable = new HTable(conf, WALTableProperties.dataTableName);
		HTable walTable = new HTable(conf, WALTableProperties.walTableName);
		
		// Change this accordingly.
		WALManagerDistTxnClient walManagerDistTxnClient = new WALManagerDistTxnClient();
		//WALManagerDistTxnClient walManagerDistTxnClient = new WALManagerDistTxnClientRefactored();

		while (true) {
			int trxQueueSize = 500;
			if (trxQueueSize + globalTrxCount > countOfTrxToBeExecuted)
				trxQueueSize = (int) (countOfTrxToBeExecuted - globalTrxCount);

			globalTrxCount += trxQueueSize;
			while ((trxLine = bin.readLine()) != null && trxQueueSize > 0) {
				trxQueue.add(trxLine);
				trxQueueSize--;
			}

			// Execute all the trx in the trxQueue in an iterative manner.
			while (!trxQueue.isEmpty() || !waitingQueue.isEmpty()) {
				// We first push maxWaitingQueueSize number of trx into the
				// waitingQueue and keep executing from there.
				// If a trx aborts, add it to its end.
				// We add even when there is 1 trx, to avoid the case where a single trx
				// aborts
				// and waits as the first guy, and then aborts and again waits etc.
				if (waitingQueue.size() <= 1) {
					for (int i = 0; i < maxWaitingQueueSize && !trxQueue.isEmpty(); i++) {
						String trx = trxQueue.pollFirst();
						String[] tokens = trx.trim().split("\\s+");
						waitingQueue.addFirst(tokens);
					}
				}

				String[] tokens = waitingQueue.pollLast();
				lenOfTrx = tokens.length;

				// TPCCPessimisticNewOrderTrxExecutor trxExecutor = new
				// TPCCPessimisticNewOrderTrxExecutor(
				// TPCCNewOrderTrxExecutor trxExecutor = new TPCCNewOrderTrxExecutor(
				TPCCNewOrderTrxExecutorClusteredPartitioning trxExecutor = new TPCCNewOrderTrxExecutorClusteredPartitioning(
						tokens, dataTable, walTable,
						walManagerDistTxnClient, thinkingTime, lenOfTrx, contentionOrder,
						doMigrateLocks);
				threadId++;
				DistTrxExecutorReturnVal trxExecReturnVal = trxExecutor.call();
				if (trxExecReturnVal.tokens == null) {
					abortCount += trxExecReturnVal.abortCount;
					numOfSuccessfulLocksAcquired += trxExecReturnVal.numOfLocksAcquired;
					numOfUnsuccessfulAttempts += trxExecReturnVal.numOfUnsuccessfulAttempts;
					nbDetoursEncountered += trxExecReturnVal.nbDetoursEncountered;
					nbNetworkRoundTripsInTotalForLocking += trxExecReturnVal.nbNetworkRoundTripsInTotalForLocking;
					lockMigrationTime += trxExecReturnVal.lockMigrationTime;
					countOfExecutedTrx++;
					readTime += trxExecReturnVal.readTime;
					putShadowTime += trxExecReturnVal.putShadowTime;
					putTxnStateTime += trxExecReturnVal.putTxnStateTime;
					lockingTime += trxExecReturnVal.lockingTime;
					versionCheckTime += trxExecReturnVal.versionCheckTime;
					commitTime += trxExecReturnVal.commitTime;
				} else {
					// Get the count of wasted locks and put this in the waiting queue.
					abortCount += trxExecReturnVal.abortCount;
					String[] abortedTrxTokens = trxExecReturnVal.tokens;
					numOfUnsuccessfulAttempts += trxExecReturnVal.numOfUnsuccessfulAttempts;
					// Add the abortedTokens to waitingQueue. If the waitingQueue is full,
					// then the next trx must be from the waitingQueue, otherwise it
					// should
					// be from the trxQueue. The limited size of waitingQueue ensures that
					// each aborted
					// trx only waits for a small time.
					waitingQueue.addFirst(abortedTrxTokens);
					numOfWastedLocksAcquired += trxExecReturnVal.numOfLocksAcquired;
				}
			}
			if (globalTrxCount == countOfTrxToBeExecuted)
				break;
		}

		dataTable.close();
		walTable.close();
		bin.close();
		DistTrxExecutorReturnVal finalRetVal = new DistTrxExecutorReturnVal(
				abortCount);
		finalRetVal.setNumOfSuccessfulLocksAcquired(numOfSuccessfulLocksAcquired);
		finalRetVal.setNumOfWastedLocksAcquired(numOfWastedLocksAcquired);
		finalRetVal.setNumOfUnsuccessfulAttempts(numOfUnsuccessfulAttempts);
		finalRetVal.setNbDetoursEncountered(nbDetoursEncountered);
		finalRetVal
				.setNbNetworkRoundTripsInTotalForLocking(nbNetworkRoundTripsInTotalForLocking);
		finalRetVal.setLockMigrationTime(lockMigrationTime);
		finalRetVal.setReadTime(readTime);
		finalRetVal.setPutShadowTime(putShadowTime);
		finalRetVal.setPutTxnStateTime(putTxnStateTime);
		finalRetVal.setLockingTime(lockingTime);
		finalRetVal.setVersionCheckTime(versionCheckTime);
		finalRetVal.setCommitTime(commitTime);
		return finalRetVal;
	}

	public void printInventoryStatus() throws IOException {
		HTable hDataTable = new HTable(conf, dataTableName);
		ResultScanner scanner = hDataTable.getScanner(dataFamily, dataColumn);
		Iterator<Result> scannerItr = scanner.iterator();
		int scannerCount = 0;
		while (scannerItr.hasNext()) {
			Result rowResult = scannerItr.next();
			String rowKey = Bytes.toString(rowResult.getRow());
			long rowVal = Bytes.toLong(rowResult.getValue(dataFamily, dataColumn));
			System.out.println(rowKey + " : " + rowVal);
			scannerCount++;
		}
		scanner.close();
		System.out.println("ScannerCount: " + scannerCount);
	}

	private static void printUsage() {
		System.out
				.println("java TestWALManagerDistTxn <numDataEntriesInItemsTable> <Option[1,2,3]. "
						+ "1 - Write to table, "
						+ "2 - Populate default routing entries, "
						+ "3 - Populate routing entries from file, "
						+ "4 - Populate local routing table from central table, "
						+ "5 - Execute transactions from file. "
						+ " <AvgLenOfTrx> <TrxFile> <CountOfTrxToBeExecuted> [NumOfDataSplits] [DoMigrateLocks]"
						+ " [ContentionOrder] [NumItemsPerWarehouseTPCC] [IsClusteredPartitioning]");
		System.exit(0);
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 5)
			printUsage();

		try {
			long numDataEntries = Long.parseLong(args[0]);
			int lenOfTrx = Integer.parseInt(args[2]);
			TestWALManagerDistTxnOld hbaseTrxCli = new TestWALManagerDistTxnOld();
			String optionStr = args[1];
			long countOfTrxToBeExecuted = Long.parseLong(args[4]);
			int numDataSplits = 1;
			boolean doMigrateLocks = false;
			int contentionOrder = 0;
			int numItemsPerWarehouseTPCC = 100000;
			boolean isClusteredPartitioning = false;
			if (args.length >= 6)
				numDataSplits = Integer.parseInt(args[5]);
			if (args.length >= 7)
				doMigrateLocks = (Integer.parseInt(args[6]) == 1) ? true : false;
			if (args.length >= 8)
				contentionOrder = Integer.parseInt(args[7]);
			if (args.length >= 9) {
				numItemsPerWarehouseTPCC = Integer.parseInt(args[8]);
				TPCCTableProperties.numItemsPerWarehouse = numItemsPerWarehouseTPCC;
			}
			if (args.length >= 10) {
				isClusteredPartitioning = (Integer.parseInt(args[9]) == 1) ? true
						: false;
			}

			WALTableProperties walTableProps = null;

			if (isClusteredPartitioning)
				walTableProps = new TPCCTablePropertiesClusteredPartitioning(
						hbaseTrxCli.conf, hbaseTrxCli.admin);
			else
				walTableProps = new TPCCTableProperties(hbaseTrxCli.conf,
						hbaseTrxCli.admin);

			for (int i = 0; i < optionStr.length(); i++) {
				char option = optionStr.charAt(i);
				switch (option) {
				case '1':
					walTableProps.createAndPopulateTable(numDataEntries, numDataSplits);
					break;
				case '2':
					// hbaseTrxCli.populateDefaultlogRoutingEntries(numDataEntries);
					walTableProps.populateDataTableEntries(numDataEntries, false);
					walTableProps.populateLocksForDataTableEntries(numDataEntries);
					break;
				case '3':
					if (args.length < 8)
						printUsage();
					String routingFileName = args[7];
					// hbaseTrxCli.populatelogRoutingEntriesFromFile(routingFileName,
					// numDataEntries);
					walTableProps.populateDataTableEntries(numDataEntries, false);
					break;
				case '4':
					// hbaseTrxCli
					// .populateLocalTableFromCentrallogRoutingTable(numDataEntries);
					break;
				case '5':
					String trxFileName = null;
					trxFileName = args[3];
					int thinkingTime = 7;

					if (args.length == 9) {
						thinkingTime = Integer.parseInt(args[8]);
					}

					// Before executing the transactions, we shall first turn off the load
					// balancer.
					walTableProps.turnOffBalancer();

					long startTime = System.currentTimeMillis();
					DistTrxExecutorReturnVal stats = hbaseTrxCli
							.executeDistTrxFromFileUsingSingleThread(trxFileName,
									doMigrateLocks, thinkingTime, countOfTrxToBeExecuted,
									lenOfTrx, contentionOrder);
					long endTime = System.currentTimeMillis();

					long abortCount = stats.abortCount;
					double avgNumOflogsAcquiredPerTrx = (double) stats.numOflogsAcquired
							/ (double) countOfTrxToBeExecuted;
					double totalTimeTaken = (double) (endTime - startTime) / 1000;
					double restartRate = (double) abortCount / (double) totalTimeTaken;
					double throughput = (double) countOfTrxToBeExecuted
							/ (double) totalTimeTaken;
					double avgReadtime = (double) stats.readTime
							/ (double) countOfTrxToBeExecuted;
					double avgPutShadowTime = (double) stats.putShadowTime
							/ (double) countOfTrxToBeExecuted;
					double avgPutTxnStateTime = (double) stats.putTxnStateTime
							/ (double) countOfTrxToBeExecuted;
					double avgFirstPutTxnStateTime = (double) stats.firstPutTxnStateTime
							/ (double) countOfTrxToBeExecuted;
					double avgSecondPutTxnStateTime = (double) stats.secondPutTxnStateTime
							/ (double) countOfTrxToBeExecuted;
					double avgLockingTime = (double) stats.lockingTime
							/ (double) countOfTrxToBeExecuted;
					double avgVersionCheckTime = (double) stats.versionCheckTime
							/ (double) countOfTrxToBeExecuted;
					double avgCommitTime = (double) stats.commitTime
							/ (double) countOfTrxToBeExecuted;
					double avgNbDetoursPerTrx = (double) stats.nbDetoursEncountered
							/ (double) countOfTrxToBeExecuted;
					double avgNbNetworkRoundTripsForLockingPerTrx = (double) stats.nbNetworkRoundTripsInTotalForLocking
							/ (double) countOfTrxToBeExecuted;
					double avgLockMigrationTime = (double) stats.lockMigrationTime
							/ (double) countOfTrxToBeExecuted;
					System.out.println("Time taken to execute all transactions: "
							+ totalTimeTaken);
					System.out.println("Throughput: " + throughput);
					System.out.println("RestartRate: " + restartRate);
					System.out.println("Average num of logs acquired per trx: "
							+ avgNumOflogsAcquiredPerTrx);
					System.out.println("Total number of Detours taken: "
							+ stats.nbDetoursEncountered);
					System.out.println("Average nb of Detours per trx: "
							+ avgNbDetoursPerTrx);
					System.out
							.println("Average nb of Network round trips for locking per trx: "
									+ avgNbNetworkRoundTripsForLockingPerTrx);
					System.out.println("Average Reading time: " + avgReadtime);
					System.out.println("Average Put Shadow time: " + avgPutShadowTime);
					System.out.println("Average Put Txn State time: "
							+ avgPutTxnStateTime);
					System.out.println("Average First Put Txn State time: "
							+ avgFirstPutTxnStateTime);
					System.out.println("Average Second Put Txn State time: "
							+ avgSecondPutTxnStateTime);
					System.out.println("Average Lock Migration time: "
							+ avgLockMigrationTime);
					System.out.println("Average Locking time: " + avgLockingTime);
					System.out.println("Average Version Check time: "
							+ avgVersionCheckTime);
					System.out.println("Average Commit time: " + avgCommitTime);
					break;
				}
			}
		} catch (IOException ioe) {
			System.out.println(ioe.getMessage());
			ioe.printStackTrace();
		} catch (InterruptedException ie) {
			System.out.println(ie.getMessage());
			ie.printStackTrace();
		} catch (ExecutionException ee) {
			System.out.println(ee.getMessage());
			ee.printStackTrace();
		}
		System.out.println(" Done! ");
	}
}
