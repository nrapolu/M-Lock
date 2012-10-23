package ham.wal;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class TestWALManager {
	static Hashtable<String, String> logRoutingTable = new Hashtable<String, String>();

	WALTableProperties tableProperties = null;
	Configuration conf = null;
	byte[] dataTableName = WALTableProperties.dataTableName;
	byte[] walTableName = WALTableProperties.walTableName;
	byte[] dataFamily = WALTableProperties.dataFamily;
	byte[] dataColumn = WALTableProperties.dataColumn;

	public TestWALManager() throws IOException {
		this.conf = (Configuration) HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		tableProperties = new WALTableProperties(conf, admin);
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

	public LocalTrxExecutorReturnVal executeLocalTrxFromFile(String fileName,
			int numParallelClients, int thinkingTime, long countOfTrxToBeExecuted,
			int lenOfTrx) throws ExecutionException, IOException,
			InterruptedException {
		long abortCount = 0;
		long numOflogsAcquired = 0;
		long countOfExecutedTrx = 0;
		long logingTime = 0;
		ExecutorService threadExecutor = Executors
				.newFixedThreadPool(numParallelClients);
		BufferedReader bin = new BufferedReader(new FileReader(fileName));
		String trxLine = null;
		countOfTrxToBeExecuted = (countOfTrxToBeExecuted < 0) ? Long.MAX_VALUE
				: countOfTrxToBeExecuted;
		// globalTrxCount denotes the number of Trx read and executed from file.
		long globalTrxCount = 0;
		LinkedList<Future<LocalTrxExecutorReturnVal>> futureList = 
			new LinkedList<Future<LocalTrxExecutorReturnVal>>();
		while (globalTrxCount < countOfTrxToBeExecuted) {
			futureList.clear();
			// localTrxCount denotes the number of Trx in each batch; a new batch
			// starts only when the previous batch completes.
			long localTrxCount = 200 * numParallelClients;
			if (localTrxCount + globalTrxCount > countOfTrxToBeExecuted)
				localTrxCount = countOfTrxToBeExecuted - globalTrxCount;

			globalTrxCount += localTrxCount;
			while ((trxLine = bin.readLine()) != null && localTrxCount > 0) {
				String[] tokens = trxLine.trim().split("\\s+");
				Future<LocalTrxExecutorReturnVal> f = threadExecutor.submit(new LocalTrxExecutor(
						tokens, dataTableName, conf, walTableName, conf, thinkingTime,
						lenOfTrx));
				futureList.addLast(f);
				localTrxCount--;
			}

			while (futureList.isEmpty() == false) {
				Iterator<Future<LocalTrxExecutorReturnVal>> futureItr = futureList
						.iterator();
				while (futureItr.hasNext()) {
					Future<LocalTrxExecutorReturnVal> future = futureItr.next();
					if (future.isDone() && future.get().tokens == null) {
						abortCount += future.get().abortCount;
						numOflogsAcquired += future.get().numOflogsAcquired;
						countOfExecutedTrx++;
						logingTime += future.get().logingTime;
						if (countOfExecutedTrx % 10 == 0) {
							System.out
									.println("Count Of Executed Trx: " + countOfExecutedTrx);
							System.out.println("logging time: " + logingTime);
						}
						futureItr.remove();
					} else if (future.isDone() && future.get().tokens != null) {
						abortCount += future.get().abortCount;
						String[] tokens = future.get().tokens;
						Future<LocalTrxExecutorReturnVal> f = threadExecutor
								.submit(new LocalTrxExecutor(tokens, dataTableName, conf,
										walTableName, conf, thinkingTime, lenOfTrx));
						futureItr.remove();
						futureList.addLast(f);
						break;
					} else if (future.isDone() == false) {
						continue;
					}
				}
			}
		}

		threadExecutor.shutdown();
		// threadExecutor.awaitTermination(100000L, TimeUnit.MINUTES);

		bin.close();
		return new LocalTrxExecutorReturnVal(abortCount, numOflogsAcquired);
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
				.println("java TestWALManager <numDataEntriesInItemsTable> <Option[1,2,3]. "
						+ "1 - Write to table, "
						+ "2 - Populate default routing entries, "
						+ "3 - Populate routing entries from file, "
						+ "4 - Populate local routing table from central table, "
						+ "5 - Execute transactions from file. "
						+ " <AvgLenOfTrx> <TrxFile> <CountOfTrxToBeExecuted> [NumOfDataSplits] [NumOfParallelClients] [RoutingFile]  [ThinkingTimeForEachClient]");
		System.exit(0);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 5)
			printUsage();

		try {
			long numDataEntries = Long.parseLong(args[0]);
			int lenOfTrx = Integer.parseInt(args[2]);
			TestWALManager hbaseTrxCli = new TestWALManager();
			WALTableProperties walTableProps = hbaseTrxCli.tableProperties;
			String optionStr = args[1];
			long countOfTrxToBeExecuted = Long.parseLong(args[4]);
			int numDataSplits = 1;
			int numParallelClients = 1;
			if (args.length >= 6)
				numDataSplits = Integer.parseInt(args[5]);
			if (args.length == 7)
				numParallelClients = Integer.parseInt(args[6]);

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
					LocalTrxExecutorReturnVal stats = hbaseTrxCli.executeLocalTrxFromFile(
							trxFileName, numParallelClients, thinkingTime,
							countOfTrxToBeExecuted, lenOfTrx);
					long endTime = System.currentTimeMillis();

					long abortCount = stats.abortCount;
					double avgNumOflogsAcquiredPerTrx = (double) stats.numOflogsAcquired
							/ (double) countOfTrxToBeExecuted;
					double totalTimeTaken = (double) (endTime - startTime) / 1000;
					System.out.println("Time taken to execute all transactions: "
							+ totalTimeTaken);
					double throughput = (double) countOfTrxToBeExecuted
							/ (double) totalTimeTaken;
					System.out.println("Throughput: " + throughput);
					double restartRate = (double) abortCount / (double) totalTimeTaken;
					System.out.println("RestartRate: " + restartRate);
					System.out.println("Average num of logs acquired per trx: "
							+ avgNumOflogsAcquiredPerTrx);
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
