/**
 * Copyright 2009 The Apache Software Foundation Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */
package ham.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseBackedTransactionLogger {

	/** Transaction status values */
	enum TransactionStatus {
		/** Transaction is pending */
		PENDING,
		/** Transaction was committed */
		COMMITTED,
		/** Transaction was aborted */
		ABORTED
	}

	/** The name of the transaction status table. */
	public static final String TABLE_NAME = "__GLOBAL_TRX_LOG__";

	public static final byte[] INFO_FAMILY = Bytes.toBytes("Info");
	public static final byte[] TXN_STATE_QUALIFIER = Bytes.toBytes("TxnState");
	/**
	 * Column which holds the transaction status.
	 */
	public static final byte[] STATUS_QUALIFIER = Bytes.toBytes("Status");

	private long startTrxId;
	private long count = 0;
	private final long MAX_CONSECUTIVE_IDS = 1000000;
	private boolean acquireIdFromTable = true;

	/**
	 * Create the global transaction table.
	 * 
	 * @throws IOException
	 */
	public static void createTable() throws IOException {
		createTable(HBaseConfiguration.create());
	}

	/**
	 * Create the global transaction table with the given configuration.
	 * 
	 * @param conf
	 * @throws IOException
	 */
	public static void createTable(final Configuration conf) throws IOException {
		HTableDescriptor tableDesc = new HTableDescriptor(TABLE_NAME);
		tableDesc.addFamily(new HColumnDescriptor(INFO_FAMILY));
		HBaseAdmin admin = new HBaseAdmin(conf);
		// Pre-splitting the table into 10 regions to avoid region-hotspotting.
		List<byte[]> splitKeys = new ArrayList<byte[]>();
		for (int i = 0; i <= 0; i++)
			splitKeys.add(Bytes.toBytes("" + i));
		if (admin.tableExists(TABLE_NAME)) {
			admin.disableTable(TABLE_NAME);
			admin.deleteTable(TABLE_NAME);
			admin.createTable(tableDesc, splitKeys.toArray(new byte[0][0]));
		} else {
			admin.createTable(tableDesc, splitKeys.toArray(new byte[0][0]));
		}
	}

	private Random random = new Random();
	// #NAR
	private Configuration conf = null;
	private HTablePool tablePool = null;

	private HTableInterface getTable() {
		return tablePool.getTable(TABLE_NAME);
	}

	private void putTable(final HTableInterface t) throws IOException {
		if (t == null) {
			return;
		}
		tablePool.putTable(t);
	}

	public HBaseBackedTransactionLogger() throws IOException {
		initTable(HBaseConfiguration.create());
	}

	public HBaseBackedTransactionLogger(final Configuration conf)
			throws IOException {
		initTable(conf);
	}

	private void initTable(final Configuration conf) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);

		if (!admin.tableExists(TABLE_NAME)) {
			throw new RuntimeException("Table not created. Call createTable() first");
		}
		// #NAR: We use this conf object in the constructor of HTablePool.
		this.conf = conf;
		this.tablePool = new HTablePool(conf, 20);
	}

	public long createNewTransactionLog() throws IOException {
		long id;
		TransactionStatus existing = null;
		if (!acquireIdFromTable && count < MAX_CONSECUTIVE_IDS) {
			count++;
			return Long.parseLong(Long.toString(startTrxId) + Long.toString(count));
		}

		// TODO: We need to eliminate these extra RPCs, by some mechanism, where the
		// table issues a random number when it detects a conflict.
		do {
			String idStr = Long.toString(Math.abs(random.nextLong()));
			id = Long.parseLong(idStr.substring(0, (6 > idStr.length()) ? idStr
					.length() : 6));

			// Essentially, the startTrxId is a unique 6 digit number chosen by
			// individual
			// clients. Once it is chosen, transaction ids for that client are all
			// numbers formed by appending another "count" (0 to infi) to this
			// startId.
			String truncatedId = String.format("%-6d", id).replace(' ', '0');
			try {
				existing = getStatusForTransaction(Long.parseLong(truncatedId));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		} while (existing != null);
		startTrxId = id;
		acquireIdFromTable = false;

		// TODO:VERIFY: Do we need to set pending status for every trx? Can't the
		// log
		// just assume that it is in pending, when it can't find an entry in the
		// table?
		// setStatusForTransaction(id, TransactionStatus.PENDING);

		return id;
	}

	public TransactionStatus getStatusForTransaction(final long transactionId)
			throws IOException {
		HTableInterface table = getTable();
		try {
			Result result = table.get(new Get(getRow(transactionId)));
			if (result == null || result.isEmpty()) {
				return null;
			}
			byte[] statusCell = result.getValue(INFO_FAMILY, STATUS_QUALIFIER);
			if (statusCell == null) {
				throw new RuntimeException("No status cell for row " + transactionId);
			}
			String statusString = Bytes.toString(statusCell);
			return TransactionStatus.valueOf(statusString);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			putTable(table);
		}
	}

	private byte[] getRow(final long transactionId) {
		return Bytes.toBytes("" + transactionId);
	}

	public void setStatusForTransaction(final long transactionId,
			final TransactionStatus status) throws IOException {
		Put update = new Put(getRow(transactionId));
		update.add(INFO_FAMILY, STATUS_QUALIFIER, HConstants.LATEST_TIMESTAMP,
				Bytes.toBytes(status.name()));

		HTableInterface table = getTable();
		try {
			table.put(update);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			putTable(table);
		}
	}

	public void persistTxnState(final long transactionId, final byte[] txnState)
			throws IOException {
		Put update = new Put(getRow(transactionId));
		update.add(INFO_FAMILY, TXN_STATE_QUALIFIER, HConstants.LATEST_TIMESTAMP,
				txnState);

		HTableInterface table = getTable();
		try {
			table.put(update);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			putTable(table);
		}
	}

	public void forgetTransaction(final long transactionId) throws IOException {
		Delete delete = new Delete(getRow(transactionId));

		HTableInterface table = getTable();
		try {
			table.delete(delete);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			putTable(table);
		}
	}
}
