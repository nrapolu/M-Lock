/*
 * Copyright 2009 The Apache Software Foundation Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */

package ham.wal.regionobserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;

/**
 * A fixture that implements and presents a KeyValueScanner. It takes a list of key/values which is then sorted
 * according to the provided comparator, and then the whole thing pretends to be a store file scanner.
 * <p>
 * Note: This class was copied from {@link org.apache.hadoop.hbase.regionserver.KeyValueScanFixture} and modified.
 */
public class KeyValueListScanner implements KeyValueScanner {
		private static final Log LOG = LogFactory.getLog(KeyValueListScanner.class);

    ArrayList<KeyValue> data;
    Iterator<KeyValue> iter = null;
    KeyValue current = null;
    KeyValue.KVComparator comparator;
    long sequenceId = 0;

    public KeyValueListScanner(final KeyValue.KVComparator comparator, final KeyValue ... incData) {
        this.comparator = comparator;

        data = new ArrayList<KeyValue>(incData.length);
        for (int i = 0; i < incData.length; ++i) {
            data.add(incData[i]);
        }
        Collections.sort(data, this.comparator);
        this.iter = data.iterator();
        if (!data.isEmpty()) {
            this.current = KeyValue.createFirstOnRow(data.get(0).getRow());
        }
    }

    public static List<KeyValueScanner> scanFixture(final KeyValue[] ... kvArrays) {
        ArrayList<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
        for (KeyValue[] kvs : kvArrays) {
            scanners.add(new KeyValueListScanner(KeyValue.COMPARATOR, kvs));
        }
        return scanners;
    }

    @Override
    public KeyValue peek() {
        return this.current;
    }

    @Override
    public KeyValue next() {
        KeyValue res = current;

        if (iter.hasNext()) {
            current = iter.next();
        } else {
            current = null;
        }
        return res;
    }

    @Override
    public boolean seek(final KeyValue key) {
        // start at beginning.
        iter = data.iterator();
        int cmp;
        KeyValue kv = null;
        do {
            if (!iter.hasNext()) {
                current = null;
                return false;
            }
            kv = iter.next();
            cmp = comparator.compare(key, kv);
        } while (cmp > 0);
        current = kv;
        return true;
    }

    @Override
    public boolean reseek(final KeyValue key) {
        return seek(key);
    }

    @Override
    public void close() {
        // noop.
    }

    /**
     * Set the sequenceId of this scanner which determines the priority in which it is used against other scanners.
     * Default is 0 (low priority).
     * 
     * @param sequenceId
     */
    public void setSequenceID(final long sequenceId) {
        this.sequenceId = sequenceId;
    }

    @Override
    public long getSequenceID() {
    	LOG.debug("The sequence id for KeyValueListScanner, which is: " + sequenceId 
    			+ " has been asked for.");
    	return sequenceId;
    }

		@Override
		public void enforceSeek() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean isFileScanner() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean realSeekDone() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean requestSeek(KeyValue arg0, boolean arg1, boolean arg2)
				throws IOException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean shouldUseScanner(Scan arg0, SortedSet<byte[]> arg1, long arg2) {
			// TODO Auto-generated method stub
			return false;
		}
}
