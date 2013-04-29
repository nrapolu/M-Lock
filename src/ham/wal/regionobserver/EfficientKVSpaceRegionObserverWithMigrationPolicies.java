package ham.wal.regionobserver;

import ham.wal.WALTableProperties;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.collect.ImmutableList;

public class EfficientKVSpaceRegionObserverWithMigrationPolicies 	extends BaseRegionObserver {
		static final Log LOG = LogFactory
				.getLog(EfficientKVSpaceRegionObserver.class);

		boolean beforeDelete = true;
		boolean scannerOpened = false;
		boolean hadPreOpen;
		boolean hadPostOpen;
		boolean hadPreClose;
		boolean hadPostClose;
		boolean hadPreFlush;
		boolean hadPostFlush;
		boolean hadPreSplit;
		boolean hadPostSplit;
		boolean hadPreCompactSelect;
		boolean hadPostCompactSelect;
		boolean hadPreCompact;
		boolean hadPostCompact;
		boolean hadPreGet = false;
		boolean hadPostGet = false;
		boolean hadPrePut = false;
		boolean hadPostPut = false;
		boolean hadPreDeleted = false;
		boolean hadPostDeleted = false;
		boolean hadPreGetClosestRowBefore = false;
		boolean hadPostGetClosestRowBefore = false;
		boolean hadPreIncrement = false;
		boolean hadPostIncrement = false;
		boolean hadPreWALRestored = false;
		boolean hadPostWALRestored = false;
		boolean hadPreScannerNext = false;
		boolean hadPostScannerNext = false;
		boolean hadPreScannerClose = false;
		boolean hadPostScannerClose = false;
		boolean hadPreScannerOpen = false;
		boolean hadPostScannerOpen = false;

		CumulativeInMemoryStateWithSingleHashMapAndMigrationPolicies inMemState = null;

		private void sysout(String line) {
			//System.out.println(line);
		}

		@Override
		public void start(CoprocessorEnvironment e) throws IOException {
			// TODO Auto-generated method stub
			super.start(e);
		}

		@Override
		public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
			hadPreOpen = true;
			this.inMemState = new CumulativeInMemoryStateWithSingleHashMapAndMigrationPolicies(c.getEnvironment()
					.getRegion().getRegionInfo());
		}

		@Override
		public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
			hadPostOpen = true;
		}

		public boolean wasOpened() {
			return hadPreOpen && hadPostOpen;
		}

		@Override
		public void preClose(ObserverContext<RegionCoprocessorEnvironment> c,
				boolean abortRequested) {
			hadPreClose = true;
		}

		@Override
		public void postClose(ObserverContext<RegionCoprocessorEnvironment> c,
				boolean abortRequested) {
			hadPostClose = true;
		}

		public boolean wasClosed() {
			return hadPreClose && hadPostClose;
		}

		@Override
		public void preFlush(ObserverContext<RegionCoprocessorEnvironment> c) {
			hadPreFlush = true;
		}

		@Override
		public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c) {
			hadPostFlush = true;
		}

		public boolean wasFlushed() {
			return hadPreFlush && hadPostFlush;
		}

		@Override
		public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c) {
			hadPreSplit = true;
		}

		@Override
		public void postSplit(ObserverContext<RegionCoprocessorEnvironment> c,
				HRegion l, HRegion r) {
			hadPostSplit = true;
		}

		public boolean wasSplit() {
			return hadPreSplit && hadPostSplit;
		}

		@Override
		public void preCompactSelection(
				ObserverContext<RegionCoprocessorEnvironment> c, Store store,
				List<StoreFile> candidates) {
			hadPreCompactSelect = true;
		}

		@Override
		public void postCompactSelection(
				ObserverContext<RegionCoprocessorEnvironment> c, Store store,
				ImmutableList<StoreFile> selected) {
			hadPostCompactSelect = true;
		}

		@Override
		public InternalScanner preCompact(
				ObserverContext<RegionCoprocessorEnvironment> e, Store store,
				InternalScanner scanner) {
			hadPreCompact = true;
			return scanner;
		}

		@Override
		public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,
				Store store, StoreFile resultFile) {
			hadPostCompact = true;
		}

		public boolean wasCompacted() {
			return hadPreCompact && hadPostCompact;
		}

		@Override
		public RegionScanner preScannerOpen(
				final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan,
				final RegionScanner s) throws IOException {
			hadPreScannerOpen = true;
			return null;
		}

		@Override
		public RegionScanner postScannerOpen(
				final ObserverContext<RegionCoprocessorEnvironment> c, final Scan scan,
				final RegionScanner s) throws IOException {
			hadPostScannerOpen = true;
			return s;
		}

		@Override
		public boolean preScannerNext(
				final ObserverContext<RegionCoprocessorEnvironment> c,
				final InternalScanner s, final List<Result> results, final int limit,
				final boolean hasMore) throws IOException {
			hadPreScannerNext = true;
			return hasMore;
		}

		@Override
		public boolean postScannerNext(
				final ObserverContext<RegionCoprocessorEnvironment> c,
				final InternalScanner s, final List<Result> results, final int limit,
				final boolean hasMore) throws IOException {
			hadPostScannerNext = true;
			return hasMore;
		}

		@Override
		public void preScannerClose(
				final ObserverContext<RegionCoprocessorEnvironment> c,
				final InternalScanner s) throws IOException {
			hadPreScannerClose = true;
		}

		@Override
		public void postScannerClose(
				final ObserverContext<RegionCoprocessorEnvironment> c,
				final InternalScanner s) throws IOException {
			hadPostScannerClose = true;
		}

		@Override
		public void preGet(final ObserverContext<RegionCoprocessorEnvironment> c,
				final Get get, final List<KeyValue> results) throws IOException {
			Map<byte[], NavigableSet<byte[]>> familyMap = get.getFamilyMap();
			RegionCoprocessorEnvironment e = c.getEnvironment();
			HRegion region = e.getRegion();
			hadPreGet = true;

			if (familyMap == null)
				return;

			// 
			// 1. If there are Delete markers in the efficientKVSpace for any KeyValue,
			// then we have to send back
			// null.
			// 2. If the efficientKVSpace returns nulls, and there is
			// regionObserverMarkerColumn in the Get
			// requests, then we go down to the HRegion and fetch those KeyValues.

			// Check for the presence of WALTableProperties.RegionObserverMarkerColumn.
			boolean processThroughEfficientKVSpace = false;
			for (NavigableSet<byte[]> columns : familyMap.values()) {
				if (columns != null) {
					for (byte[] col : columns) {
						if (Bytes.equals(col, WALTableProperties.regionObserverMarkerColumn)) {
							processThroughEfficientKVSpace = true;
							// Deleting the marker from Get object.
							columns.remove(col);
							break;
						} else if (Bytes.equals(col, WALTableProperties.writeLockColumn) || 
								Bytes.equals(col, WALTableProperties.isLockPlacedOrMigratedColumn)) {
							processThroughEfficientKVSpace = true;
							// We don't delete the writeLockColumn -- that is not a marker. We
							// are using this
							// hack to send the Get object in the checkAndMutate function
							// through InMemState.
							// TODO: Remove this else block completely once we write our own
							// locking function
							// which does not rely on checkAndMutate function of HRegion.
							break;
						}
					}
				}
				if (processThroughEfficientKVSpace)
					break;
			}

			if (!processThroughEfficientKVSpace) {
				// TODO: Do we need this? What if we include new coprocessors in future?
				// Don't go through any other chained coprocessors.
				c.complete();
				return;
			}

			Scan scan = new Scan(get);

			Get columnsNotServedByInMemoryStore = new Get(get.getRow());
			columnsNotServedByInMemoryStore.setMaxVersions(get.getMaxVersions());
			columnsNotServedByInMemoryStore.setTimeRange(get.getTimeRange().getMin(),
					get.getTimeRange().getMax());

			sysout("Inside preGet, ready for InMemory processing");
			List<KeyValue> kvsFromInMemoryStore = inMemState
					.getAllKVsForASingleGet(scan);
			Result inMemResult = new Result(kvsFromInMemoryStore);
			sysout("Inside preGet, for scan: " + scan.toJSON() + 
					", InMemoryResult: " + inMemResult.toString());

			// Verify that inMemStore returned results for all columns needed by Get.
			// If it didn't do that for any columns, create a new Get object for
			// disk-backed region.
			// Once fetched, add those keyvalues also into the overall Result object.
			Map<byte[], NavigableSet<byte[]>> scanFamMap = scan.getFamilyMap();
			for (Map.Entry<byte[], NavigableSet<byte[]>> scanFamMapEntry : scanFamMap
					.entrySet()) {
				byte[] family = scanFamMapEntry.getKey();
				NavigableMap<byte[], byte[]> resultColMap = inMemResult
						.getFamilyMap(family);
				if (resultColMap == null || resultColMap.isEmpty()) {
					columnsNotServedByInMemoryStore.addFamily(family);
					continue;
				}

				// If the scan had any particular columns mentioned.
				if (scanFamMapEntry.getValue() != null
						&& scanFamMapEntry.getValue().size() > 0) {
					for (byte[] col : scanFamMapEntry.getValue()) {
						List<KeyValue> resultKVSList = inMemResult.getColumn(family, col);
						if (resultKVSList == null || resultKVSList.isEmpty()) {
							columnsNotServedByInMemoryStore.addColumn(family, col);
							continue;
						} else {
							// Here we iterate on the KeyValues sent by the InMemoryStore. If
							// there is a Delete marker
							// in any of those KeyValues, we remove them from the list, so that
							// the client receives
							// a "null" for those columns.
							for (KeyValue kv : resultKVSList) {
								if (kv.isDelete())
									kvsFromInMemoryStore.remove(kv);
							}
						}
					}
				}
			}

			if (columnsNotServedByInMemoryStore.hasFamilies()) {
				List<KeyValue> kvsFromRegion = region.get(
						columnsNotServedByInMemoryStore, null).list();
				if (kvsFromRegion != null && !kvsFromRegion.isEmpty()) {
					results.addAll(kvsFromRegion);
					Result fromRegionResult = new Result(kvsFromRegion);
					sysout("Inside preGet, from region result: "
							+ fromRegionResult.toString());
				}
			}

			if (kvsFromInMemoryStore != null && !kvsFromInMemoryStore.isEmpty()) {
				results.addAll(kvsFromInMemoryStore);
			}

			Collections.sort(results, KeyValue.COMPARATOR);
			
			Result mergedResult = new Result(results);
			sysout("Merged result: " + mergedResult.toString());
			
			// BIGNOTE: The values fetched from the region can be stored in the
			// InMemoryState. In that case,
			// we don't need to go back to any region anymore. We need to implement this
			// in the future.
			c.complete();
			c.bypass();
		}

		@Override
		public void postGet(final ObserverContext<RegionCoprocessorEnvironment> c,
				final Get get, final List<KeyValue> results) {
			hadPostGet = true;
		}

		@Override
		public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
				final Put put, final WALEdit edit, final boolean writeToWAL)
				throws IOException {
			Map<byte[], List<KeyValue>> familyMap = put.getFamilyMap();
			RegionCoprocessorEnvironment e = c.getEnvironment();
			hadPrePut = true;

			if (familyMap == null)
				return;

			// Check for the presence of WALTableProperties.RegionObserverMarkerColumn.
			boolean processThroughEfficientKVSpace = false;
			for (List<KeyValue> kvs : familyMap.values()) {
				if (kvs != null) {
					for (int i = 0; i < kvs.size(); i++) {
						KeyValue kv = kvs.get(i);
						if (Bytes.equals(kv.getQualifier(),
								WALTableProperties.regionObserverMarkerColumn)) {
							processThroughEfficientKVSpace = true;
							kvs.remove(i);
							break;
						}
					}
				}
				if (processThroughEfficientKVSpace)
					break;
			}

			if (!processThroughEfficientKVSpace) {
				// TODO: Do we need this? What if we include new coprocessors in future?
				// Don't go through any other chained coprocessors.
				c.complete();
				return;
			}

			sysout("Inside PrePut, set for InMemory Processing");
			inMemState.addPut(put);
			c.bypass();

			if (writeToWAL) {
				sysout("Writing to WAL!");
				long now = EnvironmentEdgeManager.currentTimeMillis();
				HLog log = c.getEnvironment().getRegion().getLog();
				HTableDescriptor htd = e.getRegion().getTableDesc();

				for (List<KeyValue> kvs : familyMap.values()) {
					for (KeyValue kv : kvs) {
						edit.add(kv);
					}
				}

				log.append(e.getRegion().getRegionInfo(), htd.getName(), edit, now, htd);
			}
			sysout("Inside PrePut, done with InMemory Processing");
		}

		@Override
		public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
				final Put put, final WALEdit edit, final boolean writeToWAL)
				throws IOException {
			hadPostPut = true;
		}

		@Override
		public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
				final Delete delete, final WALEdit edit, final boolean writeToWAL)
				throws IOException {
			Map<byte[], List<KeyValue>> familyMap = delete.getFamilyMap();
			RegionCoprocessorEnvironment e = c.getEnvironment();

			if (familyMap == null)
				return;

			// Check for the presence of WALTableProperties.RegionObserverMarkerColumn.
			// For delete we'll have to delete the KeyValue associated with
			// regionObserverMarkerColumn.
			// For some reason, HRegion isn't playing well with deleting columns that
			// don't exist.
			// NOTE: Place the regionObserverMarkerColumn for only one family because
			// the following loop
			// exits after finding a single instance of the marker.
			boolean processThroughEfficientKVSpace = false;
			for (List<KeyValue> kvs : familyMap.values()) {
				if (kvs != null) {
					for (int i = 0; i < kvs.size(); i++) {
						KeyValue kv = kvs.get(i);
						if (Bytes.equals(kv.getQualifier(),
								WALTableProperties.regionObserverMarkerColumn)) {
							processThroughEfficientKVSpace = true;
							kvs.remove(i);
							break;
						}
					}
				}
				if (processThroughEfficientKVSpace)
					break;
			}

			if (!processThroughEfficientKVSpace) {
				// TODO: Do we need this? What if we include new coprocessors in future?
				// Don't go through any other chained coprocessors.
				c.complete();
				return;
			}

			sysout("In preDelete, processing this Delete: " + delete.toString()
					+ " through InMem store");
			boolean inMemRetVal = inMemState.addDelete(delete);

			// If addDelete function returns false, it implies that some deleteColumn
			// commands could not
			// be processed because there is nothing in the inMem store at those
			// positions. This means the
			// client wants to delete data that is present in the region. Thus, we just
			// pass on this delete
			// object to the region.

			if (inMemRetVal) {
				// If all deletes were taken care by the InMemState, then we can bypass
				// the region and
				// do our own WAL appends.
				// BIGNOTE: Even if a few columns couldn't be traced and deleted by the
				// InMemState, we
				// are sending the entire delete to the region. Not sure how will region
				// behave with
				// deleting columns it does not have in-store.
				sysout("In preDelete, inMemState returned true, hence bypassing the request to region.");
				c.bypass();
				long now = EnvironmentEdgeManager.currentTimeMillis();
				HLog log = e.getRegion().getLog();
				HTableDescriptor htd = e.getRegion().getTableDesc();

				for (List<KeyValue> kvs : familyMap.values()) {
					for (KeyValue kv : kvs) {
						edit.add(kv);
					}
				}

				log.append(c.getEnvironment().getRegion().getRegionInfo(), htd.getName(),
						edit, now, htd);
			}

			if (beforeDelete) {
				hadPreDeleted = true;
			}
		}

		@Override
		public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
				final Delete delete, final WALEdit edit, final boolean writeToWAL)
				throws IOException {
			Map<byte[], List<KeyValue>> familyMap = delete.getFamilyMap();
			RegionCoprocessorEnvironment e = c.getEnvironment();
			beforeDelete = false;
			hadPostDeleted = true;
		}

		@Override
		public void preGetClosestRowBefore(
				final ObserverContext<RegionCoprocessorEnvironment> c, final byte[] row,
				final byte[] family, final Result result) throws IOException {
			RegionCoprocessorEnvironment e = c.getEnvironment();
			if (beforeDelete) {
				hadPreGetClosestRowBefore = true;
			}
		}

		@Override
		public void postGetClosestRowBefore(
				final ObserverContext<RegionCoprocessorEnvironment> c, final byte[] row,
				final byte[] family, final Result result) throws IOException {
			RegionCoprocessorEnvironment e = c.getEnvironment();
			hadPostGetClosestRowBefore = true;
		}

		@Override
		public Result preIncrement(
				final ObserverContext<RegionCoprocessorEnvironment> c,
				final Increment increment) throws IOException {
			hadPreIncrement = true;
			return null;
		}

		@Override
		public Result postIncrement(
				final ObserverContext<RegionCoprocessorEnvironment> c,
				final Increment increment, final Result result) throws IOException {
			hadPostIncrement = true;
			return result;
		}

		public boolean hadPreGet() {
			return hadPreGet;
		}

		public boolean hadPostGet() {
			return hadPostGet;
		}

		public boolean hadPrePut() {
			return hadPrePut;
		}

		public boolean hadPostPut() {
			return hadPostPut;
		}

		public boolean hadDelete() {
			return !beforeDelete;
		}

		public boolean hadPreIncrement() {
			return hadPreIncrement;
		}

		public boolean hadPostIncrement() {
			return hadPostIncrement;
		}

		public boolean hadPreWALRestored() {
			return hadPreWALRestored;
		}

		public boolean hadPostWALRestored() {
			return hadPostWALRestored;
		}

		public boolean wasScannerNextCalled() {
			return hadPreScannerNext && hadPostScannerNext;
		}

		public boolean wasScannerCloseCalled() {
			return hadPreScannerClose && hadPostScannerClose;
		}

		public boolean wasScannerOpenCalled() {
			return hadPreScannerOpen && hadPostScannerOpen;
		}

		public boolean hadDeleted() {
			return hadPreDeleted && hadPostDeleted;
		}

}
