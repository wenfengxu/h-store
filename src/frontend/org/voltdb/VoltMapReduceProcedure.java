package org.voltdb;

import org.apache.log4j.Logger;
import org.apache.log4j.helpers.LogLog;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.TransactionMapResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.mit.hstore.HStoreCoordinator;
import edu.mit.hstore.callbacks.TransactionMapCallback;
import edu.mit.hstore.callbacks.TransactionMapWrapperCallback;
import edu.mit.hstore.dtxn.LocalTransaction;
import edu.mit.hstore.dtxn.MapReduceTransaction;

public abstract class VoltMapReduceProcedure extends VoltProcedure {
	public static final Logger LOG = Logger
			.getLogger(VoltMapReduceProcedure.class);
	private final static LoggerBoolean debug = new LoggerBoolean(LOG
			.isDebugEnabled());
	private final static LoggerBoolean trace = new LoggerBoolean(LOG
			.isTraceEnabled());
	static {
		LoggerUtil.attachObserver(LOG, debug, trace);
	}

	private SQLStmt mapInputQuery;
	private VoltTable mapOutput;

	private SQLStmt reduceInputQuery;
	private VoltTable reduceOutput;

	@Override
	public void globalInit(ExecutionSite site, Procedure catalogProc,
			BackendTarget eeType, HsqlBackend hsql,
			PartitionEstimator pEstimator) {
		super.globalInit(site, catalogProc, eeType, hsql, pEstimator);

		// Get the Table catalog object for the map/reduce outputs
		Database catalog_db = CatalogUtil.getDatabase(catalogProc);
		Table catalog_tbl = catalog_db.getTables().get(
				catalogProc.getMapemittable());
		this.mapOutput = CatalogUtil.getVoltTable(catalog_tbl);
		catalog_tbl = catalog_db.getTables().get(
				catalogProc.getReduceemittable());
		this.reduceOutput = CatalogUtil.getVoltTable(catalog_tbl);

		// Get the SQLStmt handles for the input queries
		this.mapInputQuery = this.getSQLStmt(catalogProc.getMapinputquery());
		assert (this.mapInputQuery != null) : "Missing "
				+ catalogProc.getMapinputquery();
		this.reduceInputQuery = this.getSQLStmt(catalogProc
				.getReduceinputquery());
		assert (this.reduceInputQuery != null) : "Missing "
				+ catalogProc.getReduceinputquery();
	}

	public abstract void map(VoltTableRow tuple);

	public abstract void reduce(VoltTable[] r);

	public final void runMap() {
		voltQueueSQL(mapInputQuery);
		VoltTable mapResult[] = voltExecuteSQL();
		assert (mapResult.length == 1);

		while (mapResult[0].advanceRow()) {
			this.map(mapResult[0].getRow());
		} // WHILE
	}

	public final void runReduce(Object params[]) {
		voltQueueSQL(reduceInputQuery, params);
		VoltTable reduceResult[] = voltExecuteSQL();
		assert (reduceResult.length == 1);

		this.reduce(reduceResult);
	}

	public final void mapEmit(Object row[]) {
		this.mapOutput.addRow(row);
	}

	public final void reduceEmit(Object row[]) {
		this.reduceOutput.addRow(row);
	}

	/**
	 * 
	 * @return
	 */
	public final VoltTable run() {
		// Object params[] = null; // This really should be passed in
		VoltTable result = null;

		// If this invocation is at the txn's base partition, then it is
		// responsible for sending out the coordination messages to the other partitions
		boolean is_local = (this.partitionId == m_localTxnState.getBasePartition());
		
		assert(this.hstore_site != null): "error in VoltMapReduceProcedure...for hstore_site..........";
		long ts_id = this.getTransactionId();
		assert(this.hstore_site.getTransaction(ts_id) != null): "error in VoltMapReduceProcedure...for this.hstore_site.getTransaction(ts_id)..........";
		
		
		MapReduceTransaction mr_ts = this.hstore_site.getTransaction(ts_id);
		
		if (mr_ts.isMapPhase()) {
//			 If this is the base partition, then we'll send the out the MAP
//			 initialization requests to all of the partitions
			if (is_local) {
				// Send out network messages to all other partitions to tell them to 
			    // execute the MAP phase of this job
				this.executor.hstore_coordinator.transactionMap(mr_ts, mr_ts.getTransactionMapCallback());
			}

			if (debug.get())
				LOG.debug("<VoltMapReduceProcedure.run> is executing ....\n");
			// XXX: Execute the map
			this.runMap();
			result = this.mapOutput;
			
			// Always invoke the TransactionMapWrapperCallback to let somebody know that
			// we finished the MAP phase at this partition
			TransactionMapWrapperCallback callback = mr_ts.getTransactionMapWrapperCallback();
			assert(callback != null) : "Unexpected null callback for " + mr_ts;
			callback.run(this.partitionId);
		}
		
		// else if (m_localTxnState.isReducePhase()) {
		// // If this is the base partition, then we'll send the out the REDUCE
		// initialization
		// // requests to all of the partitions
		// if (is_local) {
		// // We need a callback
		// this.executor.hstore_coordinator.transactionReduce(m_localTxnState,
		// callback);
		// }
		//                        
		// // XXX: Execute the reduce
		// this.runReduce(params);
		// result = this.reduceOutput;
		// }
		// else {
		// throw new RuntimeException("Invalid state for MapReduce job " + ts);
		// }

		return (result);

	}

}

/*
 * voltQueueSQL(this.mapInputQuery); VoltTable results[] = voltExecuteSQL();
 * while (results[0].advanceRow()) { Object new_row[] = {
 * results[0].getString(1), 1 }; this.reduceEmit(new_row); } // WHILE
 * 
 * voltQueueSQL(this.reduceInputQuery); VoltTable results[] = voltExecuteSQL();
 * while (results[0].advanceRow()) { Object new_row[] = {
 * results[0].getString(1), 1 }; this.reduceEmit(new_row); } // WHILE
 */
