package org.voltdb;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.PartitionEstimator;

public abstract class VoltMapReduceProcedure extends VoltProcedure {
	
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
		Table catalog_tbl = catalog_db.getTables().get(catalogProc.getMapemittable());
		this.mapOutput = CatalogUtil.getVoltTable(catalog_tbl);
		catalog_tbl = catalog_db.getTables().get(catalogProc.getReduceemittable());
		this.reduceOutput = CatalogUtil.getVoltTable(catalog_tbl);

		// Get the SQLStmt handles for the input queries
		this.mapInputQuery = this.getSQLStmt(catalogProc.getMapinputquery());
		assert(this.mapInputQuery != null) : "Missing " + catalogProc.getMapinputquery();
		this.reduceInputQuery = this.getSQLStmt(catalogProc.getReduceinputquery());
		assert(this.reduceInputQuery != null) : "Missing " + catalogProc.getReduceinputquery();
	}

	public abstract void map(VoltTableRow tuple);
	public abstract void reduce(VoltTable[] r);
	
	//TODO(xin): Execute MapInputQuery and then loop through the
    //	  result and invoke the implementing class's map()
    public final void runMap(Object params[]) {
    	voltQueueSQL(mapInputQuery, params);
    	VoltTable mapResult[] = voltExecuteSQL();
    	assert(mapResult.length == 1);
    	
    	while (mapResult[0].advanceRow()) {
    		this.map(mapResult[0].getRow());
    	} // WHILE
    }
  //TODO(xin): Execute ReduceInputQuery and then loop through the
    //	  result and invoke the implementing class's reduce()
    public final void runReduce(Object params[]) {
    	voltQueueSQL(reduceInputQuery, params);
    	VoltTable reduceResult[] = voltExecuteSQL();
    	assert(reduceResult.length == 1);
    	
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
		Object params[] = null; // This really should be passed in 
		VoltTable result = null;
		
		// If this invocation is at the txn's base partition, then it is responsible
		// for sending out the coordination messages to the other partitions 
		boolean is_local = (this.partitionId == m_localTxnState.getBasePartition());
		
//		if (m_localTxnState.isMapPhase()) {
//			// If this is the base partition, then we'll send the out the MAP initialization
//			// requests to all of the partitions
//			if (is_local) {
//				// We have to give a callback, but I'm not sure what it wil be used for
//				// It will allow us to keep track of when all the MAPPERs are done.
//				this.executor.hstore_coordinator.transactionMap(m_localTxnState, callback);
//			}
//			
//			// XXX: Execute the map
//			this.runMap(params);
//			result = this.mapOutput;
//		}
//		else if (m_localTxnState.isReducePhase()) {
//			// If this is the base partition, then we'll send the out the REDUCE initialization
//			// requests to all of the partitions
//			if (is_local) {
//				// We need a callback
//				this.executor.hstore_coordinator.transactionReduce(m_localTxnState, callback);
//			}
//			
//			// XXX: Execute the reduce
//			this.runReduce(params);
//			result = this.reduceOutput;
//		}
//		else {
//			throw new RuntimeException("Invalid state for MapReduce job " + ts);
//		}
		
		return (result);
		
	}
	
}




/*	voltQueueSQL(this.mapInputQuery);
    	VoltTable results[] = voltExecuteSQL();
    	while (results[0].advanceRow()) {
    		Object new_row[] = { results[0].getString(1), 1 };
    		this.reduceEmit(new_row);
    	} // WHILE
 * 
 * voltQueueSQL(this.reduceInputQuery);
    	VoltTable results[] = voltExecuteSQL();
    	while (results[0].advanceRow()) {
    		Object new_row[] = { results[0].getString(1), 1 };
    		this.reduceEmit(new_row);
    	} // WHILE
 * */
 