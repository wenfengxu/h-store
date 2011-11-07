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
			PartitionEstimator pEstimator, Integer localPartition) {
		super.globalInit(site, catalogProc, eeType, hsql, pEstimator, localPartition);
		
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
	
	/**
	 * TODO
	 * @param row
	 */
	public abstract void map(VoltTableRow row);
	
	/**
	 * TODO
	 * @param row
	 */
	public abstract void reduce(VoltTableRow row);
	
	protected final void runMap(Object params[]) {
		// TODO(xin): Execute MapInputQuery and then loop through the
		//   		  result and invoke the implementing class's map()
		// voltQueueSQL(this.mapInputQuery, params);
	}
	
	protected final void runReduce() {
		// TODO(xin): Execute ReduceInputQuery and then loop through the
		//   		  result and invoke the implementing class's reduce()
		// voltQueueSQL(this.reduceInputQuery);
	}
	
	public final void mapEmit(Object row[]) {
		this.mapOutput.addRow(row);
	}
	
	public final void reduceEmit(Object row[]) {
		this.reduceOutput.addRow(row);
	}
	
}
