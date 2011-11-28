package edu.mit.hstore.dtxn;

import org.voltdb.StoredProcedureInvocation;
import org.voltdb.catalog.Procedure;

import edu.mit.hstore.HStoreSite;

/**
 * Special transaction state object for MapReduce jobs
 * @author pavlo
 */
public class MapReduceTransaction extends LocalTransaction {

    /**
     * Whether this is a map_phase
     */
    public boolean map_phase;
    /**
     * Whether this is a reduce_phase
     */
    public boolean reduce_phase;
	
	public MapReduceTransaction(HStoreSite hstore_site) {
		super(hstore_site);
	}

    public MapReduceTransaction init(long txnId, int base_partition, Procedure catalog_proc, StoredProcedureInvocation invocation) {
    	super.init(txnId, -1, base_partition,
    			   hstore_site.getAllPartitionIds(), false, true,
                   null, catalog_proc, invocation, null);
    	
    	this.map_phase = false;
    	this.reduce_phase = false;
    	return (this);
    }
    
    @Override
    public void finish() {
    	super.finish();
    	
    	// TODO(xin): Reset mapPhase/reducePhase
    	
    	this.map_phase = true;
    	this.reduce_phase = false;
    }
    
    public boolean isMapPhase() {
		// TODO Auto-generated method stub
		return map_phase;
	}
    
    public boolean isReducePhase() {
		// TODO Auto-generated method stub
		return reduce_phase;
	}
    
}
