package edu.mit.hstore.dtxn;

import org.voltdb.StoredProcedureInvocation;
import org.voltdb.catalog.Procedure;

import edu.mit.hstore.HStoreSite;

/**
 * Special transaction state object for MapReduce jobs
 * @author pavlo
 */
public class MapReduceTransaction extends LocalTransaction {

	public MapReduceTransaction(HStoreSite hstore_site) {
		super(hstore_site);
	}

    public MapReduceTransaction init(long txnId, int base_partition, Procedure catalog_proc, StoredProcedureInvocation invocation) {
    	super.init(txnId, -1, base_partition,
    			   hstore_site.getAllPartitionIds(), false, true,
                   null, catalog_proc, invocation, null);
    	return (this);
    }
    
    @Override
    public void finish() {
    	super.finish();
    	
    	// TODO(xin): Reset mapPhase/reducePhase
    	// (xin): I have done them in LocalTransaction
    }

    
}
