package edu.mit.hstore.dtxn;

import java.util.Collection;

import org.voltdb.StoredProcedureInvocation;
import org.voltdb.catalog.Procedure;

import edu.brown.utils.StringUtil;
import edu.mit.hstore.HStoreSite;

/**
 * Special transaction state object for MapReduce jobs
 * @author pavlo
 */
public class MapReduceTransaction extends LocalTransaction {

    
    private final LocalTransaction local_txns[];
    private Procedure catalog_proc;
    private StoredProcedureInvocation invocation;
    
	
	public MapReduceTransaction(HStoreSite hstore_site) {
		super(hstore_site);
		this.local_txns = new LocalTransaction[hstore_site.getLocalPartitionIds().size()];
		for (int i = 0; i < this.local_txns.length; i++) {
			this.local_txns[i] = new LocalTransaction(hstore_site);
		} // FOR
	}

    public MapReduceTransaction init(long txnId, int base_partition, Procedure catalog_proc, StoredProcedureInvocation invocation) {
    	super.init(txnId, invocation.getClientHandle(), base_partition,
                   false, false, true, true);
    	for (int i = 0; i < this.local_txns.length; i++) {
    		this.local_txns[i].init(this.txn_id,
    								this.client_handle,
    								this.base_partition,
    								hstore_site.getAllPartitionIds(),
    								this.predict_readOnly,
    								this.predict_abortable,
    								null,
    								catalog_proc,
    								invocation,
    								null);
    	} // FOR
    	
    	this.catalog_proc = catalog_proc;
    	this.invocation = invocation;
    	return (this);
    }
    
    @Override
    public void finish() {
    	super.finish();
    	for (int i = 0; i < this.local_txns.length; i++) {
    		this.local_txns[i].finish();
    	} // FOR
    }
    
    /**
     * Get a LocalTransaction handle for a local partition
     * @param partition
     * @return
     */
    public LocalTransaction getLocalTransaction(int partition) {
    	int offset = hstore_site.getLocalPartitionOffset(partition);
    	return (this.local_txns[offset]);
    }
    
    // ----------------------------------------------------------------------------
    // ACCESS METHODS
    // ----------------------------------------------------------------------------

    public StoredProcedureInvocation getInvocation() {
        return invocation;
    }
    public String getProcedureName() {
        return (this.catalog_proc != null ? this.catalog_proc.getName() : null);
    }
    public Collection<Integer> getPredictTouchedPartitions() {
        return (this.hstore_site.getAllPartitionIds());
    }
    
    @Override
    public String toString() {
        if (this.isInitialized()) {
        	boolean is_map = this.local_txns[0].isMapPhase();
            return String.format("%s-%s #%d/%d",
            					 this.getProcedureName(),
            					 (is_map ? "MAP" : "REDUCE"),
            					 this.txn_id,
            					 this.base_partition);
        } else {
            return ("<Uninitialized>");
        }
    }

    @Override
    public String debug() {
        return (StringUtil.formatMaps(this.getDebugMap()));
    }

}
