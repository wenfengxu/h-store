package edu.mit.hstore.dtxn;

import java.util.Collection;

import org.voltdb.StoredProcedureInvocation;
import org.voltdb.catalog.Procedure;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.StringUtil;
import edu.mit.hstore.HStoreObjectPools;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.callbacks.TransactionMapCallback;
import edu.mit.hstore.callbacks.TransactionMapWrapperCallback;

/**
 * Special transaction state object for MapReduce jobs
 * 
 * @author pavlo
 */
public class MapReduceTransaction extends LocalTransaction {

    private final LocalTransaction local_txns[];
    // private Procedure catalog_proc;
    // private StoredProcedureInvocation invocation;

    /**
     * MapReduce Phases
     */
    private boolean map_phase = false;
    private boolean reduce_phase = false;

    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------

    /**
     */
    private final TransactionMapCallback map_callback;

    private final TransactionMapWrapperCallback mapWrapper_callback;

    private void initLocalTransactions(){
        for (int partition : this.hstore_site.getLocalPartitionIds()) {
            int offset = hstore_site.getLocalPartitionFromOffset(partition);
            this.local_txns[offset].init(this.txn_id, this.client_handle, partition, hstore_site.getAllPartitionIds(),
                                         this.predict_readOnly, this.predict_abortable,
                                         null, catalog_proc, invocation, null);
        } // FOR
    }
    
    public MapReduceTransaction(HStoreSite hstore_site) {
        super(hstore_site);
        this.local_txns = new LocalTransaction[hstore_site.getLocalPartitionIds().size()];
        for (int i = 0; i < this.local_txns.length; i++) {
            this.local_txns[i] = new LocalTransaction(hstore_site);
        } // FOR

        this.map_callback = new TransactionMapCallback(hstore_site);
        this.mapWrapper_callback = new TransactionMapWrapperCallback(hstore_site);
    }
    
    @SuppressWarnings("unchecked")
    public MapReduceTransaction init(long txnId, long clientHandle, int base_partition, boolean predict_readOnly, boolean predict_canAbort) {
        
        super.init(txnId, clientHandle, base_partition,predict_readOnly, predict_canAbort);
        this.initLocalTransactions();
        this.setMapPhase();
        this.map_callback.init(this);
        return (this);
    }
    

    public MapReduceTransaction init(long txnId, long clientHandle, int base_partition, Collection<Integer> predict_touchedPartitions, boolean predict_readOnly, boolean predict_canAbort,
            TransactionEstimator.State estimator_state, Procedure catalog_proc, StoredProcedureInvocation invocation, RpcCallback<byte[]> client_callback) {

        super.init(txnId, clientHandle, base_partition, predict_touchedPartitions, predict_readOnly, predict_canAbort, estimator_state, catalog_proc, invocation, client_callback);

        this.initLocalTransactions();
        this.setMapPhase();
        
        this.map_callback.init(this);
        //this.mapWrapper_callback.init(this, orig_callback);
        return (this);
    }

    public MapReduceTransaction init(long txnId, int base_partition, Procedure catalog_proc, StoredProcedureInvocation invocation) {
        assert (invocation != null) : "invalid StoredProcedureInvocation parameter for MapReduceTransaction.init()";
        assert (catalog_proc != null) : "invalid Procedure parameter for MapReduceTransaction.init()";

        super.init(txnId, invocation.getClientHandle(), base_partition, false, false, true, true);
        
        this.catalog_proc = catalog_proc;
        this.invocation = invocation;
        
        this.initLocalTransactions();
        this.setMapPhase();
        
        this.map_callback.init(this);
        //this.mapWrapper_callback.init(this, orig_callback);

        return (this);
    }
    

    @Override
    public void finish() {
        super.finish();
        for (int i = 0; i < this.local_txns.length; i++) {
            this.local_txns[i].finish();
        } // FOR
        this.map_phase = false;
        this.reduce_phase = false;
        this.map_callback.finish();
        this.mapWrapper_callback.finish();
    }

    /**
     * Get a LocalTransaction handle for a local partition
     * 
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

    public boolean isMapPhase() {
        return (this.map_phase);
    }

    public void setMapPhase() {
        assert (this.reduce_phase == false);
        this.map_phase = true;
    }

    public boolean isReducePhase() {
        return (this.reduce_phase);
    }

    public void setReducePhase() {
        assert (this.map_phase == true);
        this.map_phase = false;
        this.reduce_phase = true;
    }

    public StoredProcedureInvocation getInvocation() {
        return this.invocation;
    }

    public String getProcedureName() {
        return (this.catalog_proc != null ? this.catalog_proc.getName() : null);
    }

    public Collection<Integer> getPredictTouchedPartitions() {
        return (this.hstore_site.getAllPartitionIds());
    }

    public TransactionMapCallback getTransactionMapCallback() {
        return (this.map_callback);
    }

    public void initTransactionMapWrapperCallback(RpcCallback<Hstore.TransactionMapResponse> orig_callback) {
        assert (this.mapWrapper_callback.isInitialized() == false);
        this.mapWrapper_callback.init(this, orig_callback);
    }
    public TransactionMapWrapperCallback getTransactionMapWrapperCallback() {
        assert(this.mapWrapper_callback.isInitialized());
        return (this.mapWrapper_callback);
    }

    @Override
    public String toString() {
        if (this.isInitialized()) {
            boolean is_map = this.isMapPhase();
            return String.format("%s-%s #%d/%d", this.getProcedureName(), (is_map ? "MAP" : "REDUCE"), this.txn_id, this.base_partition);
        } else {
            return ("<Uninitialized>");
        }
    }

    @Override
    public String debug() {
        return (StringUtil.formatMaps(this.getDebugMap()));
    }

}
