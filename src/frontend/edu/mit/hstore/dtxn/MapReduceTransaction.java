package edu.mit.hstore.dtxn;

import java.util.Collection;
import java.util.Collections;

import org.apache.log4j.Logger;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.catalog.Procedure;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
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
    private static final Logger LOG = Logger.getLogger(MapReduceTransaction.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final LocalTransaction local_txns[];
    // private Procedure catalog_proc;
    // private StoredProcedureInvocation invocation;

    public enum State {
        MAP,
        SHUFFLE,
        REDUCE;
    }
    
    /**
     * MapReduce Phases
     */
    private State state = null;

    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------

    /**
     */
    private final TransactionMapCallback map_callback;

    private final TransactionMapWrapperCallback mapWrapper_callback;
    
    
    /**
     * Constructor 
     * @param hstore_site
     */
    public MapReduceTransaction(HStoreSite hstore_site) {
        super(hstore_site);
        this.local_txns = new LocalTransaction[hstore_site.getLocalPartitionIds().size()];
        for (int i = 0; i < this.local_txns.length; i++) {
            this.local_txns[i] = new LocalTransaction(hstore_site) {
                @Override
                public String toString() {
                    if (this.isInitialized()) {
                        return MapReduceTransaction.this.toString() + "/" + this.base_partition;
                    } else {
                        return ("<Uninitialized>");
                    }
                }
            };
        } // FOR

        this.map_callback = new TransactionMapCallback(hstore_site);
        this.mapWrapper_callback = new TransactionMapWrapperCallback(hstore_site);
    }
    
//    @Override
//    @Deprecated
//    public MapReduceTransaction init(long txnId, long clientHandle, int base_partition,
//                                     boolean predict_readOnly, boolean predict_canAbort) {
//        return (this);
//    }
    
    @Override
    public MapReduceTransaction init(long txnId, long clientHandle, int base_partition,
                                     Collection<Integer> predict_touchedPartitions, boolean predict_readOnly, boolean predict_canAbort,
                                     TransactionEstimator.State estimator_state, Procedure catalog_proc, StoredProcedureInvocation invocation, RpcCallback<byte[]> client_callback) {
        assert (invocation != null) : "invalid StoredProcedureInvocation parameter for MapReduceTransaction.init()";
        assert (catalog_proc != null) : "invalid Procedure parameter for MapReduceTransaction.init()";
        
        super.init(txnId, clientHandle, base_partition,
                   predict_touchedPartitions, predict_readOnly, predict_canAbort,
                   estimator_state, catalog_proc, invocation, client_callback);
        
        this.initLocalTransactions();
        this.setMapPhase();
        this.map_callback.init(this);
        assert(this.map_callback.isInitialized()) : "Unexpected error for " + this;

        LOG.info("Invoked MapReduceTransaction.init() -> " + this);
        return (this);
    }

    public MapReduceTransaction init(long txnId, int base_partition, Procedure catalog_proc, StoredProcedureInvocation invocation) {
        this.init(txnId, invocation.getClientHandle(), base_partition, hstore_site.getAllPartitionIds(), false, true, null, catalog_proc, invocation, null);
        LOG.info("Invoked MapReduceTransaction.init() -> " + this);
        assert(this.map_callback.isInitialized()) : "Unexpected error for " + this;
        return (this);
    }
    

    private void initLocalTransactions() {
        if (debug.get()) LOG.debug("Local Partitions: " + hstore_site.getLocalPartitionIds());
        for (int partition : this.hstore_site.getLocalPartitionIds()) {
            int offset = hstore_site.getLocalPartitionOffset(partition);
            if (trace.get()) LOG.trace(String.format("Partition[%d] -> Offset[%d]", partition, offset));
            this.local_txns[offset].init(this.txn_id, this.client_handle, partition,
                                         Collections.singleton(partition),
                                         this.predict_readOnly, this.predict_abortable,
                                         null, catalog_proc, invocation, null);
        } // FOR
    }

    @Override
    public void finish() {
        super.finish();
        for (int i = 0; i < this.local_txns.length; i++) {
            this.local_txns[i].finish();
        } // FOR
        this.state = null;
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

    public State getState() {
        return (this.state);
    }
    
    public boolean isMapPhase() {
        return (this.state == State.MAP);
    }

    public void setMapPhase() {
        assert (this.state == null);
        // FIXME(xin) this.state= true;
    }
    
    
    
    public void setShufflePhase() {
        // TODO(xin)
    }

    public boolean isReducePhase() {
        return (this.state == State.REDUCE);
    }

    public void setReducePhase() {
     // FIXME(xin)    assert (this.map_phase == true);
     // FIXME(xin) this.map_phase = false;
     // FIXME(xin) this.reduce_phase = true;
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
        if (debug.get()) LOG.debug("Trying to intialize TransactionMapWrapperCallback for " + this);
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

    @Override
    public void initRound(int partition, long undoToken) {
        assert (false) : "initRound should not be invoked on " + this.getClass();
//        int offset = hstore_site.getLocalPartitionOffset(partition);
//        this.local_txns[offset].initRound(partition, undoToken);
    }

    @Override
    public void startRound(int partition) {
        assert (false) : "startRound should not be invoked on " + this.getClass();
//        int offset = hstore_site.getLocalPartitionOffset(partition);
//        this.local_txns[offset].startRound(partition);
    }

    @Override
    public void finishRound(int partition) {
        assert (false) : "finishRound should not be invoked on " + this.getClass();
//        int offset = hstore_site.getLocalPartitionOffset(partition);
//        this.local_txns[offset].finishRound(partition);
    }

}
