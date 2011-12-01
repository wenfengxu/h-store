package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.MapReduceTransaction;

/**
 * This callback waits until all of the TransactionMapResponses have come
 * back from all other partitions in the cluster.
 * @author pavlo
 */
public class TransactionMapCallback extends BlockingCallback<Hstore.TransactionMapResponse, Hstore.TransactionMapResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionMapCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private MapReduceTransaction ts;
    private Integer reject_partition = null;
    private Long reject_txnId = null;
    private TransactionFinishCallback finish_callback;
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionMapCallback(HStoreSite hstore_site) {
        super(hstore_site, true);
    }

    public void init(MapReduceTransaction ts) {
        if (debug.get())
            LOG.debug("Starting new " + this.getClass().getSimpleName() + " for " + ts);
        this.ts = ts;
        this.finish_callback = null;
        this.reject_partition = null;
        this.reject_txnId = null;
        super.init(ts.getTransactionId(), ts.getPredictTouchedPartitions().size(), null);
    }
    
    @Override
    protected void finishImpl() {
        this.ts = null;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.ts != null);
    }
    
    /**
     * This gets invoked after all of the partitions have finished
     * executing the map phase for this txn
     */
    @Override
    protected void unblockCallback() {
        if (this.isAborted() == false) {
            if (debug.get())
                LOG.debug(ts + " is ready to execute. Passing to HStoreSite");
            
            // TODO(xin): Switch the txn to the 'reduce' phase
            ts.setReducePhase();
            hstore_site.transactionStart(ts, ts.getBasePartition());
        } else {
            assert(this.finish_callback != null);
            this.finish_callback.allowTransactionCleanup();
        }
    }
    
    public synchronized void setRejectionInfo(int partition, long txn_id) {
        this.reject_partition = partition;
        this.reject_txnId = txn_id;
    }
    
    @Override
    protected void abortCallback(Status status) {
        assert(this.isInitialized()) : "ORIG TXN: " + this.getOrigTransactionId();
        
        // Then re-queue the transaction. We want to make sure that
        // we use a new LocalTransaction handle because this one is going to get freed
        // We want to do this first because the transaction state could get
        // cleaned-up right away when we call HStoreCoordinator.transactionFinish()
        switch (status) {
            case ABORT_RESTART: {
                // If we have the transaction that we got busted up with at the remote site
                // then we'll tell the TransactionQueueManager to unblock it when it gets released
                synchronized (this) {
                    if (this.reject_txnId != null) {
                        this.hstore_site.getTransactionQueueManager().queueBlockedDTXN(this.ts, this.reject_partition, this.reject_txnId);
                    } else {
                        this.hstore_site.transactionRestart(this.ts, status);
                    }
                } // SYNCH
                break;
            }
            case ABORT_THROTTLED:
            case ABORT_REJECT:
                this.hstore_site.transactionReject(this.ts, status);
                break;
            default:
                assert(false) : String.format("Unexpected status %s for %s", status, this.ts);
        } // SWITCH
        
        // If we abort, then we have to send out an ABORT to
        // all of the partitions that we originally sent INIT requests too
        // Note that we do this *even* if we haven't heard back from the remote
        // HStoreSite that they've acknowledged our transaction
        // We don't care when we get the response for this
        this.finish_callback = this.ts.getTransactionFinishCallback(status);
        this.finish_callback.disableTransactionCleanup();
        this.hstore_site.getCoordinator().transactionFinish(this.ts, status, this.finish_callback);
    }
    
    @Override
    protected int runImpl(Hstore.TransactionMapResponse response) {
        if (debug.get())
            LOG.debug(String.format("Got %s with status %s for %s [partitions=%s]",
                                    response.getClass().getSimpleName(),
                                    response.getStatus(),
                                    this.ts, 
                                    response.getPartitionsList()));
        assert(this.ts != null) :
            String.format("Missing MapReduceTransaction handle for txn #%d", response.getTransactionId());
        assert(response.getPartitionsCount() > 0) :
            String.format("No partitions returned in %s for %s", response.getClass().getSimpleName(), this.ts);
        
        long orig_txn_id = this.getOrigTransactionId();
        long resp_txn_id = response.getTransactionId();
        long ts_txn_id = this.ts.getTransactionId();
        
        // If we get a response that matches our original txn but the LocalTransaction handle 
        // has changed, then we need to will just ignore it
        if (orig_txn_id == resp_txn_id && orig_txn_id != ts_txn_id) {
            if (debug.get()) LOG.debug(String.format("Ignoring %s for a different transaction #%d [origTxn=#%d]",
                                                     response.getClass().getSimpleName(), resp_txn_id, orig_txn_id));
            return (0);
        }
        // Otherwise, make sure it's legit
        assert(ts_txn_id == resp_txn_id) :
            String.format("Unexpected %s for a different transaction %s != #%d [expected=#%d]",
                          response.getClass().getSimpleName(), this.ts, resp_txn_id, ts_txn_id);
        
        if (response.getStatus() != Hstore.Status.OK || this.isAborted()) {
            this.abort(response.getStatus());
            return (0);
        }
        return (response.getPartitionsCount());
    }
}