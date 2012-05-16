package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionMapResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.dtxn.MapReduceTransaction;

/**
 * This callback waits until all of the TransactionMapResponses have come
 * back from all other partitions in the cluster. The unblockCallback will
 * switch the MapReduceTransaction handle into the REDUCE phase and then requeue
 * it at the local HStoreSite
 * @author pavlo
 */
public class TransactionMapCallback extends AbstractTransactionCallback<TransactionMapResponse, TransactionMapResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionMapCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionMapCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }

    public void init(MapReduceTransaction ts) {
        assert(this.isInitialized() == false) :
            String.format("Trying to initialize %s twice! [origTs=%s, newTs=%s]",
                          this.getClass().getSimpleName(), this.ts, ts);
        super.init(ts, ts.getPredictTouchedPartitions().size(), null);
    }
    
    /**
     * This gets invoked after all of the partitions have finished
     * executing the map phase for this txn
     */
    @Override
    protected boolean unblockTransactionCallback() {
        if (debug.get())
            LOG.debug(ts + " is ready to execute. Passing to HStoreSite " +
                    "<Switching to the 'reduce' phase>.......");
        
        MapReduceTransaction mr_ts = (MapReduceTransaction)this.ts;
        mr_ts.setReducePhase();
        assert(mr_ts.isReducePhase());
        
        if (hstore_site.getHStoreConf().site.mr_reduce_blocking){
            if (debug.get())
                LOG.debug(ts + ": $$$ normal reduce blocking execution way");
            // calling this hstore_site.transactionStart function will block the executing engine on each partition
            hstore_site.transactionStart(ts, ts.getBasePartition());
        } else {
            // throw reduce job to MapReduceHelperThread to do
            if (debug.get())
                LOG.debug(ts + ": $$$ non-blocking reduce execution by MapReduceHelperThread");
            hstore_site.getMapReduceHelper().queue(mr_ts);
        }
        return (false);
    }

    @Override
    protected boolean abortTransactionCallback(Status status) {
        assert(this.isInitialized()) : "ORIG TXN: " + this.getTransactionId();
        return (true);
    }
    
    @Override
    protected int runImpl(TransactionMapResponse response) {
        if (debug.get())
            LOG.debug(String.format("Got %s with status %s for %s [partitions=%s]",
                                    response.getClass().getSimpleName(),
                                    response.getStatus(),
                                    this.ts, 
                                    response.getPartitionsList()));
        assert(this.ts != null) :
            String.format("Missing LocalTransaction handle for txn #%d [status=%s]",
                          response.getTransactionId(), response.getStatus());
        // Otherwise, make sure it's legit
        assert(this.ts.getTransactionId().longValue() == response.getTransactionId()) :
            String.format("Unexpected %s for a different transaction %s != #%d [expected=#%d]",
                          response.getClass().getSimpleName(), this.ts, response.getTransactionId(), this.getTransactionId());
        
        if (response.getStatus() != Status.OK || this.isAborted()) {
            this.abort(response.getStatus());
        }
        return (response.getPartitionsCount());
    }
}