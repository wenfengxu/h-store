package edu.mit.hstore.util;

import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.MapReduceTransaction;

public class MapReduceHelperThread implements Runnable { // TODO(Xin) Shutdownable
    private static final Logger LOG = Logger.getLogger(MapReduceHelperThread.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final LinkedBlockingDeque<MapReduceTransaction> queue = new LinkedBlockingDeque<MapReduceTransaction>();
    private final HStoreSite hstore_site;
    private Thread self = null; 
    
    public MapReduceHelperThread(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
    }
    
    public void queue(MapReduceTransaction ts) {
        this.queue.offer(ts);
    }
    
    
    /**
     * @see ExecutionSitePostProcessor
     */
    @Override
    public void run() {
        this.self = Thread.currentThread();
        this.self.setName(HStoreSite.getThreadName(hstore_site, "post"));
        if (hstore_site.getHStoreConf().site.cpu_affinity) {
            hstore_site.getThreadManager().registerProcessingThread();
        }
        if (debug.get())
            LOG.debug("Starting transaction post-processing thread");
        
        MapReduceTransaction ts = null;
        while (this.self.isInterrupted() == false) {
            // Grab a MapReduceTransaction from the queue
            // Figure out what you need to do with it
            // (1) Take all of the Map output tables and perform the shuffle operation
            
            
            switch (ts.getState()) {
                case SHUFFLE: {
                    
                    break;
                }
            } // SWITCH
            
        } // WHILE

    }
    
    protected void shuffle(MapReduceTransaction ts) {
        /**
         * TODO(xin): Loop through all of the MAP output tables from the txn handle
         * For each of those, iterate through the table row-by-row and use the PartitionEstimator
         * to determine what partition you need to send the row to.
         * @see LoadMultipartitionTable.createNonReplicatedPlan()
         * 
         * Then you will use HStoreCoordinator.sendData() to send the partitioned table data to
         * each of the partitions.
         * 
         * Once that is all done, clean things up and invoke the network-outbound callback stored in the
         * TransactionMapWrapperCallback 
         */
        
        PartitionEstimator p_estimator = hstore_site.getPartitionEstimator();
        
        
        
    }

}
