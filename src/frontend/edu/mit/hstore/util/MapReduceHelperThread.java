package edu.mit.hstore.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.collections15.map.IdentityMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProfileMeasurement;
import edu.mit.hstore.HStoreConf;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.callbacks.SendDataWrapperCallback;
import edu.mit.hstore.callbacks.TransactionMapWrapperCallback;
import edu.mit.hstore.dtxn.MapReduceTransaction;
import edu.mit.hstore.interfaces.Shutdownable;

public class MapReduceHelperThread implements Runnable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(MapReduceHelperThread.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final LinkedBlockingDeque<MapReduceTransaction> queue = new LinkedBlockingDeque<MapReduceTransaction>();
    private final HStoreSite hstore_site;
    private final PartitionEstimator p_estimator;
    private Thread self = null;
    private boolean stop = false;
    
    public MapReduceHelperThread(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.p_estimator = hstore_site.getPartitionEstimator();
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
        this.self.setName(HStoreSite.getThreadName(hstore_site, "MR"));
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
            try {
                ts = this.queue.take();
            } catch (InterruptedException ex) {
                // Ignore!
                break;
            }
            assert (ts != null);

            if (ts.isShufflePhase()) {
                this.shuffle(ts);

                TransactionMapWrapperCallback callback = ts.getTransactionMapWrapperCallback();
                callback.runOrigCallback();
            }

        } // WHILE

    }

    protected void shuffle(MapReduceTransaction ts) {
        /**
         * TODO(xin): Loop through all of the MAP output tables from the txn handle For each of those, iterate through
         * the table row-by-row and use the PartitionEstimator to determine what partition you need to send the row to.
         * 
         * @see LoadMultipartitionTable.createNonReplicatedPlan()
         * 
         *      Then you will use HStoreCoordinator.sendData() to send the partitioned table data to each of the
         *      partitions.
         * 
         *      Once that is all done, clean things up and invoke the network-outbound callback stored in the
         *      TransactionMapWrapperCallback
         */

        // create a table for each partition
        Map<Integer, VoltTable> partitionedTables = new HashMap<Integer, VoltTable>();
        for (int partition : hstore_site.getAllPartitionIds()) {
            partitionedTables.put(partition, CatalogUtil.getVoltTable(ts.getMapEmit()));
        } // FOR
        if (debug.get()) LOG.debug(String.format("Created %d VoltTables for SHUFFLE phase of %s", partitionedTables.size(), ts));
        
        VoltTable table = null;
        for (int partition : this.hstore_site.getLocalPartitionIds()) {
            table = ts.getMapOutputByPartition(partition);
            assert (table != null) : String.format("Missing MapOutput table for txn #%d", ts.getTransactionId());

            while (table.advanceRow()) {
                VoltTableRow row = table.fetchRow(table.getActiveRowIndex());
                int rowPartition = -1;
                try {
                    rowPartition = p_estimator.getTableRowPartition(ts.getMapEmit(), row);
                } catch (Exception e) {
                    LOG.fatal("Failed to split input table into partitions", e);
                    throw new RuntimeException(e.getMessage());
                }
                assert (rowPartition >= 0);
                // this adds the active row from table
                partitionedTables.get(rowPartition).add(row);
            } // WHILE
        } // FOR
        
        this.hstore_site.getCoordinator().sendData(ts, partitionedTables, ts.getSendDataCallback());
//            SendDataWrapperCallback callback = ts.getSendDataWrapper_callback();
//            assert (callback != null) : "Unexpected null callback for " + ts;
//            assert (callback.isInitialized()) : "Unexpected uninitalized callback for " + ts;
//            callback.run(p);
//        } // for
    }

    @Override
    public boolean isShuttingDown() {

        return (this.stop);
    }

    @Override
    public void prepareShutdown(boolean error) {
        this.queue.clear();

    }

    @Override
    public void shutdown() {

        if (debug.get())
            LOG.debug(String.format("MapReduce Transaction helper Thread should be shutdown now ..."));
        this.stop = true;
        if (this.self != null)
            this.self.interrupt();
    }

}
