package org.voltdb;

import org.apache.log4j.Logger;
import org.apache.log4j.helpers.LogLog;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.TransactionMapResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.mit.hstore.HStoreCoordinator;
import edu.mit.hstore.callbacks.TransactionMapCallback;
import edu.mit.hstore.callbacks.TransactionMapWrapperCallback;
import edu.mit.hstore.dtxn.LocalTransaction;
import edu.mit.hstore.dtxn.MapReduceTransaction;

public abstract class VoltMapReduceProcedure extends VoltProcedure {
    public static final Logger LOG = Logger.getLogger(VoltMapReduceProcedure.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private SQLStmt mapInputQuery;
    private SQLStmt reduceInputQuery;

    private MapReduceTransaction mr_ts;
    
    @Override
    public void globalInit(ExecutionSite site, Procedure catalogProc, BackendTarget eeType, HsqlBackend hsql,
            PartitionEstimator pEstimator) {
        super.globalInit(site, catalogProc, eeType, hsql, pEstimator);
        
        // Get the SQLStmt handles for the input queries
        this.mapInputQuery = this.getSQLStmt(catalogProc.getMapinputquery());
        assert (this.mapInputQuery != null) : "Missing " + catalogProc.getMapinputquery();
        this.reduceInputQuery = this.getSQLStmt(catalogProc.getReduceinputquery());
        assert (this.reduceInputQuery != null) : "Missing " + catalogProc.getReduceinputquery();
    }
    
    /**
     * 
     * @return
     */
    public final VoltTable run(Object params[]) {
        assert (this.hstore_site != null) : "error in VoltMapReduceProcedure...for hstore_site..........";

        VoltTable result = null;

        // The MapReduceTransaction handle will have all the key information we need about this txn
        long txn_id = this.getTransactionId();
        this.mr_ts = this.hstore_site.getTransaction(txn_id);
        assert (mr_ts != null) : "Unexpected null MapReduceTransaction handle for " + this.m_localTxnState;

        // If this invocation is at the txn's base partition, then it is
        // responsible for sending out the coordination messages to the other partitions
        boolean is_local = (this.partitionId == mr_ts.getBasePartition());

        if (mr_ts.isMapPhase()) {
            // If this is the base partition, then we'll send the out the MAP
            // initialization requests to all of the partitions
            if (is_local) {
                // Send out network messages to all other partitions to tell them to
                // execute the MAP phase of this job
                this.executor.hstore_coordinator.transactionMap(mr_ts, mr_ts.getTransactionMapCallback());
            }

            if (debug.get())
                LOG.debug("<VoltMapReduceProcedure.run> is executing ..<MAP>..\n");
            // XXX: Execute the map
            this.runMap(params);
            result = mr_ts.getMapOutputByPartition(this.partitionId);

            // Always invoke the TransactionMapWrapperCallback to let somebody know that
            // we finished the MAP phase at this partition
            TransactionMapWrapperCallback callback = mr_ts.getTransactionMapWrapperCallback();
            assert (callback != null) : "Unexpected null callback for " + mr_ts;
            assert (callback.isInitialized()) : "Unexpected uninitalized callback for " + mr_ts;
            callback.run(this.partitionId);
        }

        else if (mr_ts.isReducePhase()) {
            if (debug.get())
                LOG.debug("<VoltMapReduceProcedure.run> is executing ..<Reduce>..\n");
            

          
        }
        return (result);

    }
    

    public abstract void map(VoltTableRow tuple);

    public abstract void reduce(VoltTable[] r);

    public final void runMap(Object params[]) {
        voltQueueSQL(mapInputQuery, params);
        VoltTable mapResult[] = voltExecuteSQLForceSinglePartition();
        assert (mapResult.length == 1);

        while (mapResult[0].advanceRow()) {
            this.map(mapResult[0].getRow());
        } // WHILE
    }

    public final void runReduce(Object params[]) {
        voltQueueSQL(reduceInputQuery, params);
        VoltTable reduceResult[] = voltExecuteSQL();
        assert (reduceResult.length == 1);

        this.reduce(reduceResult);
    }

    public final void mapEmit(Object row[]) {
        // DONE(xin): Update the mapOutput for this partition in mr_ts 
        mr_ts.getMapOutputByPartition(this.partitionId).addRow(row);       
    }

    public final void reduceEmit(Object row[]) {
        
        mr_ts.getReduceOutputByPartition(this.partitionId).addRow(row);
    }



}

