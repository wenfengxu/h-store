package org.voltdb;

import java.util.Iterator;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.mit.hstore.callbacks.TransactionMapWrapperCallback;
import edu.mit.hstore.dtxn.MapReduceTransaction;

public abstract class VoltMapReduceProcedure<K> extends VoltProcedure {
    public static final Logger LOG = Logger.getLogger(VoltMapReduceProcedure.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private SQLStmt mapInputQuery;
    private SQLStmt reduceInputQuery;

    // Thread-local data
    private MapReduceTransaction mr_ts;
    private VoltTable map_output;
    
    private VoltTable reduce_input;
    private VoltTable reduce_output;
    
    // -----------------------------------------------------------------
    // MAP REDUCE API
    // -----------------------------------------------------------------

    /**
     * Returns the schema of the MapOutput table
     * @return
     */
    public abstract VoltTable.ColumnInfo[] getMapOutputSchema();
    
    /**
     * TODO(xin)
     * @param tuple
     */
    public abstract void map(VoltTableRow tuple);

    /**
     * TODO(xin)
     * @param r
     */
    public abstract void reduce(K key, Iterator<VoltTableRow> rows);
    
    // -----------------------------------------------------------------
    // INTERNAL METHODS
    // -----------------------------------------------------------------
    
    @Override
    public void globalInit(ExecutionSite site, Procedure catalogProc, BackendTarget eeType, HsqlBackend hsql,
            PartitionEstimator pEstimator) {
        super.globalInit(site, catalogProc, eeType, hsql, pEstimator);
        
        // Get the SQLStmt handles for the input queries
        this.mapInputQuery = this.getSQLStmt(catalogProc.getMapinputquery());
        assert (this.mapInputQuery != null) : "Missing MapInputQuery " + catalogProc.getMapinputquery();
        this.reduceInputQuery = this.getSQLStmt(catalogProc.getReduceinputquery());
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
            
            this.map_output = mr_ts.getMapOutputByPartition(this.partitionId);
            assert(this.map_output != null);
            
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
            this.reduce_input = null; // mr_ts.getReduceInputByPartition(this.partitionId);
            assert(this.reduce_input != null);
            this.reduce_output = mr_ts.getReduceOutputByPartition(this.partitionId);
            assert(this.reduce_output != null);
            
            if (debug.get())
                LOG.debug("<VoltMapReduceProcedure.run> is executing ..<Reduce>..\n");
            
            // TODO(xin): If this is the local/base partition, send out the start REDUCE message 
            if (is_local) {
                // Send out network messages to all other partitions to tell them to
                // execute the MAP phase of this job
                // this.executor.hstore_coordinator.transactionReduce(mr_ts, mr_ts.getTransactionMapCallback());
            }
            
            // TODO(xin): Sort the the MAP_OUTPUT table
            VoltTable sorted = VoltTableUtil.sort(this.reduce_input, Pair.of(0, SortDirectionType.ASC));
            assert(sorted != null);
            
            // TODO(xin): Build an "smart" iterator that loops through the MAP_OUTPUT table key-by-key
            
            // TODO(xin): Loop over that iterator and call runReduce 
            
          
        }
        return (result);

    }
    


    private final void runMap(Object params[]) {
        voltQueueSQL(mapInputQuery, params);
        VoltTable mapResult[] = voltExecuteSQLForceSinglePartition();
        assert (mapResult.length == 1);

        while (mapResult[0].advanceRow()) {
            this.map(mapResult[0].getRow());
        } // WHILE
    }

    private final void runReduce(Object params[]) {
        voltQueueSQL(reduceInputQuery, params);
        VoltTable reduceResult[] = voltExecuteSQL();
        assert (reduceResult.length == 1);

        // this.reduce(params[0], reduceResult);
    }

    /**
     * TODO(xin)
     * @param key
     * @param row
     */
    public final void mapEmit(K key, Object row[]) {
        assert(key == row[0]);
        this.map_output.addRow(row);       
    }

    /**
     * TODO(xin)
     * @param row
     */
    public final void reduceEmit(Object row[]) {
        this.reduce_output.addRow(row);
    }



}

