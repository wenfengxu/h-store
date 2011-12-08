package edu.mit.hstore.handlers;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.messaging.FastDeserializer;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.Hstore.HStoreService;
import edu.brown.hstore.Hstore.SendDataRequest;
import edu.brown.hstore.Hstore.SendDataResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreCoordinator;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransaction;
import edu.mit.hstore.dtxn.MapReduceTransaction;

public class SendDataHandler extends AbstractTransactionHandler<SendDataRequest, SendDataResponse> {
    private static final Logger LOG = Logger.getLogger(SendDataHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    //final Dispatcher<Object[]> MapDispatcher;
    
    public SendDataHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }
    
    @Override
    public void sendLocal(long txn_id, SendDataRequest request, Collection<Integer> partitions, RpcCallback<SendDataResponse> callback) {
        /*
         * We also need to make sure that if have to send
         * data to a partition that's on our same machine, then we don't want to 
         * waste time serializing + deserializing the data when didn't have to.
         * */
        //handler.sendData(null, request, callback);
        
        // later I will deal with sendLocal, now just send it to remoteHandler
        this.remoteHandler(null, request, callback);
    }
    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, SendDataRequest request, RpcCallback<SendDataResponse> callback) {
        channel.sendData(controller, request, callback);
    }
    @Override
    public void remoteQueue(RpcController controller, SendDataRequest request,
            RpcCallback<SendDataResponse> callback) {
        this.remoteHandler(controller, request, callback);
    }
    @Override
    public void remoteHandler(RpcController controller, SendDataRequest request,
            RpcCallback<SendDataResponse> callback) {
        assert(request.hasTransactionId()) : "Got Hstore." + request.getClass().getSimpleName() + " without a txn id!";
        long txn_id = request.getTransactionId();
        
        if (debug.get())
            LOG.debug("__FILE__:__LINE__ " + String.format("Got %s for txn #%d",
                                   request.getClass().getSimpleName(), txn_id));

        // Deserialize the VoltTable object for mapOutput
        VoltTable mapOutputData = null;
        try {
            mapOutputData = FastDeserializer.deserialize(request.getData().toByteArray(), VoltTable.class);
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected error when deserializing VoltTable", ex);
        }
        assert(mapOutputData != null);
        MapReduceTransaction mr_ts = hstore_site.getTransaction(txn_id);
        
        mr_ts.initSendDataWrapperCallback(callback);
        int destPartition = request.getPartitionId();
        assert(hstore_site.getLocalPartitionIds().contains(destPartition));
//        for (int partition : ) {
//            if (partition == destPartition) { 
//                /*
//                 * The data on the remote side reducer needs is ready, then start reducer right now.
//                 * */
//                if (debug.get())
//                    LOG.debug("__FILE__:__LINE__ " + String.format("Got %s for txn #%d start Reducer on %d for ",
//                                    request.getClass().getSimpleName(),txn_id, destPartition));
//            }
//        } // FOR
       
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionWorkController(site_id);
    }
}
