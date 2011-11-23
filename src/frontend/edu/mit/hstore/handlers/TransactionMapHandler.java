package edu.mit.hstore.handlers;

import java.util.Collection;

import org.apache.log4j.Logger;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.Hstore.HStoreService;
import edu.brown.hstore.Hstore.TransactionMapRequest;
import edu.brown.hstore.Hstore.TransactionMapResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreCoordinator;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransaction;

public class TransactionMapHandler extends AbstractTransactionHandler<TransactionMapRequest, TransactionMapResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionMapHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public TransactionMapHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        super(hstore_site, hstore_coord);
    }
    
    @Override
    public void sendLocal(long txn_id, TransactionMapRequest request, Collection<Integer> partitions, RpcCallback<TransactionMapResponse> callback) {
        handler.transactionMap(null, request, callback);
    }
    @Override
    public void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionMapRequest request, RpcCallback<TransactionMapResponse> callback) {
        channel.transactionMap(controller, request, callback);
    }
    @Override
    public void remoteQueue(RpcController controller, TransactionMapRequest request,
            RpcCallback<TransactionMapResponse> callback) {
        this.remoteHandler(controller, request, callback);
    }
    @Override
    public void remoteHandler(RpcController controller, TransactionMapRequest request,
            RpcCallback<TransactionMapResponse> callback) {
        assert(request.hasTransactionId()) : "Got Hstore." + request.getClass().getSimpleName() + " without a txn id!";
        long txn_id = request.getTransactionId();
        if (debug.get())
            LOG.debug("__FILE__:__LINE__ " + String.format("Got %s for txn #%d",
                                   request.getClass().getSimpleName(), txn_id));

        // TODO(xin)
    }
    @Override
    protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
        return ts.getTransactionWorkController(site_id);
    }
}
