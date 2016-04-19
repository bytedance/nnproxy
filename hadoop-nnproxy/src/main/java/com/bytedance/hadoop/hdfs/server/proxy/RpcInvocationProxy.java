package com.bytedance.hadoop.hdfs.server.proxy;

import com.bytedance.hadoop.hdfs.server.NNProxy;
import com.bytedance.hadoop.hdfs.server.exception.WrappedExecutionException;
import com.bytedance.hadoop.hdfs.server.upstream.UpstreamManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class RpcInvocationProxy implements InvocationHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RpcInvocationProxy.class);

    final NNProxy nnProxy;
    final Configuration conf;
    final ProxyClientProtocolHandler protocolHandler;
    volatile boolean isShuttingDown;
    final AtomicLong activeRequests;

    public RpcInvocationProxy(NNProxy nnProxy, Configuration conf, UpstreamManager upstreamManager) {
        this.nnProxy = nnProxy;
        this.conf = conf;
        this.protocolHandler = new ProxyClientProtocolHandler(nnProxy, conf, upstreamManager);
        this.isShuttingDown = false;
        this.activeRequests = new AtomicLong(0);
    }

    void setupClientAddress() {
        String clientAddress = Server.getRemoteAddress();
        Client.setClientAddress(clientAddress);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (isShuttingDown) {
            throw new StandbyException("Proxy is shutting down");
        }
        try {
            activeRequests.incrementAndGet();
            setupClientAddress();
            return method.invoke(protocolHandler, args);
        } catch (InvocationTargetException e) {
            LOG.error("Error handling client", e);
            if (e.getCause() instanceof RemoteException) {
                // needs to pass RemoteException to client untouched
                RemoteException remoteException = (RemoteException) e.getCause();
                throw new ProxyRpcServerException(
                        RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.ERROR,
                        remoteException.getErrorCode(),
                        remoteException.getClassName(),
                        remoteException.getMessage());
            } else {
                throw e.getCause();
            }
        } catch (WrappedExecutionException e) {
            LOG.error("Error handling client", e);
            throw e.getCause();
        } catch (Exception e) {
            // log errors here otherwise no trace is left on server side
            LOG.error("Error handling client", e);
            throw e;
        } finally {
            activeRequests.decrementAndGet();
        }
    }

    public void shutdown() {
        isShuttingDown = true;
        // sleep a moment just to make sure all requests are accounted in activeRequests
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {

        }
        while (activeRequests.get() > 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }
            LOG.info("Waiting for all requests to finish... " + activeRequests.get() + " left");
        }
    }
}
