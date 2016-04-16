package com.bytedance.hadoop.hdfs.server.proxy;

import com.bytedance.hadoop.hdfs.server.NNProxy;
import com.bytedance.hadoop.hdfs.ProxyConfig;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class ProxyServer {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyServer.class);

    final NNProxy nnProxy;
    final Configuration conf;
    final InvocationHandler invocationHandler;

    RPC.Server rpcServer;
    InetSocketAddress rpcAddress;
    ClientProtocol protocol;

    public ProxyServer(NNProxy nnProxy, Configuration conf, InvocationHandler invocationHandler) {
        this.nnProxy = nnProxy;
        this.conf = conf;
        this.invocationHandler = invocationHandler;
    }

    public void start() throws IOException {
        int rpcHandlerCount = conf.getInt(ProxyConfig.PROXY_HANDLER_COUNT, ProxyConfig.PROXY_HANDLER_COUNT_DEFAULT);
        RPC.setProtocolEngine(conf, ClientNamenodeProtocolPB.class,
                ProtobufRpcEngine.class);
        RPC.setProtocolEngine(conf, NamenodeProtocolPB.class,
                ProtobufRpcEngine.class);

        this.protocol = (ClientProtocol) Proxy.newProxyInstance(
                this.getClass().getClassLoader(),
                new Class[]{ClientProtocol.class},
                this.invocationHandler);

        ClientNamenodeProtocolPB proxy = new ClientNamenodeProtocolServerSideTranslatorPB(this.protocol);
        BlockingService clientNNPbService = ClientNamenodeProtocolProtos.ClientNamenodeProtocol.
                newReflectiveBlockingService(proxy);

        int port = conf.getInt(ProxyConfig.RPC_PORT, ProxyConfig.RPC_PORT_DEFAULT);

        this.rpcServer = new RPC.Builder(conf)
                .setProtocol(org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB.class)
                .setInstance(clientNNPbService).setBindAddress("0.0.0.0")
                .setPort(port).setNumHandlers(rpcHandlerCount)
                .setVerbose(false).build();
        this.rpcServer.start();

        InetSocketAddress listenAddr = rpcServer.getListenerAddress();
        rpcAddress = new InetSocketAddress("0.0.0.0", listenAddr.getPort());
    }

    public InetSocketAddress getRpcAddress() {
        return rpcAddress;
    }

    public ClientProtocol getProtocol() {
        return protocol;
    }

    public void join() throws InterruptedException {
        this.rpcServer.join();
    }

    public void shutdown() {
        this.rpcServer.stop();
    }
}

