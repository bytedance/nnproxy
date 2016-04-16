package com.bytedance.hadoop.hdfs.server;

import com.bytedance.hadoop.hdfs.BDManifest;
import com.bytedance.hadoop.hdfs.server.cache.CacheRegistry;
import com.bytedance.hadoop.hdfs.server.mount.BlockPoolRegistry;
import com.bytedance.hadoop.hdfs.server.mount.MountsManager;
import com.bytedance.hadoop.hdfs.server.proxy.ProxyMetrics;
import com.bytedance.hadoop.hdfs.server.proxy.ProxyServer;
import com.bytedance.hadoop.hdfs.server.proxy.RpcInvocationProxy;
import com.bytedance.hadoop.hdfs.server.upstream.UpstreamManager;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class NNProxy {

    private static final Logger LOG = LoggerFactory.getLogger(NNProxy.class);

    protected final Configuration conf;
    @VisibleForTesting
    protected MountsManager mounts;
    protected final UpstreamManager upstreamManager;
    protected final BlockPoolRegistry blockPoolRegistry;
    protected final CacheRegistry cacheRegistry;
    protected final RpcInvocationProxy router;
    protected final ProxyServer server;

    public static ProxyMetrics proxyMetrics;

    public NNProxy(Configuration conf) throws Exception {
        DefaultMetricsSystem.initialize("NNProxy");
        proxyMetrics = ProxyMetrics.create(conf);
        this.conf = conf;
        this.mounts = new MountsManager();

        this.upstreamManager = new UpstreamManager(this, conf);
        this.blockPoolRegistry = new BlockPoolRegistry(this, conf, upstreamManager);
        this.cacheRegistry = new CacheRegistry(this, conf, upstreamManager);
        this.router = new RpcInvocationProxy(this, conf, upstreamManager);

        this.server = new ProxyServer(this, conf, router);
    }

    public void start() throws IOException, InterruptedException {
        this.mounts.init(conf);
        this.mounts.start();
        this.mounts.waitUntilInstalled();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });
        this.cacheRegistry.start();
        this.server.start();
        LOG.info("Started nnproxy, revision " + BDManifest.getBuildNumber());
    }

    public void shutdown() {
        this.cacheRegistry.shutdown();
        LOG.info("Gracefully shutting down nnproxy...");
        this.router.shutdown();
        this.server.shutdown();
        LOG.info("NNProxy shutdown completed");
    }

    public void join() throws InterruptedException {
        this.server.join();
    }

    public MountsManager getMounts() {
        return mounts;
    }

    public Configuration getConf() {
        return conf;
    }

    public UpstreamManager getUpstreamManager() {
        return upstreamManager;
    }

    public BlockPoolRegistry getBlockPoolRegistry() {
        return blockPoolRegistry;
    }

    public CacheRegistry getCacheRegistry() {
        return cacheRegistry;
    }

    public RpcInvocationProxy getRouter() {
        return router;
    }

    public ProxyServer getServer() {
        return server;
    }

    public InetSocketAddress getRpcAddress() {
        return server.getRpcAddress();
    }

}
