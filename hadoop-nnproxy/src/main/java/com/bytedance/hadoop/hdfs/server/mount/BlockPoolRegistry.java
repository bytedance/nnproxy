package com.bytedance.hadoop.hdfs.server.mount;

import com.bytedance.hadoop.hdfs.ProxyConfig;
import com.bytedance.hadoop.hdfs.server.NNProxy;
import com.bytedance.hadoop.hdfs.server.upstream.UpstreamManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Provides blockPoolId to NameNode mapping.
 * This is based on the assumption that blockPoolId assigned for one particular FS never changes.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class BlockPoolRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(BlockPoolRegistry.class);

    final NNProxy nnProxy;
    final UpstreamManager upstreamManager;
    final Map<String, String> bp2fs;
    final String superuser;

    public BlockPoolRegistry(NNProxy proxy, Configuration conf, UpstreamManager upstreamManager) {
        this.nnProxy = proxy;
        this.upstreamManager = upstreamManager;
        this.bp2fs = new HashMap<>();
        this.superuser = conf.get(ProxyConfig.SUPERUSER, ProxyConfig.SUPERUSER_DEFAULT);
    }

    void refreshBlockPools() throws ExecutionException, IOException {
        for (String fs : nnProxy.getMounts().getAllFs()) {
            NamespaceInfo nsInfo = upstreamManager.getUpstream(superuser, fs).nnProxyAndInfo.getProxy().versionRequest();
            String bpId = nsInfo.getBlockPoolID();
            bp2fs.put(bpId, fs);
        }
    }

    public synchronized String getFs(String bpId) throws IOException {
        if (bp2fs.containsKey(bpId)) {
            return bp2fs.get(bpId);
        }
        try {
            refreshBlockPools();
        } catch (ExecutionException e) {
            LOG.error("Failed to refresh block pools", e);
        }
        return bp2fs.get(bpId);
    }

}
