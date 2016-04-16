package com.bytedance.hadoop.hdfs.server.mount;

import com.bytedance.hadoop.hdfs.ProxyConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Manages mount table and keep up-to-date to ZooKeeper.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class MountsManager extends AbstractService {

    private static final Logger LOG = LoggerFactory.getLogger(MountsManager.class);

    static class MountEntry {
        final String fsUri;
        final String mountPoint;
        final String[] attributes;

        public MountEntry(String fsUri, String mountPoint, String[] attributes) {
            this.fsUri = fsUri;
            this.mountPoint = mountPoint;
            this.attributes = attributes;
        }

        @Override
        public String toString() {
            return "MountEntry [" +
                    "fsUri=" + fsUri +
                    ", mountPoint=" + mountPoint +
                    ", attributes=" + Arrays.toString(attributes) +
                    ']';
        }
    }

    CuratorFramework framework;
    String zkMountTablePath;
    ImmutableList<MountEntry> mounts;
    ImmutableList<String> allFs;
    MountEntry root;
    NodeCache nodeCache;

    @VisibleForTesting
    protected volatile boolean installed;

    public MountsManager() {
        super("MountsManager");
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        String zkConnectString = conf.get(ProxyConfig.MOUNT_TABLE_ZK_QUORUM);
        zkMountTablePath = conf.get(ProxyConfig.MOUNT_TABLE_ZK_PATH);
        int sessionTimeout = conf.getInt(ProxyConfig.MOUNT_TABLE_ZK_SESSION_TIMEOUT,
                ProxyConfig.MOUNT_TABLE_ZK_SESSION_TIMEOUT_DEFAULT);
        int connectionTimeout = conf.getInt(ProxyConfig.MOUNT_TABLE_ZK_CONNECTION_TIMEOUT,
                ProxyConfig.MOUNT_TABLE_ZK_CONNECTION_TIMEOUT_DEFAULT);
        int maxRetries = conf.getInt(ProxyConfig.MOUNT_TABLE_ZK_MAX_RETRIES,
                ProxyConfig.MOUNT_TABLE_ZK_MAX_RETRIES_DEFAULT);
        int retryBaseSleep = conf.getInt(ProxyConfig.MOUNT_TABLE_ZK_RETRY_BASE_SLEEP,
                ProxyConfig.MOUNT_TABLE_ZK_RETRY_BASE_SLEEP_DEFAULT);
        framework = CuratorFrameworkFactory.newClient(
                zkConnectString, sessionTimeout, connectionTimeout,
                new ExponentialBackoffRetry(retryBaseSleep, maxRetries));
        installed = false;
    }

    public ImmutableList<MountEntry> getMounts() {
        return mounts;
    }

    public ImmutableList<String> getAllFs() {
        return allFs;
    }

    public String resolve(String path) {
        ImmutableList<MountEntry> entries = this.mounts;
        MountEntry chosen = null;
        for (MountEntry entry : entries) {
            if (path == null || !(path.startsWith(entry.mountPoint + "/") || path.equals(entry.mountPoint))) {
                continue;
            }
            if (chosen == null || chosen.mountPoint.length() < entry.mountPoint.length()) {
                chosen = entry;
            }
        }
        if (chosen == null) {
            chosen = root;
        }
        return chosen.fsUri;
    }

    /**
     * Determine whether given path is exactly a valid mount point
     *
     * @param path
     * @return
     */
    public boolean isMountPoint(String path) {
        ImmutableList<MountEntry> entries = this.mounts;
        for (MountEntry entry : entries) {
            if (entry.mountPoint.equals(path)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determine whether given path contains a mount point.
     * Directory is considered unified even if itself is a mount point, unless it contains another mount point.
     *
     * @param path
     * @return
     */
    public boolean isUnified(String path) {
        String prefix = path + "/";
        ImmutableList<MountEntry> entries = this.mounts;
        for (MountEntry entry : entries) {
            if (entry.mountPoint.startsWith(prefix)) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    protected void installMountTable(List<MountEntry> entries) {
        LOG.info("Installed mount table: " + entries);
        List<String> fs = new ArrayList<>();
        for (MountEntry entry : entries) {
            if (entry.mountPoint.equals("/")) {
                root = entry;
            }
            if (!fs.contains(entry.fsUri)) {
                fs.add(entry.fsUri);
            }
        }
        this.allFs = ImmutableList.copyOf(fs);
        this.mounts = ImmutableList.copyOf(entries);
        this.installed = true;
    }

    @VisibleForTesting
    protected List<MountEntry> parseMountTable(String mounts) {
        List<MountEntry> table = new ArrayList<>();
        boolean hasRoot = false;
        for (String s : mounts.split("\n")) {
            if (StringUtils.isEmpty(s)) {
                continue;
            }
            String[] cols = s.split(" ");
            String fsUri = cols[0];
            String mountPoint = cols[1];
            String[] attrs = (cols.length > 2) ? cols[2].split(",") : new String[0];
            table.add(new MountEntry(fsUri, mountPoint, attrs));
            if (mountPoint.equals("/")) {
                hasRoot = true;
            }
        }
        if (!hasRoot) {
            LOG.error("Ignored invalid mount table: " + mounts);
            return null;
        }
        return table;
    }

    @VisibleForTesting
    protected void handleMountTableChange(byte[] data) {
        if (data == null || data.length == 0) {
            LOG.info("Invalid mount table");
            return;
        }
        String mounts = new String(data);
        List<MountEntry> table = parseMountTable(mounts);
        if (table != null) {
            installMountTable(table);
        }
    }

    @Override
    protected void serviceStart() throws Exception {
        framework.start();
        nodeCache = new NodeCache(framework, zkMountTablePath, false);
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                handleMountTableChange(nodeCache.getCurrentData().getData());
            }
        });
        nodeCache.start(false);
    }

    @Override
    protected void serviceStop() throws Exception {
        nodeCache.close();
        framework.close();
    }

    public void waitUntilInstalled() throws InterruptedException {
        while (!installed) {
            Thread.sleep(100);
        }
    }

    public String dump() {
        ImmutableList<MountEntry> entries = this.mounts;
        StringBuilder result = new StringBuilder();
        for (MountEntry entry : entries) {
            result.append(entry.fsUri);
            result.append(' ');
            result.append(entry.mountPoint);
            result.append(' ');
            result.append(StringUtils.join(entry.attributes, ","));
            result.append('\n');
        }
        return result.toString();
    }

    public void load(String mounts) throws Exception {
        framework.setData().forPath(zkMountTablePath, mounts.getBytes());
    }
}
