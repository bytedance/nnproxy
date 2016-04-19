package com.bytedance.hadoop.hdfs.server.cache;

import com.bytedance.hadoop.hdfs.ProxyConfig;
import com.bytedance.hadoop.hdfs.server.NNProxy;
import com.bytedance.hadoop.hdfs.server.upstream.UpstreamManager;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

/**
 * This manages a view of cache pools and directives aggregated from all backend NameNodes.
 * View is always updated in async fashion, thus view may be inconsistent after update.
 * Note that CachePool id may be indistinguishable between NameNodes.
 * To solve the complex, each id is marked with FsId on higher 16 bits.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CacheRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(CacheRegistry.class);

    final NNProxy nnProxy;
    final UpstreamManager upstreamManager;
    TreeMap<Long, CacheDirectiveEntry> directivesById =
            new TreeMap<>();
    TreeMap<String, CachePoolEntry> cachePools =
            new TreeMap<>();
    Map<String, String> pool2fs = new HashMap<>();
    final String superuser;
    final int maxListCachePoolsResponses;
    final int maxListCacheDirectivesNumResponses;
    final long reloadIntervalMs;
    final Thread reloadThread;
    volatile boolean running;

    long maskDirectiveId(long id, long fsIndex) {
        id &= 0x0000ffffffffffffL;
        id |= (fsIndex << 48);
        return id;
    }

    long getFsIndex(long maskedId) {
        return maskedId >> 48;
    }

    long getDirectiveId(long maskedId) {
        return maskedId & 0x0000ffffffffffffL;
    }

    public CacheRegistry(NNProxy proxy, Configuration conf, UpstreamManager upstreamManager) {
        this.nnProxy = proxy;
        this.upstreamManager = upstreamManager;
        this.superuser = conf.get(ProxyConfig.SUPERUSER, ProxyConfig.SUPERUSER_DEFAULT);

        this.maxListCachePoolsResponses = conf.getInt(
                DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES,
                DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES_DEFAULT);
        this.maxListCacheDirectivesNumResponses = conf.getInt(
                DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES,
                DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES_DEFAULT);
        this.reloadIntervalMs = conf.getLong(
                ProxyConfig.CACHE_REGISTRY_RELOAD_INTERVAL_MS,
                ProxyConfig.CACHE_REGISTRY_RELOAD_INTERVAL_MS_DEFAULT);
        this.reloadThread = new Thread(new Runnable() {
            @Override
            public void run() {
                reloader();
            }
        });
        this.reloadThread.setName("Cache Registry Reloader");
        this.reloadThread.setDaemon(true);
    }

    public void start() {
        this.running = true;
        this.reloadThread.start();
    }

    public void shutdown() {
        this.running = false;
        this.reloadThread.interrupt();
    }

    List<CacheDirectiveEntry> getAllCacheDirectives(UpstreamManager.Upstream upstream) throws IOException {
        CacheDirectiveInfo filter = new CacheDirectiveInfo.Builder().build();
        List<CacheDirectiveEntry> directives = new ArrayList<>();
        long prevId = -1;
        while (true) {
            BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> it =
                    upstream.protocol.listCacheDirectives(prevId, filter);
            if (it.size() == 0) {
                break;
            }
            for (int i = 0; i < it.size(); i++) {
                CacheDirectiveEntry entry = it.get(i);
                prevId = entry.getInfo().getId();
                directives.add(entry);
            }
        }
        return directives;
    }

    List<CachePoolEntry> getAllCachePools(UpstreamManager.Upstream upstream) throws IOException {
        String prevPool = "";
        List<CachePoolEntry> pools = new ArrayList<>();

        while (true) {
            BatchedRemoteIterator.BatchedEntries<CachePoolEntry> it = upstream.protocol.listCachePools(prevPool);
            if (it.size() == 0) {
                break;
            }
            for (int i = 0; i < it.size(); i++) {
                CachePoolEntry entry = it.get(i);
                prevPool = entry.getInfo().getPoolName();
                pools.add(entry);
            }
        }
        return pools;
    }

    List<CacheDirectiveEntry> maskWithFsIndex(List<CacheDirectiveEntry> entries, int fsIndex) {
        List<CacheDirectiveEntry> masked = new ArrayList<>(entries.size());
        for (CacheDirectiveEntry entry : entries) {
            CacheDirectiveInfo info = new CacheDirectiveInfo.Builder()
                    .setId(maskDirectiveId(entry.getInfo().getId(), fsIndex))
                    .setPath(entry.getInfo().getPath())
                    .setReplication(entry.getInfo().getReplication())
                    .setPool(entry.getInfo().getPool())
                    .setExpiration(entry.getInfo().getExpiration())
                    .build();
            masked.add(new CacheDirectiveEntry(info, entry.getStats()));
        }
        return masked;
    }

    void reload() throws Exception {
        List<CacheDirectiveEntry> allDirectives = new ArrayList<>();
        List<CachePoolEntry> allPools = new ArrayList<>();
        Map<String, String> newPool2fs = new HashMap<>();
        int i = 0;
        for (String fs : nnProxy.getMounts().getAllFs()) {
            UpstreamManager.Upstream upstream = upstreamManager.getUpstream(superuser, fs);
            List<CachePoolEntry> pools = getAllCachePools(upstream);
            for (CachePoolEntry pool : pools) {
                newPool2fs.put(pool.getInfo().getPoolName(), fs);
            }
            allPools.addAll(pools);
            allDirectives.addAll(maskWithFsIndex(getAllCacheDirectives(upstream), i));
            i++;
        }
        TreeMap<Long, CacheDirectiveEntry> newDirectivesById =
                new TreeMap<>();
        TreeMap<String, CachePoolEntry> newCachePools =
                new TreeMap<>();
        for (CacheDirectiveEntry directive : allDirectives) {
            newDirectivesById.put(directive.getInfo().getId(), directive);
        }
        for (CachePoolEntry pool : allPools) {
            newCachePools.put(pool.getInfo().getPoolName(), pool);
        }
        LOG.debug("Cache directives: {}", newDirectivesById);
        LOG.debug("Cache pools: {}", newCachePools);
        LOG.debug("Cache pool to fs mapping: {}", newPool2fs);
        this.directivesById = newDirectivesById;
        this.cachePools = newCachePools;
        this.pool2fs = newPool2fs;
    }

    void reloader() {
        while (this.running) {
            try {
                reload();
            } catch (Exception e) {
                LOG.error("Failed to reload cache view", e);
            }
            try {
                Thread.sleep(reloadIntervalMs);
            } catch (InterruptedException e) {
                continue;
            }
        }
    }

    private static String validatePath(CacheDirectiveInfo directive)
            throws InvalidRequestException {
        if (directive.getPath() == null) {
            throw new InvalidRequestException("No path specified.");
        }
        String path = directive.getPath().toUri().getPath();
        if (!DFSUtil.isValidName(path)) {
            throw new InvalidRequestException("Invalid path '" + path + "'.");
        }
        return path;
    }

    public BatchedRemoteIterator.BatchedListEntries<CacheDirectiveEntry> listCacheDirectives(long prevId,
                                                                                             CacheDirectiveInfo filter) throws InvalidRequestException {
        final int NUM_PRE_ALLOCATED_ENTRIES = 16;
        String filterPath = null;
        if (filter.getPath() != null) {
            filterPath = validatePath(filter);
        }
        if (filter.getReplication() != null) {
            throw new InvalidRequestException(
                    "Filtering by replication is unsupported.");
        }

        // Querying for a single ID
        final Long id = filter.getId();
        if (id != null) {
            if (!directivesById.containsKey(id)) {
                throw new InvalidRequestException("Did not find requested id " + id);
            }
            // Since we use a tailMap on directivesById, setting prev to id-1 gets
            // us the directive with the id (if present)
            prevId = id - 1;
        }

        ArrayList<CacheDirectiveEntry> replies =
                new ArrayList<CacheDirectiveEntry>(NUM_PRE_ALLOCATED_ENTRIES);
        int numReplies = 0;
        SortedMap<Long, CacheDirectiveEntry> tailMap =
                directivesById.tailMap(prevId + 1);
        for (Map.Entry<Long, CacheDirectiveEntry> cur : tailMap.entrySet()) {
            if (numReplies >= maxListCacheDirectivesNumResponses) {
                return new BatchedRemoteIterator.BatchedListEntries<>(replies, true);
            }
            CacheDirectiveInfo info = cur.getValue().getInfo();

            // If the requested ID is present, it should be the first item.
            // Hitting this case means the ID is not present, or we're on the second
            // item and should break out.
            if (id != null &&
                    !(info.getId().equals(id))) {
                break;
            }
            if (filter.getPool() != null &&
                    !info.getPool().equals(filter.getPool())) {
                continue;
            }
            if (filterPath != null &&
                    !info.getPath().toUri().getPath().equals(filterPath)) {
                continue;
            }
            replies.add(cur.getValue());
            numReplies++;
        }
        return new BatchedRemoteIterator.BatchedListEntries<>(replies, false);
    }

    public BatchedRemoteIterator.BatchedListEntries<CachePoolEntry> listCachePools(String prevKey) {
        final int NUM_PRE_ALLOCATED_ENTRIES = 16;
        ArrayList<CachePoolEntry> results =
                new ArrayList<CachePoolEntry>(NUM_PRE_ALLOCATED_ENTRIES);
        SortedMap<String, CachePoolEntry> tailMap = cachePools.tailMap(prevKey, false);
        int numListed = 0;
        for (Map.Entry<String, CachePoolEntry> cur : tailMap.entrySet()) {
            if (numListed++ >= maxListCachePoolsResponses) {
                return new BatchedRemoteIterator.BatchedListEntries<>(results, true);
            }
            results.add(cur.getValue());
        }
        return new BatchedRemoteIterator.BatchedListEntries<>(results, false);
    }

    UpstreamManager.Upstream getUpstream(String pool) throws IOException {
        String fs = pool2fs.get(pool);
        int fsIndex = -1;
        if (fs == null) {
            throw new IOException("Cannot find namespace associated with pool " + pool);
        }
        ImmutableList<String> allFs = nnProxy.getMounts().getAllFs();
        for (int i = 0; i < allFs.size(); i++) {
            if (allFs.get(i).equals(fs)) {
                fsIndex = i;
                break;
            }
        }
        if (fsIndex < 0) {
            throw new IOException("No fs index associated with fs " + fs);
        }
        try {
            UpstreamManager.Upstream upstream = upstreamManager.getUpstream(superuser, fs);
            upstream.setFsIndex(fsIndex);
            return upstream;
        } catch (ExecutionException e) {
            throw new IOException("Failed to get upstream");
        }
    }

    public long addCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags)
            throws IOException {
        UpstreamManager.Upstream upstream = getUpstream(directive.getPool());
        long id = maskDirectiveId(upstream.protocol.addCacheDirective(directive, flags), upstream.fsIndex);
        reloadThread.interrupt();
        return id;
    }

    public void modifyCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags)
            throws IOException {
        UpstreamManager.Upstream upstream = getUpstream(directive.getPool());
        upstream.protocol.modifyCacheDirective(directive, flags);
        reloadThread.interrupt();
    }

    public void removeCacheDirective(long id) throws IOException {
        int fsIndex = (int) getFsIndex(id);
        long directiveId = getDirectiveId(id);
        ImmutableList<String> allFs = nnProxy.getMounts().getAllFs();
        if (allFs.size() <= fsIndex) {
            throw new IOException("No fs associated with index " + fsIndex);
        }
        UpstreamManager.Upstream upstream;
        try {
            upstream = upstreamManager.getUpstream(superuser, allFs.get(fsIndex));
        } catch (ExecutionException e) {
            throw new IOException("Failed to get upstream");
        }
        upstream.protocol.removeCacheDirective(directiveId);
        reloadThread.interrupt();
    }
}
