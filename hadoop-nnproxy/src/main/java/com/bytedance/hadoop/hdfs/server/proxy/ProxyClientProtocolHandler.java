package com.bytedance.hadoop.hdfs.server.proxy;

import com.bytedance.hadoop.hdfs.server.NNProxy;
import com.bytedance.hadoop.hdfs.server.upstream.UpstreamManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/** */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProxyClientProtocolHandler implements ClientProtocol {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyClientProtocolHandler.class);

    final NNProxy nnProxy;
    final Configuration conf;
    final UpstreamManager upstreamManager;
    final Router router;

    public ProxyClientProtocolHandler(NNProxy nnProxy, Configuration conf, UpstreamManager upstreamManager) {
        this.nnProxy = nnProxy;
        this.conf = conf;
        this.upstreamManager = upstreamManager;
        this.router = new Router(nnProxy, conf, upstreamManager);
    }

    void ensureCanRename(String path) throws IOException {
        if (nnProxy.getMounts().isMountPoint(path)) {
            throw new IOException("Cannot rename a mount point (" + path + ")");
        }
        if (!nnProxy.getMounts().isUnified(path)) {
            throw new IOException("Cannot rename a non-unified directory " + path + " (contains mount point)");
        }
    }

    /* begin protocol handlers */

    @Override
    public LocatedBlocks getBlockLocations(String src, long offset, long length)
            throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.getBlockLocations(routeInfo.realPath, offset, length);
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
        return router.getRoot().upstream.getServerDefaults();
    }

    @Override
    public HdfsFileStatus create(String src, FsPermission masked, String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent, short replication, long blockSize, CryptoProtocolVersion[] supportedVersions) throws AccessControlException, AlreadyBeingCreatedException, DSQuotaExceededException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.create(routeInfo.realPath, masked, clientName, flag, createParent, replication, blockSize, supportedVersions);
    }

    @Override
    public LocatedBlock append(String src, String clientName) throws AccessControlException, DSQuotaExceededException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.append(routeInfo.realPath, clientName);
    }

    @Override
    public boolean setReplication(String src, short replication) throws AccessControlException, DSQuotaExceededException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.setReplication(routeInfo.realPath, replication);
    }

    @Override
    public BlockStoragePolicy[] getStoragePolicies() throws IOException {
        return router.getRoot().upstream.getStoragePolicies();
    }

    @Override
    public void setStoragePolicy(String src, String policyName) throws SnapshotAccessControlException, UnresolvedLinkException, FileNotFoundException, QuotaExceededException, IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.setStoragePolicy(routeInfo.realPath, policyName);
    }

    @Override
    public void setPermission(String src, FsPermission permission) throws
            AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.setPermission(routeInfo.realPath, permission);
    }

    @Override
    public void setOwner(String src, String username, String groupname) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.setOwner(routeInfo.realPath, username, groupname);
    }

    @Override
    public void abandonBlock(ExtendedBlock b, long fileId, String src, String holder) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.abandonBlock(b, fileId, routeInfo.realPath, holder);
    }

    @Override
    public LocatedBlock addBlock(String src, String clientName, ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId, String[] favoredNodes) throws AccessControlException, FileNotFoundException, NotReplicatedYetException, SafeModeException, UnresolvedLinkException, IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.addBlock(routeInfo.realPath, clientName, previous, excludeNodes, fileId, favoredNodes);
    }

    @Override
    public LocatedBlock getAdditionalDatanode(String src, long fileId, ExtendedBlock blk, DatanodeInfo[] existings, String[] existingStorageIDs, DatanodeInfo[] excludes, int numAdditionalNodes, String clientName) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.getAdditionalDatanode(routeInfo.realPath, fileId, blk, existings, existingStorageIDs, excludes, numAdditionalNodes, clientName);
    }

    @Override
    public boolean complete(String src, String clientName, ExtendedBlock last, long fileId) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.complete(routeInfo.realPath, clientName, last, fileId);
    }

    @Override
    public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
        Map<String, List<LocatedBlock>> fsBlocks = new HashMap<>();
        for (LocatedBlock blk : blocks) {
            String bpId = blk.getBlock().getBlockPoolId();
            String fs = nnProxy.getBlockPoolRegistry().getFs(bpId);
            if (fs == null) {
                throw new IOException("Unknown block pool: " + bpId);
            }
            if (!fsBlocks.containsKey(fs)) {
                fsBlocks.put(fs, new ArrayList<LocatedBlock>());
            }
            fsBlocks.get(fs).add(blk);
        }
        for (Map.Entry<String, List<LocatedBlock>> entry : fsBlocks.entrySet()) {
            String fs = entry.getKey();
            router.getProtocol(fs).reportBadBlocks(entry.getValue().toArray(new LocatedBlock[0]));
        }
    }

    @Override
    public boolean rename(String src, String dst) throws UnresolvedLinkException, SnapshotAccessControlException, IOException {
        ensureCanRename(src);
        ensureCanRename(dst);
        RouteInfo srcRouteInfo = router.route(src);
        RouteInfo dstRouteInfo = router.route(dst);
        if (!srcRouteInfo.fs.equals(dstRouteInfo.fs)) {
            throw new IOException("Cannot rename across namespaces");
        }
        return srcRouteInfo.upstream.rename(srcRouteInfo.realPath, dstRouteInfo.realPath);
    }

    @Override
    public void concat(String trg, String[] srcs) throws IOException, UnresolvedLinkException, SnapshotAccessControlException {
        RouteInfo trgRouteInfo = router.route(trg);
        RouteInfo[] routeInfos = new RouteInfo[srcs.length];
        for (int i = 0; i < srcs.length; i++) {
            routeInfos[i] = router.route(srcs[i]);
        }
        String fs = null;
        String[] newSrcs = new String[srcs.length];
        for (int i = 0; i < routeInfos.length; i++) {
            if (fs != null && !fs.equals(routeInfos[i].fs)) {
                throw new IOException("Cannot concat across namespaces");
            }
            fs = routeInfos[i].fs;
            newSrcs[i] = routeInfos[i].realPath;
        }
        if (fs != null && !fs.equals(trgRouteInfo.fs)) {
            throw new IOException("Cannot concat across namespaces");
        }
        trgRouteInfo.upstream.concat(trgRouteInfo.realPath, newSrcs);
    }

    @Override
    public void rename2(String src, String dst, Options.Rename... options) throws AccessControlException, DSQuotaExceededException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        ensureCanRename(src);
        ensureCanRename(dst);
        RouteInfo srcRouteInfo = router.route(src);
        RouteInfo dstRouteInfo = router.route(dst);
        if (!srcRouteInfo.fs.equals(dstRouteInfo.fs)) {
            throw new IOException("Cannot rename across namespaces");
        }
        srcRouteInfo.upstream.rename2(srcRouteInfo.realPath, dstRouteInfo.realPath, options);
    }

    @Override
    public boolean delete(String src, boolean recursive) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.delete(routeInfo.realPath, recursive);
    }

    @Override
    public boolean mkdirs(String src, FsPermission masked, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.mkdirs(routeInfo.realPath, masked, createParent);
    }

    @Override
    public DirectoryListing getListing(String src, byte[] startAfter, boolean needLocation) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.getListing(routeInfo.realPath, startAfter, needLocation);
    }

    @Override
    public SnapshottableDirectoryStatus[] getSnapshottableDirListing() throws IOException {
        return new SnapshottableDirectoryStatus[0];
    }

    @Override
    public void renewLease(String clientName) throws AccessControlException, IOException {
        // currently, just renew lease on all namenodes
        for (String fs : nnProxy.getMounts().getAllFs()) {
            router.getProtocol(fs).renewLease(clientName);
        }
    }

    @Override
    public boolean recoverLease(String src, String clientName) throws IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.recoverLease(routeInfo.realPath, clientName);
    }

    @Override
    public long[] getStats() throws IOException {
        return router.getRoot().upstream.getStats();
    }

    @Override
    public DatanodeInfo[] getDatanodeReport(HdfsConstants.DatanodeReportType type) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public DatanodeStorageReport[] getDatanodeStorageReport(HdfsConstants.DatanodeReportType type) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public long getPreferredBlockSize(String filename) throws IOException, UnresolvedLinkException {
        RouteInfo routeInfo = router.route(filename);
        return routeInfo.upstream.getPreferredBlockSize(routeInfo.realPath);
    }

    @Override
    public boolean setSafeMode(HdfsConstants.SafeModeAction action, boolean isChecked) throws IOException {
        if (action.equals(HdfsConstants.SafeModeAction.SAFEMODE_GET)) {
            // FIXME: properly handle
            return false;
        }
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public void saveNamespace() throws AccessControlException, IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public long rollEdits() throws AccessControlException, IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public boolean restoreFailedStorage(String arg) throws AccessControlException, IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public void refreshNodes() throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public void finalizeUpgrade() throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public RollingUpgradeInfo rollingUpgrade(HdfsConstants.RollingUpgradeAction action) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie) throws IOException {
        RouteInfo routeInfo = router.route(path);
        return routeInfo.upstream.listCorruptFileBlocks(routeInfo.realPath, cookie);
    }

    @Override
    public void metaSave(String filename) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public void setBalancerBandwidth(long bandwidth) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public HdfsFileStatus getFileInfo(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.getFileInfo(routeInfo.realPath);
    }

    @Override
    public boolean isFileClosed(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.isFileClosed(routeInfo.realPath);
    }

    @Override
    public HdfsFileStatus getFileLinkInfo(String src) throws AccessControlException, UnresolvedLinkException, IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.getFileInfo(routeInfo.realPath);
    }

    @Override
    public ContentSummary getContentSummary(String path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        RouteInfo routeInfo = router.route(path);
        return routeInfo.upstream.getContentSummary(routeInfo.realPath);
    }

    @Override
    public void setQuota(String path, long namespaceQuota, long diskspaceQuota) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        RouteInfo routeInfo = router.route(path);
        routeInfo.upstream.setQuota(routeInfo.realPath, namespaceQuota, diskspaceQuota);
    }

    @Override
    public void fsync(String src, long inodeId, String client, long lastBlockLength) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.fsync(routeInfo.realPath, inodeId, client, lastBlockLength);
    }

    @Override
    public void setTimes(String src, long mtime, long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.setTimes(routeInfo.realPath, mtime, atime);
    }

    @Override
    public void createSymlink(String target, String link, FsPermission dirPerm, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        RouteInfo routeInfo = router.route(target);
        routeInfo.upstream.getFileInfo(routeInfo.realPath);
    }

    @Override
    public String getLinkTarget(String path) throws AccessControlException, FileNotFoundException, IOException {
        RouteInfo routeInfo = router.route(path);
        return routeInfo.upstream.getLinkTarget(routeInfo.realPath);
    }

    @Override
    public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName) throws IOException {
        return router.getUpstreamForBlockPool(block.getBlockPoolId()).updateBlockForPipeline(block, clientName);
    }

    @Override
    public void updatePipeline(String clientName, ExtendedBlock oldBlock, ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs) throws IOException {
        if (!newBlock.getBlockPoolId().equals(oldBlock.getBlockPoolId())) {
            throw new IOException("Cannot update pipeline across block pools");
        }
        router.getUpstreamForBlockPool(newBlock.getBlockPoolId()).updatePipeline(clientName, oldBlock, newBlock, newNodes, newStorageIDs);
    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public DataEncryptionKey getDataEncryptionKey() throws IOException {
        return router.getRoot().upstream.getDataEncryptionKey();
    }

    @Override
    public String createSnapshot(String snapshotRoot, String snapshotName) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public void deleteSnapshot(String snapshotRoot, String snapshotName) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public void renameSnapshot(String snapshotRoot, String snapshotOldName, String snapshotNewName) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public void allowSnapshot(String snapshotRoot) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public void disallowSnapshot(String snapshotRoot) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot, String fromSnapshot, String toSnapshot) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public long addCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
        return nnProxy.getCacheRegistry().addCacheDirective(directive, flags);
    }

    @Override
    public void modifyCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
        nnProxy.getCacheRegistry().modifyCacheDirective(directive, flags);
    }

    @Override
    public void removeCacheDirective(long id) throws IOException {
        nnProxy.getCacheRegistry().removeCacheDirective(id);
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> listCacheDirectives(long prevId, CacheDirectiveInfo filter) throws IOException {
        return nnProxy.getCacheRegistry().listCacheDirectives(prevId, filter);
    }

    @Override
    public void addCachePool(CachePoolInfo info) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public void modifyCachePool(CachePoolInfo req) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public void removeCachePool(String pool) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<CachePoolEntry> listCachePools(String prevPool) throws IOException {
        return nnProxy.getCacheRegistry().listCachePools(prevPool);
    }

    @Override
    public void modifyAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.modifyAclEntries(routeInfo.realPath, aclSpec);
    }

    @Override
    public void removeAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.removeAclEntries(routeInfo.realPath, aclSpec);
    }

    @Override
    public void removeDefaultAcl(String src) throws IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.removeDefaultAcl(routeInfo.realPath);
    }

    @Override
    public void removeAcl(String src) throws IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.removeAcl(routeInfo.realPath);
    }

    @Override
    public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.setAcl(routeInfo.realPath, aclSpec);
    }

    @Override
    public AclStatus getAclStatus(String src) throws IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.getAclStatus(routeInfo.realPath);
    }

    @Override
    public void createEncryptionZone(String src, String keyName) throws IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.createEncryptionZone(routeInfo.realPath, keyName);
    }

    @Override
    public EncryptionZone getEZForPath(String src) throws IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.getEZForPath(routeInfo.realPath);
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<EncryptionZone> listEncryptionZones(long prevId) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag) throws IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.setXAttr(routeInfo.realPath, xAttr, flag);
    }

    @Override
    public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs) throws IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.getXAttrs(routeInfo.realPath, xAttrs);
    }

    @Override
    public List<XAttr> listXAttrs(String src) throws IOException {
        RouteInfo routeInfo = router.route(src);
        return routeInfo.upstream.listXAttrs(routeInfo.realPath);
    }

    @Override
    public void removeXAttr(String src, XAttr xAttr) throws IOException {
        RouteInfo routeInfo = router.route(src);
        routeInfo.upstream.removeXAttr(routeInfo.realPath, xAttr);

    }

    @Override
    public void checkAccess(String path, FsAction mode) throws IOException {
        RouteInfo routeInfo = router.route(path);
        routeInfo.upstream.checkAccess(routeInfo.realPath, mode);
    }

    @Override
    public long getCurrentEditLogTxid() throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

    @Override
    public EventBatchList getEditsFromTxid(long txid) throws IOException {
        throw new IOException("Invalid operation, do not use proxy");
    }

}
