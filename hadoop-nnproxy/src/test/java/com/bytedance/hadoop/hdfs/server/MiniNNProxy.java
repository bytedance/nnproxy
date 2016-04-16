package com.bytedance.hadoop.hdfs.server;

import com.bytedance.hadoop.hdfs.server.NNProxy;
import com.bytedance.hadoop.hdfs.server.mount.MountsManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.URI;

/** */
public class MiniNNProxy extends NNProxy {

    final String mountTable;
    final MiniDFSCluster[] clusters;
    ClientProtocol clientProtocol;
    DFSClient dfs;
    FileSystem fs;

    class MockedMountsManager extends MountsManager {

        @Override
        protected void serviceInit(Configuration conf) throws Exception {
            handleMountTableChange(mountTable.getBytes());
        }

        @Override
        protected void serviceStart() throws Exception {
        }

        @Override
        protected void serviceStop() throws Exception {
        }
    }

    public MiniNNProxy(Configuration conf, String mountTable, MiniDFSCluster[] clusters) throws Exception {
        super(conf);
        this.mountTable = mountTable;
        this.clusters = clusters;
        this.mounts = new MockedMountsManager();
        this.start();


        UserGroupInformation curr = UserGroupInformation.getCurrentUser();
        clientProtocol = NameNodeProxies.createNonHAProxy(conf,
                getRpcAddress(), ClientProtocol.class,
                curr, false).getProxy();
        dfs = new DFSClient(URI.create("hdfs://127.0.0.1:" + getRpcAddress().getPort()), conf);
        fs = FileSystem.newInstance(URI.create("hdfs://127.0.0.1:" + getRpcAddress().getPort()), conf, curr.getUserName());
    }

    public ClientProtocol getClientProtocol() {
        return clientProtocol;
    }

    public DFSClient getDfs() {
        return dfs;
    }

    public FileSystem getFs() {
        return fs;
    }
}
