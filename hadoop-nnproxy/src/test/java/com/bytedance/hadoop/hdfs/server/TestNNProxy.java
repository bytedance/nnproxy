package com.bytedance.hadoop.hdfs.server;

import com.bytedance.hadoop.hdfs.ProxyConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** */
public class TestNNProxy {

    private static final Logger LOG = LoggerFactory.getLogger(TestNNProxy.class);

    Configuration conf;
    MiniNNProxy nnProxy;
    ClientProtocol clientProtocol;
    DFSClient dfs;
    FileSystem fs;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        conf.setInt(ProxyConfig.PROXY_HANDLER_COUNT, 12);
        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
        cluster.waitActive();
        int port = cluster.getNameNode().getNameNodeAddress().getPort();
        nnProxy = new MiniNNProxy(conf, "hdfs://127.0.0.1:" + port + " /", new MiniDFSCluster[]{cluster});
        clientProtocol = nnProxy.getClientProtocol();
        dfs = nnProxy.getDfs();
        fs = nnProxy.getFs();
    }

    @Test
    public void testGetListing() throws IOException {
        fs.create(new Path("/test"), (short) 3).close();
        assertEquals(1, fs.listStatus(new Path("/")).length);
        assertEquals("test", fs.listStatus(new Path("/"))[0].getPath().getName());
    }


}
