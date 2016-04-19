package com.bytedance.hadoop.hdfs.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class ProxyMain implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyMain.class);

    Configuration conf;

    public static void main(String[] args) throws Exception {
        ProxyMain main = new ProxyMain();
        System.exit(ToolRunner.run(new HdfsConfiguration(), main, args));
    }

    @Override
    public int run(String[] args) throws Exception {
        NNProxy nnProxy = new NNProxy(conf);
        nnProxy.start();
        nnProxy.join();
        LOG.info("NNProxy halted");
        return 0;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
