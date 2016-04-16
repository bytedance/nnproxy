package com.bytedance.hadoop.hdfs.tools;

import com.bytedance.hadoop.hdfs.server.mount.MountsManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class DumpMount implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(DumpMount.class);

    Configuration conf;

    public static void main(String[] args) throws Exception {
        DumpMount main = new DumpMount();
        System.exit(ToolRunner.run(new HdfsConfiguration(), main, args));
    }

    @Override
    public int run(String[] args) throws Exception {
        MountsManager mountsManager = new MountsManager();
        mountsManager.init(conf);
        mountsManager.start();
        mountsManager.waitUntilInstalled();
        System.out.println(mountsManager.dump());
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
