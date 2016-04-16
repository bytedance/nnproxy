package com.bytedance.hadoop.hdfs.tools;

import com.bytedance.hadoop.hdfs.server.mount.MountsManager;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class LoadMount implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(LoadMount.class);

    Configuration conf;

    public static void main(String[] args) throws Exception {
        LoadMount main = new LoadMount();
        System.exit(ToolRunner.run(new HdfsConfiguration(), main, args));
    }

    @Override
    public int run(String[] args) throws Exception {
        String mounts = IOUtils.toString(System.in);
        MountsManager mountsManager = new MountsManager();
        mountsManager.init(conf);
        mountsManager.start();
        mountsManager.load(mounts);
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
