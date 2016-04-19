package com.bytedance.hadoop.hdfs.server.proxy;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

/** */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@Metrics(name = "ProxyActivity", about = "NameNode proxy metrics", context = "nnproxy")
public class ProxyMetrics {
    final MetricsRegistry registry = new MetricsRegistry("nnproxy");

    private static final Logger LOG = LoggerFactory.getLogger(ProxyMetrics.class);

    @Metric
    public MutableCounterLong throttledOps;
    @Metric
    public MutableCounterLong successOps;
    @Metric
    public MutableCounterLong failedOps;

    JvmMetrics jvmMetrics = null;

    ProxyMetrics(String processName, String sessionId, final JvmMetrics jvmMetrics) {
        this.jvmMetrics = jvmMetrics;
        registry.tag(ProcessName, processName).tag(SessionId, sessionId);
    }

    public static ProxyMetrics create(Configuration conf) {
        String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
        String processName = "NNPROXY";
        MetricsSystem ms = DefaultMetricsSystem.instance();
        JvmMetrics jm = JvmMetrics.create(processName, sessionId, ms);

        return ms.register(new ProxyMetrics(processName, sessionId, jm));
    }

}
