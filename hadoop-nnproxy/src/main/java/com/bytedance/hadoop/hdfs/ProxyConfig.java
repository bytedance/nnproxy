package com.bytedance.hadoop.hdfs;

/** */
public class ProxyConfig {
    public static final String MOUNT_TABLE_ZK_QUORUM = "dfs.nnproxy.mount-table.zk.quorum";
    public static final String MOUNT_TABLE_ZK_PATH = "dfs.nnproxy.mount-table.zk.path";
    public static final String MOUNT_TABLE_ZK_SESSION_TIMEOUT = "dfs.nnproxy.mount-table.zk.session.timeout";
    public static final int MOUNT_TABLE_ZK_SESSION_TIMEOUT_DEFAULT = 30000;
    public static final String MOUNT_TABLE_ZK_CONNECTION_TIMEOUT = "dfs.nnproxy.mount-table.zk.connection.timeout";
    public static final int MOUNT_TABLE_ZK_CONNECTION_TIMEOUT_DEFAULT = 30000;
    public static final String MOUNT_TABLE_ZK_MAX_RETRIES = "dfs.nnproxy.mount-table.zk.max.retries";
    public static final int MOUNT_TABLE_ZK_MAX_RETRIES_DEFAULT = 10;
    public static final String MOUNT_TABLE_ZK_RETRY_BASE_SLEEP = "dfs.nnproxy.mount-table.zk.retry.base-sleep";
    public static final int MOUNT_TABLE_ZK_RETRY_BASE_SLEEP_DEFAULT = 1000;
    public static final String PROXY_HANDLER_COUNT = "dfs.nnproxy.handler.count";
    public static final int PROXY_HANDLER_COUNT_DEFAULT = 2048;
    public static final String USER_PROXY_EXPIRE_MS = "dfs.nnproxy.user-proxy.expire.ms";
    public static final long USER_PROXY_EXPIRE_MS_DEFAULT = 3 * 3600 * 1000L;
    public static final String RPC_PORT = "dfs.nnproxy.rpc.port";
    public static final int RPC_PORT_DEFAULT = 65212;
    public static final String MAX_CONCURRENT_REQUEST_PER_FS = "dfs.nnproxy.max.concurrent.request-per-fs";
    public static final long MAX_CONCURRENT_REQUEST_PER_FS_DEFAULT = 1637;
    public static final String CACHE_REGISTRY_RELOAD_INTERVAL_MS = "dfs.nnproxy.cache.registry.reload-interval-ms";
    public static final long CACHE_REGISTRY_RELOAD_INTERVAL_MS_DEFAULT = 300 * 1000L;
    public static final String SUPERUSER = "dfs.nnproxy.superuser";
    public static final String SUPERUSER_DEFAULT = System.getProperty("user.name");
}
