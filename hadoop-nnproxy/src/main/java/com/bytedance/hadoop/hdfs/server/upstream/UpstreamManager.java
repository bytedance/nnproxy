package com.bytedance.hadoop.hdfs.server.upstream;

import com.bytedance.hadoop.hdfs.ProxyConfig;
import com.bytedance.hadoop.hdfs.server.NNProxy;
import com.bytedance.hadoop.hdfs.server.quota.ThrottleInvocationHandler;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class UpstreamManager {

    private static final Logger LOG = LoggerFactory.getLogger(UpstreamManager.class);

    final NNProxy nnProxy;
    final Configuration conf;
    final LoadingCache<UpstreamTicket, Upstream> upstreamCache;
    final long maxConrruentRequestPerFs;
    final Map<String, AtomicLong> fsRequests;

    public static class Upstream {
        public final ClientProtocol protocol;
        public final NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyAndInfo;
        public final NameNodeProxies.ProxyAndInfo<NamenodeProtocol> nnProxyAndInfo;
        public volatile int fsIndex;

        public Upstream(ClientProtocol protocol,
                        NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyAndInfo,
                        NameNodeProxies.ProxyAndInfo<NamenodeProtocol> nnProxyAndInfo) {
            this.protocol = protocol;
            this.proxyAndInfo = proxyAndInfo;
            this.nnProxyAndInfo = nnProxyAndInfo;
        }

        public int getFsIndex() {
            return fsIndex;
        }

        public void setFsIndex(int fsIndex) {
            this.fsIndex = fsIndex;
        }
    }

    public static class UpstreamTicket {
        public final String user;
        public final String fs;

        public UpstreamTicket(String user, String fs) {
            this.user = user;
            this.fs = fs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof UpstreamTicket)) return false;

            UpstreamTicket that = (UpstreamTicket) o;

            if (user != null ? !user.equals(that.user) : that.user != null) return false;
            return !(fs != null ? !fs.equals(that.fs) : that.fs != null);
        }

        @Override
        public int hashCode() {
            int result = user != null ? user.hashCode() : 0;
            result = 31 * result + (fs != null ? fs.hashCode() : 0);
            return result;
        }
    }

    public UpstreamManager(NNProxy nnProxy, Configuration conf) {
        this.nnProxy = nnProxy;
        this.conf = conf;
        final long cacheExpire =
                conf.getLong(ProxyConfig.USER_PROXY_EXPIRE_MS, ProxyConfig.USER_PROXY_EXPIRE_MS_DEFAULT);
        maxConrruentRequestPerFs =
                conf.getLong(ProxyConfig.MAX_CONCURRENT_REQUEST_PER_FS, ProxyConfig.MAX_CONCURRENT_REQUEST_PER_FS_DEFAULT);
        this.upstreamCache = CacheBuilder.<UpstreamTicket, Upstream>newBuilder()
                .expireAfterAccess(cacheExpire, TimeUnit.MILLISECONDS)
                .build(new CacheLoader<UpstreamTicket, Upstream>() {
                    @Override
                    public Upstream load(UpstreamTicket ticket) throws Exception {
                        return makeUpstream(ticket);
                    }
                });
        this.fsRequests = new ConcurrentHashMap<>();
    }

    synchronized <T> T wrapWithThrottle(final String key, final T underlying, final Class<T> xface) {
        if (!fsRequests.containsKey(key)) {
            fsRequests.put(key, new AtomicLong(0L));
        }
        final Function<Method, AtomicLong> counterGetter = new Function<Method, AtomicLong>() {
            @Override
            public AtomicLong apply(Method method) {
                return fsRequests.get(key);
            }
        };
        ThrottleInvocationHandler throttleHandler = new ThrottleInvocationHandler(underlying, counterGetter, maxConrruentRequestPerFs);
        return (T) Proxy.newProxyInstance(this.getClass().getClassLoader(),
                new Class[]{xface}, throttleHandler);
    }

    synchronized Upstream makeUpstream(UpstreamTicket ticket) throws IOException {
        if (ticket.user != null) {
            UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(ticket.user,
                    SaslRpcServer.AuthMethod.SIMPLE));
        } else {
            UserGroupInformation.setLoginUser(null);
        }
        URI fsUri = URI.create(ticket.fs);
        NameNodeProxies.ProxyAndInfo proxyAndInfo = NameNodeProxies.createProxy(conf, fsUri, ClientProtocol.class);
        NameNodeProxies.ProxyAndInfo nnProxyAndInfo = NameNodeProxies.createProxy(conf, fsUri, NamenodeProtocol.class);
        LOG.info("New upstream: " + ticket.user + "@" + ticket.fs);
        ClientProtocol clientProtocol = (ClientProtocol) proxyAndInfo.getProxy();
        return new Upstream(wrapWithThrottle(ticket.fs, clientProtocol, ClientProtocol.class), proxyAndInfo, nnProxyAndInfo);
    }

    public Upstream getUpstream(String user, String fs) throws ExecutionException {
        return upstreamCache.get(new UpstreamTicket(user, fs));
    }
}
