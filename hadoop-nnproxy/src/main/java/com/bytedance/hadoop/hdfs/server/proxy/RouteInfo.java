package com.bytedance.hadoop.hdfs.server.proxy;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class RouteInfo {

    final ClientProtocol upstream;
    final String realPath;
    final String fs;

    public RouteInfo(ClientProtocol upstream, String realPath, String fs) {
        this.upstream = upstream;
        this.realPath = realPath;
        this.fs = fs;
    }
}
