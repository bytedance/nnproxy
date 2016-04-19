# NameNodeProxy

*Another HDFS Federation solution from [bytedance](http://www.bytedance.com/).*

### Features
 * NameNode RPC proxy instead of ViewFS, no client-side changes.
 * Multiple language client (i.e. snakebite) support.
 * Stateless. Easy to scale to multiple instances.
 * High throughput with little latency introduced.
 * Request throttle. Single NameNode malfunction will not affect globally.


Compared with ViewFS and WebHDFS:
<table>
<tr><th></th><th>NNProxy</th><th>ViewFS</th><th>WebHDFS</th></tr>
<tr><td>Multiple language support</td><td>Yes</td><td>No</td><td>Yes</td></tr>
<tr><td>Unified APIs provided</td><td>Yes</td><td>Yes</td><td>No</td></tr>
<tr><td>Mount table stored in</td><td>Zookeeper</td><td>Configuration</td><td>Configuration</td></tr>
<tr><td></td><td colspan="3">* Client-side configurations are not usually easy to change</td></tr>
<tr><td>Client-side library</td><td>No</td><td>Heavy</td><td>Light</td></tr>
<tr><td>Call latency introduced</td><td>Medium</td><td>No latency</td><td>High</td></tr>
<tr><td>Centralized cache support</td><td>Yes</td><td>Yes</td><td>No</td></tr>
</table>

### Benchmark
Benchmark result with NameNode Benchmark 0.4:
<table>
<tr><th></th><th>Direct TPS</th><th>NNProxy TPS (3 proxy instances)</th></tr>
<tr><td>create_write @ 16maps</td><td>263,294</td><td>247,799</td></tr>
<tr><td>create_write @ 128maps</td><td>3,008,604</td><td>2,563,465</td></tr>
<tr><td>rename @ 128maps</td><td>474,671</td><td>471,240</td></tr>
</table>
(numOfFiles 512 each map, NameNode CPU: Xeon E5-2630 v2 * 2, network RTT 0.1ms)

### Build

**IMPORTANT: NNProxy works only with patched NameNode. See `/hadoop-patches` in git repository. These patches add proxy support to `hadoop-common` library. Each of them must be applied before compiling.**

Build with maven:

```
$ cd nnproxy/hadoop-nnproxy
$ mvn package -DskipTests
```
Built jar are shaded with necessary libraries, which can run with `hadoop jar`.

### Deploy

Organize working directory as follows:
```
bin/
bin/nnproxy
lib/
lib/hadoop-nnproxy-0.1.0.jar
```
Then run `bin/nnproxy` with configuration arguments to start proxy server.
Note that before starting proxy server, a mount table must be deployed first.

For example:
```
# Setup zk environment, for simplicity, not necessary
export NNPROXY_ZK_QUORUM="127.0.0.1:2181"
export NNPROXY_MOUNT_TABLE_ZKPATH="/hadoop/hdfs/mounts"

export NNPROXY_OPTS="-Ddfs.nnproxy.mount-table.zk.quorum=$NNPROXY_ZK_QUORUM -Ddfs.nnproxy.mount-table.zk.path=$NNPROXY_MOUNT_TABLE_ZKPATH"

# Deploy mount table, example above mounts / to hdfs://toutiaonn0 and /data 
# to hdfs://toutiaonn1. In which case, accessing /data/recommend would be 
# transparently redirected to hdfs://toutiaonn1/data/recommend.
# In this example, toutiaonn0 and toutiaonn1 are fs names configured with 
# auto failover HA. Using ip:port directly (non-HA setup) are also fesible.
echo 'hdfs://toutiaonn0 /' > mounts
echo 'hdfs://toutiaonn1 /data' >> mounts
cat mounts | bin/nnproxy load $NNPROXY_OPTS

# Run proxy (remember these args)
bin/nnproxy proxy $NNPROXY_OPTS
```
Look into class `com.bytedance.hadoop.hdfs.ProxyConfig` for more configurations available.

Proxy server metrics are also available with hadoop-metrics2.

### Mount table management
Once proxy server is started, it watches mount table node on ZooKeeper for further changes.

Run `bin/nnproxy dump $NNPROXY_OPTS` to dump current mount table to stdout.

Run `bin/nnproxy load $NNPROXY_OPTS` to load mount table from stdin.

One can also alter data stored in mount table ZK node directly (or possibly build APIs or UIs). 
All these changes take effect immediately.

Mount table are delimited text with 3 columns: `targetFS`, `path` and `attributes`, in that order.
For example:
```
hdfs://toutiaonn0   /        none
hdfs://toutiaonn1   /data    none
```
Each row advises proxy to redirect `path` and its children to specified `targetFS`. `attributes` are currently reserved and is of no use.


### Future work
* Support `readonly` attribute in mounts
* More kind of operations through proxy

### License
Copyright 2016 Bytedance Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
