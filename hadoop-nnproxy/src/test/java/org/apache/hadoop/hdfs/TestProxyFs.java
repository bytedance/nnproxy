package org.apache.hadoop.hdfs;

import com.bytedance.hadoop.hdfs.server.MiniNNProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.Random;

import static org.junit.Assert.*;

/** */
public class TestProxyFs {

    private static final Random RAN = new Random();
    private static final Logger LOG = LoggerFactory.getLogger(TestProxyFs.class);

    Configuration conf;
    MiniNNProxy nnProxy;
    ClientProtocol clientProtocol;
    DFSClient dfs;
    FileSystem fs;
    MiniDFSCluster cluster;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
        conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
        conf.setInt("dfs.namenode.fs-limits.min-block-size", 1024);
        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
        cluster.waitActive();
        int port = cluster.getNameNode().getNameNodeAddress().getPort();
        nnProxy = new MiniNNProxy(conf, "hdfs://127.0.0.1:" + port + " /", new MiniDFSCluster[]{cluster});
        clientProtocol = nnProxy.getClientProtocol();
        dfs = nnProxy.getDfs();
        fs = nnProxy.getFs();
    }

    @Test
    public void testDFSClient() throws Exception {
        final long grace = 1000L;
        try {
            final String filepathstring = "/test/LeaseChecker/foo";
            final Path[] filepaths = new Path[4];
            for (int i = 0; i < filepaths.length; i++) {
                filepaths[i] = new Path(filepathstring + i);
            }
            final long millis = Time.now();

            {
                final DistributedFileSystem dfs = cluster.getFileSystem();
                dfs.dfs.getLeaseRenewer().setGraceSleepPeriod(grace);
                assertFalse(dfs.dfs.getLeaseRenewer().isRunning());

                {
                    //create a file
                    final FSDataOutputStream out = dfs.create(filepaths[0]);
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
                    //write something
                    out.writeLong(millis);
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
                    //close
                    out.close();
                    Thread.sleep(grace / 4 * 3);
                    //within grace period
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
                    for (int i = 0; i < 3; i++) {
                        if (dfs.dfs.getLeaseRenewer().isRunning()) {
                            Thread.sleep(grace / 2);
                        }
                    }
                    //passed grace period
                    assertFalse(dfs.dfs.getLeaseRenewer().isRunning());
                }

                {
                    //create file1
                    final FSDataOutputStream out1 = dfs.create(filepaths[1]);
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
                    //create file2
                    final FSDataOutputStream out2 = dfs.create(filepaths[2]);
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());

                    //write something to file1
                    out1.writeLong(millis);
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
                    //close file1
                    out1.close();
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());

                    //write something to file2
                    out2.writeLong(millis);
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
                    //close file2
                    out2.close();
                    Thread.sleep(grace / 4 * 3);
                    //within grace period
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
                }

                {
                    //create file3
                    final FSDataOutputStream out3 = dfs.create(filepaths[3]);
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
                    Thread.sleep(grace / 4 * 3);
                    //passed previous grace period, should still running
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
                    //write something to file3
                    out3.writeLong(millis);
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
                    //close file3
                    out3.close();
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
                    Thread.sleep(grace / 4 * 3);
                    //within grace period
                    assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
                    for (int i = 0; i < 3; i++) {
                        if (dfs.dfs.getLeaseRenewer().isRunning()) {
                            Thread.sleep(grace / 2);
                        }
                    }
                    //passed grace period
                    assertFalse(dfs.dfs.getLeaseRenewer().isRunning());
                }

                dfs.close();
            }

            {
                // Check to see if opening a non-existent file triggers a FNF
                FileSystem fs = cluster.getFileSystem();
                Path dir = new Path("/wrwelkj");
                assertFalse("File should not exist for test.", fs.exists(dir));

                try {
                    FSDataInputStream in = fs.open(dir);
                    try {
                        in.close();
                        fs.close();
                    } finally {
                        assertTrue("Did not get a FileNotFoundException for non-existing" +
                                " file.", false);
                    }
                } catch (FileNotFoundException fnf) {
                    // This is the proper exception to catch; move on.
                }

            }

            {
                final DistributedFileSystem dfs = cluster.getFileSystem();
                assertFalse(dfs.dfs.getLeaseRenewer().isRunning());

                //open and check the file
                FSDataInputStream in = dfs.open(filepaths[0]);
                assertFalse(dfs.dfs.getLeaseRenewer().isRunning());
                assertEquals(millis, in.readLong());
                assertFalse(dfs.dfs.getLeaseRenewer().isRunning());
                in.close();
                assertFalse(dfs.dfs.getLeaseRenewer().isRunning());
                dfs.close();
            }

            { // test accessing DFS with ip address. should work with any hostname
                // alias or ip address that points to the interface that NameNode
                // is listening on. In this case, it is localhost.
                String uri = "hdfs://127.0.0.1:" + cluster.getNameNodePort() +
                        "/test/ipAddress/file";
                Path path = new Path(uri);
                FileSystem fs = FileSystem.get(path.toUri(), conf);
                FSDataOutputStream out = fs.create(path);
                byte[] buf = new byte[1024];
                out.write(buf);
                out.close();

                FSDataInputStream in = fs.open(path);
                in.readFully(buf);
                in.close();
                fs.close();
            }
        } finally {
            if (cluster != null) {
                cluster.shutdown();
            }
        }
    }

    @Test
    public void testFileChecksum() throws Exception {
        final long seed = RAN.nextLong();
        System.out.println("seed=" + seed);
        RAN.setSeed(seed);

        final FileSystem hdfs = fs;

        final UserGroupInformation current = UserGroupInformation.getCurrentUser();
        final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
                current.getShortUserName() + "x", new String[]{"user"});

        try {
            hdfs.getFileChecksum(new Path(
                    "/test/TestNonExistingFile"));
            fail("Expecting FileNotFoundException");
        } catch (FileNotFoundException e) {
            assertTrue("Not throwing the intended exception message", e.getMessage()
                    .contains("File does not exist: /test/TestNonExistingFile"));
        }

        try {
            Path path = new Path("/test/TestExistingDir/");
            hdfs.mkdirs(path);
            hdfs.getFileChecksum(path);
            fail("Expecting FileNotFoundException");
        } catch (FileNotFoundException e) {
            assertTrue("Not throwing the intended exception message", e.getMessage()
                    .contains("Path is not a file: /test/TestExistingDir"));
        }

        final Path dir = new Path("/filechecksum");
        final int block_size = 1024;
        final int buffer_size = conf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096);
        conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);

        //try different number of blocks
        for (int n = 0; n < 5; n++) {
            //generate random data
            final byte[] data = new byte[RAN.nextInt(block_size / 2 - 1) + n * block_size + 1];
            RAN.nextBytes(data);
            System.out.println("data.length=" + data.length);

            //write data to a file
            final Path foo = new Path(dir, "foo" + n);
            {
                final FSDataOutputStream out = hdfs.create(foo, false, buffer_size,
                        (short) 2, block_size);
                out.write(data);
                out.close();
            }

            //compute checksum
            final FileChecksum hdfsfoocs = hdfs.getFileChecksum(foo);
            System.out.println("hdfsfoocs=" + hdfsfoocs);

            //create a zero byte file
            final Path zeroByteFile = new Path(dir, "zeroByteFile" + n);
            {
                final FSDataOutputStream out = hdfs.create(zeroByteFile, false, buffer_size,
                        (short) 2, block_size);
                out.close();
            }

            // verify the magic val for zero byte files
            {
                final FileChecksum zeroChecksum = hdfs.getFileChecksum(zeroByteFile);
                assertEquals(zeroChecksum.toString(),
                        "MD5-of-0MD5-of-0CRC32:70bc8f4b72a86921468bf8e8441dce51");
            }

            //write another file
            final Path bar = new Path(dir, "bar" + n);
            {
                final FSDataOutputStream out = hdfs.create(bar, false, buffer_size,
                        (short) 2, block_size);
                out.write(data);
                out.close();
            }

            { //verify checksum
                final FileChecksum barcs = hdfs.getFileChecksum(bar);
                final int barhashcode = barcs.hashCode();
                assertEquals(hdfsfoocs.hashCode(), barhashcode);
                assertEquals(hdfsfoocs, barcs);

            }

            hdfs.setPermission(dir, new FsPermission((short) 0));
            hdfs.setPermission(dir, new FsPermission((short) 0777));
        }
        cluster.shutdown();
    }


}
