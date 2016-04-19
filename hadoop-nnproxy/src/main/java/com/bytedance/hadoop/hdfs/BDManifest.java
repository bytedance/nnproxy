package com.bytedance.hadoop.hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** */
public class BDManifest {

    private static final Logger LOG = LoggerFactory.getLogger(BDManifest.class);

    static final Properties properties;

    static {
        InputStream inputStream = BDManifest.class.getResourceAsStream("/bdversion.properties");
        properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (Exception e) {
            LOG.warn("No version information available", e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }

    public static String getBuildNumber() {
        return properties.getProperty("gitrev");
    }
}
