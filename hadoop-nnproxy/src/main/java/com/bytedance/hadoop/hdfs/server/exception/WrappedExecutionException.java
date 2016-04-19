package com.bytedance.hadoop.hdfs.server.exception;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An ExecutionException wrapped as unchecked, this is for internal exception handling in proxy
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class WrappedExecutionException extends RuntimeException {
    public WrappedExecutionException() {
    }

    public WrappedExecutionException(String message) {
        super(message);
    }

    public WrappedExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public WrappedExecutionException(Throwable cause) {
        super(cause);
    }
}
