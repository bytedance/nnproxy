package com.bytedance.hadoop.hdfs.server.quota;

import com.bytedance.hadoop.hdfs.server.NNProxy;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.StandbyException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class ThrottleInvocationHandler implements InvocationHandler {

    final Object underlying;
    final Function<Method, AtomicLong> opCounter;
    final long threshold;

    public ThrottleInvocationHandler(Object underlying, Function<Method, AtomicLong> opCounter, long threshold) {
        this.underlying = underlying;
        this.opCounter = opCounter;
        this.threshold = threshold;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        AtomicLong counter = opCounter.apply(method);
        Preconditions.checkState(counter != null);
        long current = counter.getAndIncrement();
        try {
            if (current > threshold) {
                NNProxy.proxyMetrics.throttledOps.incr();
                throw new StandbyException("Too many requests (" + current + "/" + threshold + "), try later");
            }
            Object ret = method.invoke(underlying, args);
            NNProxy.proxyMetrics.successOps.incr();
            return ret;
        } catch (InvocationTargetException e) {
            NNProxy.proxyMetrics.failedOps.incr();
            throw e.getCause();
        } finally {
            counter.decrementAndGet();
        }
    }
}
