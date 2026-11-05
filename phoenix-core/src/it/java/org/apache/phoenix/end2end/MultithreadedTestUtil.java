package org.apache.phoenix.end2end;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Based on HBase's testing framework.
 * Source: hbase-rel-1.1.5/hbase-server/src/test/java/org/apache/hadoop/hbase/MultithreadedTestUtil.java
 */
public class MultithreadedTestUtil {
    private static final Log LOG = LogFactory.getLog(MultithreadedTestUtil.class);

    public static class TestContext {
        private final Configuration conf;
        private Throwable err = null;
        private boolean stopped = false;
        private int threadDoneCount = 0;
        private Set<TestThread> testThreads = new HashSet<TestThread>();

        public TestContext(Configuration configuration) {
            this.conf = configuration;
        }

        protected Configuration getConf() {
            return conf;
        }

        public synchronized boolean shouldRun()  {
            return !stopped && err == null;
        }

        public void addThread(TestThread t) {
            testThreads.add(t);
        }

        public void startThreads() {
            for (TestThread t : testThreads) {
                t.start();
            }
        }

        public void waitFor(long millis) throws Exception {
            long endTime = System.currentTimeMillis() + millis;
            while (!stopped) {
                long left = endTime - System.currentTimeMillis();
                if (left <= 0) break;
                synchronized (this) {
                    checkException();
                    wait(left);
                }
            }
        }
        private synchronized void checkException() throws Exception {
            if (err != null) {
                throw new RuntimeException("Deferred", err);
            }
        }

        public synchronized void threadFailed(Throwable t) {
            if (err == null) err = t;
            LOG.error("Failed!", err);
            notify();
        }

        public synchronized void threadDone() {
            threadDoneCount++;
        }

        public boolean removeAllThreads(){
            if(shouldRun()){
                return false;
            } else {
                testThreads.clear();
                return true;
            }
        }

        public void setStopFlag(boolean s) throws Exception {
            synchronized (this) {
                stopped = s;
            }
        }

        public void stop() throws Exception {
            synchronized (this) {
                stopped = true;
            }
            for (TestThread t : testThreads) {
                t.join();
            }
            checkException();
        }
    }

    /**
     * A thread that can be added to a test context, and properly
     * passes exceptions through.
     */
    public static abstract class TestThread extends Thread {
        protected final TestContext ctx;
        protected boolean stopped;

        public TestThread(TestContext ctx) {
            this.ctx = ctx;
        }

        public void run() {
            try {
                doWork();
            } catch (Throwable t) {
                ctx.threadFailed(t);
            }
            ctx.threadDone();
        }

        public abstract void doWork() throws Exception;

        protected void stopTestThread() {
            this.stopped = true;
        }
    }

    /**
     * A test thread that performs a repeating operation.
     */
    public static abstract class RepeatingTestThread extends TestThread {
        public RepeatingTestThread(TestContext ctx) {
            super(ctx);
        }

        public final void doWork() throws Exception {
            while (ctx.shouldRun() && !stopped) {
                doAnAction();
            }
        }

        public abstract void doAnAction() throws Exception;
    }

    /**
     * Verify that no assertions have failed inside a future.
     * Used for unit tests that spawn threads. E.g.,
     * <p>
     * <code>
     *   List<Future<Void>> results = Lists.newArrayList();
     *   Future<Void> f = executor.submit(new Callable<Void> {
     *     public Void call() {
     *       assertTrue(someMethod());
     *     }
     *   });
     *   results.add(f);
     *   assertOnFutures(results);
     * </code>
     * @param threadResults A list of futures
     * @param <T>
     * @throws InterruptedException If interrupted when waiting for a result
     *                              from one of the futures
     * @throws ExecutionException If an exception other than AssertionError
     *                            occurs inside any of the futures
     */
    public static <T> void assertOnFutures(List<Future<T>> threadResults)
            throws InterruptedException, ExecutionException {
        for (Future<T> threadResult : threadResults) {
            try {
                threadResult.get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof AssertionError) {
                    throw (AssertionError) e.getCause();
                }
                throw e;
            }
        }
    }
}
