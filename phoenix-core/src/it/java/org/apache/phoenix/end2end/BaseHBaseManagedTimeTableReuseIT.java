package org.apache.phoenix.end2end;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Base class for tests that let HBase set timestamps.
 * We need to separate these from tests that rely on clients
 * to set timestamps, because we create/destroy the Phoenix tables
 * between tests and only allow a table time stamp to increase.
 * Without this separation table deletion/creation would fail.
 *
 * All tests extending this class use the mini cluster that is
 * shared by all classes extending this class
 *
 * Remember to use BaseTest.generateRandomString() to generate table
 * names for your tests otherwise there might be naming collisions between
 * other tests.
 * {@link BaseClientManagedTimeIT}.
 *
 * @since 0.1
 */
@NotThreadSafe
@Category(HBaseManagedTimeTest.class)
public class BaseHBaseManagedTimeTableReuseIT extends BaseTest {

  protected static Configuration getTestClusterConfig() {
    // don't want callers to modify config.
    return new Configuration(config);
  }

  @BeforeClass
  public static void doSetup() throws Exception {
    setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);
  }

  @AfterClass
  public static void doTeardown() throws Exception {
    // no teardown since we are creating unique table names
  }

  @After
  public void cleanUpAfterTest() throws Exception {
    // no cleanup since we are using unique table names
  }

}
