package org.apache.phoenix;

import org.apache.hadoop.hbase.TableName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class PhoenixTestTableName extends TestWatcher {

    private String tableName;

    @Override
    protected void starting(Description description) {
        tableName = TableName.valueOf(cleanUpTestName(description.getClassName())).getNameAsString();
    }

    /**
     * Helper to handle parameterized method names. Unlike regular test methods, parameterized method
     * names look like 'foo[x]'. This is problematic for tests that use this name for HBase tables.
     * This helper strips out the parameter suffixes.
     * @return current test method name with out parameterized suffixes.
     */
    public static String cleanUpTestName(String className) {
        int lastIndex = className.lastIndexOf('.');
        if (lastIndex == -1) {
            return className;
        }

        return className.substring(lastIndex+1).toUpperCase();
    }

    public String getTableName() {
        return tableName;
    }
}

