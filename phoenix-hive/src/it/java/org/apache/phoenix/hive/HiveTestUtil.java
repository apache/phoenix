/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive;

import com.google.common.collect.ImmutableList;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.common.io.DigestPrintStream;
import org.apache.hadoop.hive.common.io.SortAndDigestPrintStream;
import org.apache.hadoop.hive.common.io.SortPrintStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.ZooKeeperHiveLockManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.Shell;
import org.apache.hive.common.util.StreamPrinter;
import org.apache.tools.ant.BuildException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * HiveTestUtil cloned from Hive QTestUtil. Can be outdated and may require update once a problem
 * found.
 */
public class HiveTestUtil {

    public static final String UTF_8 = "UTF-8";
    private static final Log LOG = LogFactory.getLog("HiveTestUtil");
    private static final String QTEST_LEAVE_FILES = "QTEST_LEAVE_FILES";
    public static final String DEFAULT_DATABASE_NAME = "default";

    private String testWarehouse;
    private final String testFiles;
    protected final String outDir;
    protected final String logDir;
    private final TreeMap<String, String> qMap;
    private final Set<String> qSkipSet;
    private final Set<String> qSortSet;
    private final Set<String> qSortQuerySet;
    private final Set<String> qHashQuerySet;
    private final Set<String> qSortNHashQuerySet;
    private final Set<String> qJavaVersionSpecificOutput;
    private static final String SORT_SUFFIX = ".sorted";
    private static MiniClusterType clusterType = MiniClusterType.none;
    private ParseDriver pd;
    protected Hive db;
    protected HiveConf conf;
    private BaseSemanticAnalyzer sem;
    protected final boolean overWrite;
    private CliDriver cliDriver;
    private HadoopShims.MiniMrShim mr = null;
    private HadoopShims.MiniDFSShim dfs = null;
    private String hadoopVer = null;
    private HiveTestSetup setup = null;
    private boolean isSessionStateStarted = false;
    private static final String javaVersion = getJavaVersion();

    private String initScript = "";
    private String cleanupScript = "";

    public HiveConf getConf() {
        return conf;
    }

    public boolean deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        return (path.delete());
    }

    public void copyDirectoryToLocal(Path src, Path dest) throws Exception {

        FileSystem srcFs = src.getFileSystem(conf);
        FileSystem destFs = dest.getFileSystem(conf);
        if (srcFs.exists(src)) {
            FileStatus[] files = srcFs.listStatus(src);
            for (FileStatus file : files) {
                String name = file.getPath().getName();
                Path dfs_path = file.getPath();
                Path local_path = new Path(dest, name);

                if (file.isDir()) {
                    if (!destFs.exists(local_path)) {
                        destFs.mkdirs(local_path);
                    }
                    copyDirectoryToLocal(dfs_path, local_path);
                } else {
                    srcFs.copyToLocalFile(dfs_path, local_path);
                }
            }
        }
    }

    static Pattern mapTok = Pattern.compile("(\\.?)(.*)_map_(.*)");
    static Pattern reduceTok = Pattern.compile("(.*)(reduce_[^\\.]*)((\\..*)?)");

    public void normalizeNames(File path) throws Exception {
        if (path.isDirectory()) {
            File[] files = path.listFiles();
            for (File file : files) {
                normalizeNames(file);
            }
        } else {
            Matcher m = reduceTok.matcher(path.getName());
            if (m.matches()) {
                String name = m.group(1) + "reduce" + m.group(3);
                path.renameTo(new File(path.getParent(), name));
            } else {
                m = mapTok.matcher(path.getName());
                if (m.matches()) {
                    String name = m.group(1) + "map_" + m.group(3);
                    path.renameTo(new File(path.getParent(), name));
                }
            }
        }
    }

    public String getOutputDirectory() {
        return outDir;
    }

    public String getLogDirectory() {
        return logDir;
    }

    private String getHadoopMainVersion(String input) {
        if (input == null) {
            return null;
        }
        Pattern p = Pattern.compile("^(\\d+\\.\\d+).*");
        Matcher m = p.matcher(input);
        if (m.matches()) {
            return m.group(1);
        }
        return null;
    }

    public void initConf() throws Exception {
        // Plug verifying metastore in for testing.
        conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
                "org.apache.hadoop.hive.metastore.VerifyingObjectStore");

        if (mr != null) {
            assert dfs != null;

            mr.setupConfiguration(conf);

            // set fs.default.name to the uri of mini-dfs
            String dfsUriString = WindowsPathUtil.getHdfsUriString(dfs.getFileSystem().getUri()
                    .toString());
            conf.setVar(HiveConf.ConfVars.HADOOPFS, dfsUriString);
            // hive.metastore.warehouse.dir needs to be set relative to the mini-dfs
            conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE,
                    (new Path(dfsUriString,
                            "/build/ql/test/data/warehouse/")).toString());
        }

        // Windows paths should be converted after MiniMrShim.setupConfiguration()
        // since setupConfiguration may overwrite configuration values.
        if (Shell.WINDOWS) {
            WindowsPathUtil.convertPathsFromWindowsToHdfs(conf);
        }
    }

    public enum MiniClusterType {
        mr,
        tez,
        none;

        public static MiniClusterType valueForString(String type) {
            if (type.equals("miniMR")) {
                return mr;
            } else if (type.equals("tez")) {
                return tez;
            } else {
                return none;
            }
        }
    }

    public HiveTestUtil(String outDir, String logDir, MiniClusterType clusterType, String hadoopVer)
            throws Exception {
        this(outDir, logDir, clusterType, null, hadoopVer);
    }

    public HiveTestUtil(String outDir, String logDir, MiniClusterType clusterType, String confDir,
                        String hadoopVer)
            throws Exception {
        this.outDir = outDir;
        this.logDir = logDir;
        if (confDir != null && !confDir.isEmpty()) {
            HiveConf.setHiveSiteLocation(new URL("file://" + new File(confDir).toURI().getPath()
                    + "/hive-site.xml"));
            LOG.info("Setting hive-site: " + HiveConf.getHiveSiteLocation());
        }
        conf = new HiveConf();
        String tmpBaseDir = System.getProperty("test.tmp.dir");
        if (tmpBaseDir == null || tmpBaseDir == "") {
            tmpBaseDir = System.getProperty("java.io.tmpdir");
        }
        String metaStoreURL = "jdbc:derby:" + tmpBaseDir + File.separator + "metastore_dbtest;" +
                "create=true";
        conf.set(ConfVars.METASTORECONNECTURLKEY.varname, metaStoreURL);
        System.setProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, metaStoreURL);

        //set where derby logs
        File derbyLogFile = new File(tmpBaseDir + "/derby.log");
        derbyLogFile.createNewFile();
        System.setProperty("derby.stream.error.file", derbyLogFile.getPath());

        this.hadoopVer = getHadoopMainVersion(hadoopVer);
        qMap = new TreeMap<String, String>();
        qSkipSet = new HashSet<String>();
        qSortSet = new HashSet<String>();
        qSortQuerySet = new HashSet<String>();
        qHashQuerySet = new HashSet<String>();
        qSortNHashQuerySet = new HashSet<String>();
        qJavaVersionSpecificOutput = new HashSet<String>();
        this.clusterType = clusterType;

        // Using randomUUID for dfs cluster
        System.setProperty("test.build.data", "target/test-data/hive-" + UUID.randomUUID().toString
                ());

        HadoopShims shims = ShimLoader.getHadoopShims();
        int numberOfDataNodes = 4;

        if (clusterType != MiniClusterType.none) {
            dfs = shims.getMiniDfs(conf, numberOfDataNodes, true, null);
            FileSystem fs = dfs.getFileSystem();
            String uriString = WindowsPathUtil.getHdfsUriString(fs.getUri().toString());
            if (clusterType == MiniClusterType.tez) {
                mr = shims.getMiniTezCluster(conf, 4, uriString, 1);
            } else {
                mr = shims.getMiniMrCluster(conf, 4, uriString, 1);
            }
        }

        initConf();

        // Use the current directory if it is not specified
        String dataDir = conf.get("test.data.files");
        if (dataDir == null) {
            dataDir = new File(".").getAbsolutePath() + "/data/files";
        }

        testFiles = dataDir;

        // Use the current directory if it is not specified
        String scriptsDir = conf.get("test.data.scripts");
        if (scriptsDir == null) {
            scriptsDir = new File(".").getAbsolutePath() + "/data/scripts";
        }
        if (!initScript.isEmpty()) {
            this.initScript = scriptsDir + "/" + initScript;
        }
        if (!cleanupScript.isEmpty()) {
            this.cleanupScript = scriptsDir + "/" + cleanupScript;
        }

        overWrite = "true".equalsIgnoreCase(System.getProperty("test.output.overwrite"));

        setup = new HiveTestSetup();
        setup.preTest(conf);
        init();
    }

    public void shutdown() throws Exception {
        cleanUp();
        setup.tearDown();
        if (mr != null) {
            mr.shutdown();
            mr = null;
        }
        FileSystem.closeAll();
        if (dfs != null) {
            dfs.shutdown();
            dfs = null;
        }
    }

    public String readEntireFileIntoString(File queryFile) throws IOException {
        InputStreamReader isr = new InputStreamReader(
                new BufferedInputStream(new FileInputStream(queryFile)), HiveTestUtil.UTF_8);
        StringWriter sw = new StringWriter();
        try {
            IOUtils.copy(isr, sw);
        } finally {
            if (isr != null) {
                isr.close();
            }
        }
        return sw.toString();
    }

    public void addFile(String queryFile) throws IOException {
        addFile(queryFile, false);
    }

    public void addFile(String queryFile, boolean partial) throws IOException {
        addFile(new File(queryFile));
    }

    public void addFile(File qf) throws IOException {
        addFile(qf, false);
    }

    public void addFile(File qf, boolean partial) throws IOException {
        String query = readEntireFileIntoString(qf);
        qMap.put(qf.getName(), query);
        if (partial) return;

        if (matches(SORT_BEFORE_DIFF, query)) {
            qSortSet.add(qf.getName());
        } else if (matches(SORT_QUERY_RESULTS, query)) {
            qSortQuerySet.add(qf.getName());
        } else if (matches(HASH_QUERY_RESULTS, query)) {
            qHashQuerySet.add(qf.getName());
        } else if (matches(SORT_AND_HASH_QUERY_RESULTS, query)) {
            qSortNHashQuerySet.add(qf.getName());
        }
    }

    private static final Pattern SORT_BEFORE_DIFF = Pattern.compile("-- SORT_BEFORE_DIFF");
    private static final Pattern SORT_QUERY_RESULTS = Pattern.compile("-- SORT_QUERY_RESULTS");
    private static final Pattern HASH_QUERY_RESULTS = Pattern.compile("-- HASH_QUERY_RESULTS");
    private static final Pattern SORT_AND_HASH_QUERY_RESULTS = Pattern.compile("-- " +
            "SORT_AND_HASH_QUERY_RESULTS");

    private boolean matches(Pattern pattern, String query) {
        Matcher matcher = pattern.matcher(query);
        if (matcher.find()) {
            return true;
        }
        return false;
    }

    /**
     * Get formatted Java version to include minor version, but
     * exclude patch level.
     *
     * @return Java version formatted as major_version.minor_version
     */
    private static String getJavaVersion() {
        String version = System.getProperty("java.version");
        if (version == null) {
            throw new NullPointerException("No java version could be determined " +
                    "from system properties");
        }

        // "java version" system property is formatted
        // major_version.minor_version.patch_level.
        // Find second dot, instead of last dot, to be safe
        int pos = version.indexOf('.');
        pos = version.indexOf('.', pos + 1);
        return version.substring(0, pos);
    }

    /**
     * Clear out any side effects of running tests
     */
    public void clearPostTestEffects() throws Exception {
        setup.postTest(conf);
    }

    /**
     * Clear out any side effects of running tests
     */
    public void clearTablesCreatedDuringTests() throws Exception {
        if (System.getenv(QTEST_LEAVE_FILES) != null) {
            return;
        }

        // Delete any tables other than the source tables
        // and any databases other than the default database.
        for (String dbName : db.getAllDatabases()) {
            SessionState.get().setCurrentDatabase(dbName);
            for (String tblName : db.getAllTables()) {
                if (!DEFAULT_DATABASE_NAME.equals(dbName)) {
                    Table tblObj = db.getTable(tblName);
                    // dropping index table can not be dropped directly. Dropping the base
                    // table will automatically drop all its index table
                    if (tblObj.isIndexTable()) {
                        continue;
                    }
                    db.dropTable(dbName, tblName);
                } else {
                    // this table is defined in srcTables, drop all indexes on it
                    List<Index> indexes = db.getIndexes(dbName, tblName, (short) -1);
                    if (indexes != null && indexes.size() > 0) {
                        for (Index index : indexes) {
                            db.dropIndex(dbName, tblName, index.getIndexName(), true, true);
                        }
                    }
                }
            }
            if (!DEFAULT_DATABASE_NAME.equals(dbName)) {
                // Drop cascade, may need to drop functions
                db.dropDatabase(dbName, true, true, true);
            }
        }

        // delete remaining directories for external tables (can affect stats for following tests)
        try {
            Path p = new Path(testWarehouse);
            FileSystem fileSystem = p.getFileSystem(conf);
            if (fileSystem.exists(p)) {
                for (FileStatus status : fileSystem.listStatus(p)) {
                    if (status.isDir()) {
                        fileSystem.delete(status.getPath(), true);
                    }
                }
            }
        } catch (IllegalArgumentException e) {
            // ignore.. provides invalid url sometimes intentionally
        }
        SessionState.get().setCurrentDatabase(DEFAULT_DATABASE_NAME);

        List<String> roleNames = db.getAllRoleNames();
        for (String roleName : roleNames) {
            if (!"PUBLIC".equalsIgnoreCase(roleName) && !"ADMIN".equalsIgnoreCase(roleName)) {
                db.dropRole(roleName);
            }
        }
    }

    /**
     * Clear out any side effects of running tests
     */
    public void clearTestSideEffects() throws Exception {
        if (System.getenv(QTEST_LEAVE_FILES) != null) {
            return;
        }

        clearTablesCreatedDuringTests();
    }

    public void cleanUp() throws Exception {
        if (!isSessionStateStarted) {
            startSessionState();
        }
        if (System.getenv(QTEST_LEAVE_FILES) != null) {
            return;
        }

        clearTablesCreatedDuringTests();

        SessionState.get().getConf().setBoolean("hive.test.shutdown.phase", true);

        if (cleanupScript != "") {
            String cleanupCommands = readEntireFileIntoString(new File(cleanupScript));
            LOG.info("Cleanup (" + cleanupScript + "):\n" + cleanupCommands);
            if (cliDriver == null) {
                cliDriver = new CliDriver();
            }
            cliDriver.processLine(cleanupCommands);
        }

        SessionState.get().getConf().setBoolean("hive.test.shutdown.phase", false);

        // delete any contents in the warehouse dir
        Path p = new Path(testWarehouse);
        FileSystem fs = p.getFileSystem(conf);

        try {
            FileStatus[] ls = fs.listStatus(p);
            for (int i = 0; (ls != null) && (i < ls.length); i++) {
                fs.delete(ls[i].getPath(), true);
            }
        } catch (FileNotFoundException e) {
            // Best effort
        }

        FunctionRegistry.unregisterTemporaryUDF("test_udaf");
        FunctionRegistry.unregisterTemporaryUDF("test_error");
    }

    public void createSources() throws Exception {
        if (!isSessionStateStarted) {
            startSessionState();
        }
        conf.setBoolean("hive.test.init.phase", true);

        if (cliDriver == null) {
            cliDriver = new CliDriver();
        }
        cliDriver.processLine("set test.data.dir=" + testFiles + ";");

        conf.setBoolean("hive.test.init.phase", false);
    }

    public void init() throws Exception {
        testWarehouse = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
        conf.setBoolVar(HiveConf.ConfVars.SUBMITLOCALTASKVIACHILD, false);
        String execEngine = conf.get("hive.execution.engine");
        conf.set("hive.execution.engine", "mr");
        SessionState.start(conf);
        conf.set("hive.execution.engine", execEngine);
        db = Hive.get(conf);
        pd = new ParseDriver();
        sem = new SemanticAnalyzer(conf);
    }

    public void init(String tname) throws Exception {
        cleanUp();
        createSources();
        cliDriver.processCmd("set hive.cli.print.header=true;");
    }

    public void cliInit(String tname) throws Exception {
        cliInit(tname, true);
    }

    public String cliInit(String tname, boolean recreate) throws Exception {
        if (recreate) {
            cleanUp();
            createSources();
        }

        HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
                "org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator");
        Utilities.clearWorkMap();
        CliSessionState ss = new CliSessionState(conf);
        assert ss != null;
        ss.in = System.in;

        String outFileExtension = getOutFileExtension(tname);
        String stdoutName = null;
        if (outDir != null) {
            File qf = new File(outDir, tname);
            stdoutName = qf.getName().concat(outFileExtension);
        } else {
            stdoutName = tname + outFileExtension;
        }

        File outf = new File(logDir, stdoutName);
        OutputStream fo = new BufferedOutputStream(new FileOutputStream(outf));
        if (qSortQuerySet.contains(tname)) {
            ss.out = new SortPrintStream(fo, "UTF-8");
        } else if (qHashQuerySet.contains(tname)) {
            ss.out = new DigestPrintStream(fo, "UTF-8");
        } else if (qSortNHashQuerySet.contains(tname)) {
            ss.out = new SortAndDigestPrintStream(fo, "UTF-8");
        } else {
            ss.out = new PrintStream(fo, true, "UTF-8");
        }
        ss.err = new CachingPrintStream(fo, true, "UTF-8");
        ss.setIsSilent(true);
        SessionState oldSs = SessionState.get();

        if (oldSs != null && clusterType == MiniClusterType.tez) {
            oldSs.close();
        }

        if (oldSs != null && oldSs.out != null && oldSs.out != System.out) {
            oldSs.out.close();
        }
        SessionState.start(ss);

        cliDriver = new CliDriver();
        cliDriver.processInitFiles(ss);

        return outf.getAbsolutePath();
    }

    private CliSessionState startSessionState()
            throws IOException {

        HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
                "org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator");

        String execEngine = conf.get("hive.execution.engine");
        conf.set("hive.execution.engine", "mr");
        CliSessionState ss = new CliSessionState(conf);
        assert ss != null;
        ss.in = System.in;
        ss.out = System.out;
        ss.err = System.out;

        SessionState oldSs = SessionState.get();
        if (oldSs != null && clusterType == MiniClusterType.tez) {
            oldSs.close();
        }
        if (oldSs != null && oldSs.out != null && oldSs.out != System.out) {
            oldSs.out.close();
        }
        SessionState.start(ss);

        isSessionStateStarted = true;

        conf.set("hive.execution.engine", execEngine);
        return ss;
    }

    public int executeOne(String tname) {
        String q = qMap.get(tname);

        if (q.indexOf(";") == -1) {
            return -1;
        }

        String q1 = q.substring(0, q.indexOf(";") + 1);
        String qrest = q.substring(q.indexOf(";") + 1);
        qMap.put(tname, qrest);

        LOG.info("Executing " + q1);
        return cliDriver.processLine(q1);
    }

    public static final String CRLF = System.getProperty("line.separator");

    public int executeClient(String tname1, String tname2) {
        String commands = getCommands(tname1) + CRLF + getCommands(tname2);
        return cliDriver.processLine(commands);
    }

    public int executeClient(String tname) {
        conf.set("mapreduce.job.name", "test");
        return cliDriver.processLine(getCommands(tname), false);
    }

    private String getCommands(String tname) {
        String commands = qMap.get(tname);
        StringBuilder newCommands = new StringBuilder(commands.length());
        int lastMatchEnd = 0;
        Matcher commentMatcher = Pattern.compile("^--.*$", Pattern.MULTILINE).matcher(commands);
        while (commentMatcher.find()) {
            newCommands.append(commands.substring(lastMatchEnd, commentMatcher.start()));
            newCommands.append(commentMatcher.group().replaceAll("(?<!\\\\);", "\\\\;"));
            lastMatchEnd = commentMatcher.end();
        }
        newCommands.append(commands.substring(lastMatchEnd, commands.length()));
        commands = newCommands.toString();
        return commands;
    }

    public boolean shouldBeSkipped(String tname) {
        return qSkipSet.contains(tname);
    }

    private String getOutFileExtension(String fname) {
        String outFileExtension = ".out";
        if (qJavaVersionSpecificOutput.contains(fname)) {
            outFileExtension = ".java" + javaVersion + ".out";
        }

        return outFileExtension;
    }

    /**
     * Given the current configurations (e.g., hadoop version and execution mode), return
     * the correct file name to compare with the current test run output.
     *
     * @param outDir   The directory where the reference log files are stored.
     * @param testName The test file name (terminated by ".out").
     * @return The file name appended with the configuration values if it exists.
     */
    public String outPath(String outDir, String testName) {
        String ret = (new File(outDir, testName)).getPath();
        // List of configurations. Currently the list consists of hadoop version and execution
        // mode only
        List<String> configs = new ArrayList<String>();
        configs.add(this.hadoopVer);

        Deque<String> stack = new LinkedList<String>();
        StringBuilder sb = new StringBuilder();
        sb.append(testName);
        stack.push(sb.toString());

        // example file names are input1.q.out_0.20.0_minimr or input2.q.out_0.17
        for (String s : configs) {
            sb.append('_');
            sb.append(s);
            stack.push(sb.toString());
        }
        while (stack.size() > 0) {
            String fileName = stack.pop();
            File f = new File(outDir, fileName);
            if (f.exists()) {
                ret = f.getPath();
                break;
            }
        }
        return ret;
    }

    private Pattern[] toPattern(String[] patternStrs) {
        Pattern[] patterns = new Pattern[patternStrs.length];
        for (int i = 0; i < patternStrs.length; i++) {
            patterns[i] = Pattern.compile(patternStrs[i]);
        }
        return patterns;
    }

    private void maskPatterns(Pattern[] patterns, String fname) throws Exception {
        String maskPattern = "#### A masked pattern was here ####";

        String line;
        BufferedReader in;
        BufferedWriter out;

        File file = new File(fname);
        File fileOrig = new File(fname + ".orig");
        FileUtils.copyFile(file, fileOrig);

        in = new BufferedReader(new InputStreamReader(new FileInputStream(fileOrig), "UTF-8"));
        out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"));

        boolean lastWasMasked = false;
        while (null != (line = in.readLine())) {
            for (Pattern pattern : patterns) {
                line = pattern.matcher(line).replaceAll(maskPattern);
            }

            if (line.equals(maskPattern)) {
                // We're folding multiple masked lines into one.
                if (!lastWasMasked) {
                    out.write(line);
                    out.write("\n");
                    lastWasMasked = true;
                }
            } else {
                out.write(line);
                out.write("\n");
                lastWasMasked = false;
            }
        }

        in.close();
        out.close();
    }

    private final Pattern[] planMask = toPattern(new String[]{
            ".*file:.*",
            ".*pfile:.*",
            ".*hdfs:.*",
            ".*/tmp/.*",
            ".*invalidscheme:.*",
            ".*lastUpdateTime.*",
            ".*lastAccessTime.*",
            ".*lastModifiedTime.*",
            ".*[Oo]wner.*",
            ".*CreateTime.*",
            ".*LastAccessTime.*",
            ".*Location.*",
            ".*LOCATION '.*",
            ".*transient_lastDdlTime.*",
            ".*last_modified_.*",
            ".*at org.*",
            ".*at sun.*",
            ".*at java.*",
            ".*at junit.*",
            ".*Caused by:.*",
            ".*LOCK_QUERYID:.*",
            ".*LOCK_TIME:.*",
            ".*grantTime.*",
            ".*[.][.][.] [0-9]* more.*",
            ".*job_[0-9_]*.*",
            ".*job_local[0-9_]*.*",
            ".*USING 'java -cp.*",
            "^Deleted.*",
            ".*DagName:.*",
            ".*Input:.*/data/files/.*",
            ".*Output:.*/data/files/.*",
            ".*total number of created files now is.*"
    });

    public int checkCliDriverResults(String tname) throws Exception {
        assert (qMap.containsKey(tname));

        String outFileExtension = getOutFileExtension(tname);
        String outFileName = outPath(outDir, tname + outFileExtension);

        File f = new File(logDir, tname + outFileExtension);

        maskPatterns(planMask, f.getPath());
        int exitVal = executeDiffCommand(f.getPath(),
                outFileName, false,
                qSortSet.contains(tname));

        if (exitVal != 0 && overWrite) {
            exitVal = overwriteResults(f.getPath(), outFileName);
        }

        return exitVal;
    }


    public int checkCompareCliDriverResults(String tname, List<String> outputs) throws Exception {
        assert outputs.size() > 1;
        maskPatterns(planMask, outputs.get(0));
        for (int i = 1; i < outputs.size(); ++i) {
            maskPatterns(planMask, outputs.get(i));
            int ecode = executeDiffCommand(
                    outputs.get(i - 1), outputs.get(i), false, qSortSet.contains(tname));
            if (ecode != 0) {
                LOG.info("Files don't match: " + outputs.get(i - 1) + " and " + outputs.get(i));
                return ecode;
            }
        }
        return 0;
    }

    private static int overwriteResults(String inFileName, String outFileName) throws Exception {
        // This method can be replaced with Files.copy(source, target, REPLACE_EXISTING)
        // once Hive uses JAVA 7.
        LOG.info("Overwriting results " + inFileName + " to " + outFileName);
        return executeCmd(new String[]{
                "cp",
                getQuotedString(inFileName),
                getQuotedString(outFileName)
        });
    }

    private static int executeDiffCommand(String inFileName,
                                          String outFileName,
                                          boolean ignoreWhiteSpace,
                                          boolean sortResults
    ) throws Exception {

        int result = 0;

        if (sortResults) {
            // sort will try to open the output file in write mode on windows. We need to
            // close it first.
            SessionState ss = SessionState.get();
            if (ss != null && ss.out != null && ss.out != System.out) {
                ss.out.close();
            }

            String inSorted = inFileName + SORT_SUFFIX;
            String outSorted = outFileName + SORT_SUFFIX;

            result = sortFiles(inFileName, inSorted);
            result |= sortFiles(outFileName, outSorted);
            if (result != 0) {
                LOG.error("ERROR: Could not sort files before comparing");
                return result;
            }
            inFileName = inSorted;
            outFileName = outSorted;
        }

        ArrayList<String> diffCommandArgs = new ArrayList<String>();
        diffCommandArgs.add("diff");

        // Text file comparison
        diffCommandArgs.add("-a");

        // Ignore changes in the amount of white space
        if (ignoreWhiteSpace || Shell.WINDOWS) {
            diffCommandArgs.add("-b");
        }

        // Files created on Windows machines have different line endings
        // than files created on Unix/Linux. Windows uses carriage return and line feed
        // ("\r\n") as a line ending, whereas Unix uses just line feed ("\n").
        // Also StringBuilder.toString(), Stream to String conversions adds extra
        // spaces at the end of the line.
        if (Shell.WINDOWS) {
            diffCommandArgs.add("--strip-trailing-cr"); // Strip trailing carriage return on input
            diffCommandArgs.add("-B"); // Ignore changes whose lines are all blank
        }
        // Add files to compare to the arguments list
        diffCommandArgs.add(getQuotedString(inFileName));
        diffCommandArgs.add(getQuotedString(outFileName));

        result = executeCmd(diffCommandArgs);

        if (sortResults) {
            new File(inFileName).delete();
            new File(outFileName).delete();
        }

        return result;
    }

    private static int sortFiles(String in, String out) throws Exception {
        return executeCmd(new String[]{
                "sort",
                getQuotedString(in),
        }, out, null);
    }

    private static int executeCmd(Collection<String> args) throws Exception {
        return executeCmd(args, null, null);
    }

    private static int executeCmd(String[] args) throws Exception {
        return executeCmd(args, null, null);
    }

    private static int executeCmd(Collection<String> args, String outFile, String errFile) throws
            Exception {
        String[] cmdArray = args.toArray(new String[args.size()]);
        return executeCmd(cmdArray, outFile, errFile);
    }

    private static int executeCmd(String[] args, String outFile, String errFile) throws Exception {
        LOG.info("Running: " + org.apache.commons.lang.StringUtils.join(args, ' '));

        PrintStream out = outFile == null ?
                SessionState.getConsole().getChildOutStream() :
                new PrintStream(new FileOutputStream(outFile), true);
        PrintStream err = errFile == null ?
                SessionState.getConsole().getChildErrStream() :
                new PrintStream(new FileOutputStream(errFile), true);

        Process executor = Runtime.getRuntime().exec(args);

        StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, err);
        StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, out);

        outPrinter.start();
        errPrinter.start();

        int result = executor.waitFor();

        outPrinter.join();
        errPrinter.join();

        if (outFile != null) {
            out.close();
        }

        if (errFile != null) {
            err.close();
        }

        return result;
    }

    private static String getQuotedString(String str) {
        return Shell.WINDOWS ? String.format("\"%s\"", str) : str;
    }

    public ASTNode parseQuery(String tname) throws Exception {
        return pd.parse(qMap.get(tname));
    }

    public void resetParser() throws SemanticException {
        pd = new ParseDriver();
        sem = new SemanticAnalyzer(conf);
    }

    public TreeMap<String, String> getQMap() {
        return qMap;
    }

    /**
     * HiveTestSetup defines test fixtures which are reused across testcases,
     * and are needed before any test can be run
     */
    public static class HiveTestSetup {
        private MiniZooKeeperCluster zooKeeperCluster = null;
        private int zkPort;
        private ZooKeeper zooKeeper;

        public HiveTestSetup() {
        }

        public void preTest(HiveConf conf) throws Exception {

            if (zooKeeperCluster == null) {
                //create temp dir
                String tmpBaseDir = System.getProperty("test.tmp.dir");
                File tmpDir = Utilities.createTempDir(tmpBaseDir);

                zooKeeperCluster = new MiniZooKeeperCluster();
                zkPort = zooKeeperCluster.startup(tmpDir);
            }

            if (zooKeeper != null) {
                zooKeeper.close();
            }

            int sessionTimeout = (int) conf.getTimeVar(HiveConf.ConfVars
                    .HIVE_ZOOKEEPER_SESSION_TIMEOUT, TimeUnit.MILLISECONDS);
            zooKeeper = new ZooKeeper("localhost:" + zkPort, sessionTimeout, new Watcher() {
                @Override
                public void process(WatchedEvent arg0) {
                }
            });

            String zkServer = "localhost";
            conf.set("hive.zookeeper.quorum", zkServer);
            conf.set("hive.zookeeper.client.port", "" + zkPort);
        }

        public void postTest(HiveConf conf) throws Exception {
            if (zooKeeperCluster == null) {
                return;
            }

            if (zooKeeper != null) {
                zooKeeper.close();
            }

            ZooKeeperHiveLockManager.releaseAllLocks(conf);
        }

        public void tearDown() throws Exception {
            if (zooKeeperCluster != null) {
                zooKeeperCluster.shutdown();
                zooKeeperCluster = null;
            }
        }
    }

    /**
     * QTRunner: Runnable class for running a a single query file.
     **/
    public static class HiveTestRunner implements Runnable {
        private final HiveTestUtil qt;
        private final String fname;

        public HiveTestRunner(HiveTestUtil qt, String fname) {
            this.qt = qt;
            this.fname = fname;
        }

        @Override
        public void run() {
            try {
                // assumption is that environment has already been cleaned once globally
                // hence each thread does not call cleanUp() and createSources() again
                qt.cliInit(fname, false);
                qt.executeClient(fname);
            } catch (Throwable e) {
                LOG.error("Query file " + fname + " failed with exception ", e);
                e.printStackTrace();
                outputTestFailureHelpMessage();
            }
        }
    }

    /**
     * Executes a set of query files in sequence.
     *
     * @param qfiles array of input query files containing arbitrary number of hive
     *               queries
     * @param qt     array of HiveTestUtils, one per qfile
     * @return true if all queries passed, false otw
     */
    public static boolean queryListRunnerSingleThreaded(File[] qfiles, HiveTestUtil[] qt)
            throws Exception {
        boolean failed = false;
        qt[0].cleanUp();
        qt[0].createSources();
        for (int i = 0; i < qfiles.length && !failed; i++) {
            qt[i].clearTestSideEffects();
            qt[i].cliInit(qfiles[i].getName(), false);
            qt[i].executeClient(qfiles[i].getName());
            int ecode = qt[i].checkCliDriverResults(qfiles[i].getName());
            if (ecode != 0) {
                failed = true;
                LOG.error("Test " + qfiles[i].getName()
                        + " results check failed with error code " + ecode);
                outputTestFailureHelpMessage();
            }
            qt[i].clearPostTestEffects();
        }
        return (!failed);
    }

    public static void outputTestFailureHelpMessage() {
        LOG.error("See ./ql/target/tmp/log/hive.log or ./itests/qtest/target/tmp/log/hive.log, "
                + "or check ./ql/target/surefire-reports or " +
                "./itests/qtest/target/surefire-reports/ for specific test cases logs.");
    }

    public static String ensurePathEndsInSlash(String path) {
        if (path == null) {
            throw new NullPointerException("Path cannot be null");
        }
        if (path.endsWith(File.separator)) {
            return path;
        } else {
            return path + File.separator;
        }
    }

    private static String[] cachedQvFileList = null;
    private static ImmutableList<String> cachedDefaultQvFileList = null;
    private static Pattern qvSuffix = Pattern.compile("_[0-9]+.qv$", Pattern.CASE_INSENSITIVE);

    public static List<String> getVersionFiles(String queryDir, String tname) {
        ensureQvFileList(queryDir);
        List<String> result = getVersionFilesInternal(tname);
        if (result == null) {
            result = cachedDefaultQvFileList;
        }
        return result;
    }

    private static void ensureQvFileList(String queryDir) {
        if (cachedQvFileList != null) return;
        // Not thread-safe.
        LOG.info("Getting versions from " + queryDir);
        cachedQvFileList = (new File(queryDir)).list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".qv");
            }
        });
        if (cachedQvFileList == null) return; // no files at all
        Arrays.sort(cachedQvFileList, String.CASE_INSENSITIVE_ORDER);
        List<String> defaults = getVersionFilesInternal("default");
        cachedDefaultQvFileList = (defaults != null)
                ? ImmutableList.copyOf(defaults) : ImmutableList.<String>of();
    }

    private static List<String> getVersionFilesInternal(String tname) {
        if (cachedQvFileList == null) {
            return new ArrayList<String>();
        }
        int pos = Arrays.binarySearch(cachedQvFileList, tname, String.CASE_INSENSITIVE_ORDER);
        if (pos >= 0) {
            throw new BuildException("Unexpected file list element: " + cachedQvFileList[pos]);
        }
        List<String> result = null;
        for (pos = (-pos - 1); pos < cachedQvFileList.length; ++pos) {
            String candidate = cachedQvFileList[pos];
            if (candidate.length() <= tname.length()
                    || !tname.equalsIgnoreCase(candidate.substring(0, tname.length()))
                    || !qvSuffix.matcher(candidate.substring(tname.length())).matches()) {
                break;
            }
            if (result == null) {
                result = new ArrayList<String>();
            }
            result.add(candidate);
        }
        return result;
    }

    public void failed(int ecode, String fname, String debugHint) {
        String command = SessionState.get() != null ? SessionState.get().getLastCommand() : null;
        Assert.fail("Client Execution failed with error code = " + ecode +
                (command != null ? " running " + command : "") + (debugHint != null ? debugHint :
                ""));
    }

    // for negative tests, which is succeeded.. no need to print the query string
    public void failed(String fname, String debugHint) {
        Assert.fail("Client Execution was expected to fail, but succeeded with error code 0 " +
                (debugHint != null ? debugHint : ""));
    }

    public void failedDiff(int ecode, String fname, String debugHint) {
        Assert.fail("Client Execution results failed with error code = " + ecode +
                (debugHint != null ? debugHint : ""));
    }

    public void failed(Throwable e, String fname, String debugHint) {
        String command = SessionState.get() != null ? SessionState.get().getLastCommand() : null;
        LOG.error("Exception: ", e);
        e.printStackTrace();
        LOG.error("Failed query: " + fname);
        Assert.fail("Unexpected exception " +
                org.apache.hadoop.util.StringUtils.stringifyException(e) + "\n" +
                (command != null ? " running " + command : "") +
                (debugHint != null ? debugHint : ""));
    }

    public static class WindowsPathUtil {

        public static void convertPathsFromWindowsToHdfs(HiveConf conf) {
            // Following local paths are used as HDFS paths in unit tests.
            // It works well in Unix as the path notation in Unix and HDFS is more or less same.
            // But when it comes to Windows, drive letter separator ':' & backslash '\" are invalid
            // characters in HDFS so we need to converts these local paths to HDFS paths before
            // using them
            // in unit tests.

            String orgWarehouseDir = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
            conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, getHdfsUriString(orgWarehouseDir));

            String orgTestTempDir = System.getProperty("test.tmp.dir");
            System.setProperty("test.tmp.dir", getHdfsUriString(orgTestTempDir));

            String orgTestWarehouseDir = System.getProperty("test.warehouse.dir");
            System.setProperty("test.warehouse.dir", getHdfsUriString(orgTestWarehouseDir));

            String orgScratchDir = conf.getVar(HiveConf.ConfVars.SCRATCHDIR);
            conf.setVar(HiveConf.ConfVars.SCRATCHDIR, getHdfsUriString(orgScratchDir));
        }

        public static String getHdfsUriString(String uriStr) {
            assert uriStr != null;
            if (Shell.WINDOWS) {
                // If the URI conversion is from Windows to HDFS then replace the '\' with '/'
                // and remove the windows single drive letter & colon from absolute path.
                return uriStr.replace('\\', '/')
                        .replaceFirst("/[c-zC-Z]:", "/")
                        .replaceFirst("^[c-zC-Z]:", "");
            }
            return uriStr;
        }
    }
}
