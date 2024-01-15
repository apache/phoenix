package org.apache.phoenix.end2end.prefix;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.prefix.table.TableTTLInfo;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.InstanceResolver;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class ConsoleApplication  {

	public static void run(String... args) {
		
		TableTTLInfoTestHelper tableTTLInfoTestHelper = new TableTTLInfoTestHelper();
		Scanner in = new Scanner(System.in);
		try {
			registerDriver();
			System.out.println("> help - mode (binary), db (tablename), test, find (prefix, offset), info");
			System.out.print("> ");
			String op = in.next();
			boolean binaryMode = true;
			while (!op.equals("quit")) {
				switch (op) {
					case "mode" :
						System.out.print("mode > ");
							String mode = in.next();
							if (mode.equalsIgnoreCase("b") || mode.equalsIgnoreCase("binary")) {
								binaryMode = true;
							}
							System.out.println("Setting mode to " + (binaryMode ? "binary" : "text"));
							break;
					case "db" : {
						System.out.print("tablename > ");
						String fullTableName = in.next();
						String[] tableNameParts = fullTableName.split("\\.");
						String parentSchemaName = tableNameParts.length == 2 ? tableNameParts[0] : "";
						String parentTableName = tableNameParts.length == 2 ? tableNameParts[1] : tableNameParts[0];
						System.out.println(String.format("Indexing %s.%s", parentSchemaName, parentTableName));
						tableTTLInfoTestHelper.indexTableInfoFromCatalog("jdbc:phoenix:localhost:2181", parentSchemaName, parentTableName);
						break;
					}
					case "test" :
						tableTTLInfoTestHelper.indexTableInfoFromSampleData(getSampleData());
						break;
					case "find" :
						System.out.print("search-str > ");
						String searchPrefixStr = in.next();
						System.out.print("offset > ");
						int searchOffset = in.nextInt();
						TableTTLInfo
                                t = tableTTLInfoTestHelper.findTable(searchPrefixStr, searchOffset, binaryMode);
						if (t != null) {
							System.out.println(String.format("Matched table with info : %d\t%s\t%s", t.getTTL(),
									Bytes.toStringBinary(t.getTenantId()),
									Bytes.toStringBinary(t.getEntityName())));
						} else {
							System.out.println("Did not find any matching table");
						}
						break;
					case "info" : {
						System.out.print("tablename > ");
						String fullTableName = in.next();
						String[] tableNameParts = fullTableName.split("\\.");
						String parentSchemaName = tableNameParts.length == 2 ? tableNameParts[0] : "";
						String parentTableName = tableNameParts.length == 2 ? tableNameParts[1] : tableNameParts[0];
						System.out.println("num-tables = " + tableTTLInfoTestHelper.getTtlInfoCache().getNumTablesInCache());
						System.out.println("num-prefixes = " + tableTTLInfoTestHelper.getIndex().getValidPrefixes());
						List<TableTTLInfo> tableInfoList = tableTTLInfoTestHelper.getTableInfoFromCatalog("jdbc:phoenix:localhost:2181", parentSchemaName, parentTableName);
						tableInfoList.forEach(m -> {
							System.out.println("table : " + m);
						});
						break;
					}
					default:
						String operand3 = in.next();
						System.out.println("not supported = " + operand3);
						break;
				}
				System.out.print(">");
				op = in.next();			
			}
		} catch (SQLException e) {
			System.out.println("Failed to register phoenix driver");
			e.printStackTrace(System.out);
			throw new RuntimeException(e);
		} finally {
			in.close();
		}
	}

	private static void registerDriver() throws SQLException {
		final Configuration conf = HBaseConfiguration.create();
		conf.set(QueryServices.GLOBAL_METRICS_ENABLED, String.valueOf(false));
		conf.set(QueryServices.RENEW_LEASE_ENABLED, String.valueOf(false));
		// Clear the cached singletons so we can inject our own.
		InstanceResolver.clearSingletons();
		// Make sure the ConnectionInfo doesn't try to pull a default Configuration
		InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
			@Override
			public Configuration getConfiguration() {
				return conf;
			}

			@Override
			public Configuration getConfiguration(Configuration confToClone) {
				Configuration copy = new Configuration(conf);
				copy.addResource(confToClone);
				return copy;
			}
		});

		Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
		//setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));

		DriverManager.registerDriver(PhoenixDriver.INSTANCE);
	}

	private static List<TableTTLInfo> getSampleData() {

		List<TableTTLInfo> tableList = new ArrayList<TableTTLInfo>();
		tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "", "TEST_ENTITY.GV_000001", "001", 30000));
		tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "", "TEST_ENTITY.GV_000002", "002", 60000));
		tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "", "TEST_ENTITY.GV_000003", "003", 60000));
		tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "00D0t0001000001", "TEST_ENTITY.Z01", "00D0t0001000001Z01", 60000));
		tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "00D0t0002000001", "TEST_ENTITY.Z01","00D0t0002000001Z01", 120000));
		tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "00D0t0003000001", "TEST_ENTITY.Z01","00D0t0003000001Z01", 180000));
		tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "00D0t0004000001", "TEST_ENTITY.Z01","00D0t0004000001Z01", 300000));
		tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "00D0t0005000001", "TEST_ENTITY.Z01","00D0t0005000001Z01", 6000));
		return tableList;
	}

	public static void main(String[] args) {		
		ConsoleApplication.run(args);
	}

}
