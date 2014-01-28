# Building Phoenix Project

Phoenix is a fully mavenized project. That means you can build simply by doing:

```
$ mvn package
```

builds, test and package Phoenix and put the resulting jars (phoenix-[version].jar and phoenix-[version]-client.jar) in the generated phoenix-core/target/ and phoenix-assembly/target/ directories respectively.

To build, but skip running the tests, you can do:

```
 $ mvn package -DskipTests
```

To only build the generated parser (i.e. <code>PhoenixSQLLexer</code> and <code>PhoenixSQLParser</code>), you can do:

```
 $ mvn install -DskipTests
 $ mvn process-sources
```

To build an Eclipse project, install the m2e plugin and do an File->Import...->Import Existing Maven Projects selecting the root directory of Phoenix.

