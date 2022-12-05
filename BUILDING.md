<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

![logo](http://phoenix.apache.org/images/logo.png)

<b>[Apache Phoenix](http://phoenix.apache.org/)</b> is a SQL skin over HBase delivered as a
client-embedded JDBC driver targeting low latency queries over HBase data. Visit the Apache
Phoenix website <b>[here](http://phoenix.apache.org/)</b>.


Building Apache Phoenix
========================

Phoenix uses Maven (3.X) to build all its necessary resources.

HBase 2 and Hadoop 3
--------------------
Phoenix 5.x requires Hadoop 3. While HBase 2.x is compatible with Hadoop 3, the public Maven Hbase
artifacts are built with Hadoop 2, and are not.

With the 2.5.2 release HBase has started releasing Maven artifacts built with Hadoop3.
Where available, use the Maven artifacts with the `-hadoop3` postfix in the version,
i.e. `2.5.2-hadoop3`, and ignore the rest of this section.
For HBase 2.5, Phoenix already uses the -hadoop3 version by default.

For HBase versions where hadoop3 artifacts are not available, you need to rebuild HBase with
Hadoop 3, and install it to the local maven repo of the build host.

`$ wget https://downloads.apache.org/hbase/2.2.5/hbase-2.2.5-src.tar.gz`
`$ tar xfvz hbase-2.2.5-src.tar.gz`
`$ cd hbase-2.2.5`
`$ mvn install -Dhadoop.profile=3.0 -DskipTests`

Replace 2.2.5 with the actual Hbase version you are using in the Phoenix build.

You can find the exact HBase version each phoenix HBase profile uses by checking <hbase.version>
in the corresponding profile section at the end of phoenix/pom.xml, or you can specify the HBase
version to build Phoenix with explicitly (see below)

See https://issues.apache.org/jira/browse/PHOENIX-5993 for more information.

Building from source
--------------------

On first setup, you may need to run `$ mvn install -DskipTests`
to install the local jars. This is a side-effect of multi-module maven projects

To re-generate the antlr based files:  
`$ mvn process-sources`

To build the jars and the assembly tarball:  
`$ mvn package`
and optionally, to just skip all the tests and build the jars:  
`$ mvn package -DskipTests`

Note: javadocs are generated in target/apidocs

HBase version compatibility
---------------------------

As Phoenix uses *limited public* HBase APIs, which sometimes change even within a minor release,
Phoenix may not build or work with older releases of HBase, or ones that were released after
Phoenix, even within the same HBase minor release.

By default, Phoenix will be built for the latest known patch level of the latest HBase 2.x
minor release that Phoenix supports.

You can specify the targeted HBase minor release by setting the `hbase.profile` system property for 
maven.

You can also specify the exact HBase release to build Phoenix with by additionally
setting the `hbase.version` system property.

 * `mvn clean install` will use the latest known patch release of the the latest supported HBase 2 minor relese
 * `mvn clean install -Dhbase.profile=2.1` will use the latest known patch release of HBase 2.1
 * `mvn clean install -Dhbase.profile=2.1 -Dhbase.version=2.1.7` will build with HBase 2.1.7

Phoenix verifies the specified `hbase.profile` and `hbase.version` properties, and will reject
combinations that are known not to work. You may disable this verification by adding
`-Denforcer.skip=true` to the maven command line. (In case you are using an HBase package that
modifies the canonical version number in a way that Phoenix cannot parse)

Importing into eclipse
----------------------

Use the m2e eclipse plugin and do Import->Maven Project and just pick the root 'phoenix' directory.

Running the tests
-----------------

All Unit Tests  
`$ mvn clean test`

All Unit Tests and Integration tests (takes a few hours)
`$ mvn clean verify`

The verify maven target will also run dependency:analyze-only, which checks if the dependencies
 used in the code and declared in the maven projects match. The code coverage report would be
generated at /target/site/jacoco/index.html

To skip code coverage analysis
`$ mvn verify -Dskip.code-coverage`

Running project reports
-----------------------

Phoenix currently supports generating the standard set of Maven Project Info Reports, as well as
Spotbugs, Apache Creadur RAT, OWASP Dependency-Check, and Jacoco Code Coverage reports.

To run all available reports (takes a few hours)
`$ mvn clean verify site -Dspotbugs.site`

To run OWASP, RAT and Spotbugs, but not Jacoco (takes ~10 minutes)
`$ mvn clean compile test-compile site -Dspotbugs.site`

The reports are accessible via `target/site/index.html`, under the main project,
as well as each of the subprojects. (not every project has all reports)

Generate Apache Web Site
------------------------

checkout https://svn.apache.org/repos/asf/phoenix  
`$ build.sh`
