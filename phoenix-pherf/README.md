Pherf is a performance test framework that exercises HBase through Apache Phoenix, a SQL layer interface.

## Build 
mvn clean package -DskipTests

## Important arguments:

- -h _Help_ <br />
- -l _Apply schema and load data_<br/>
- -q _Executes Multi-threaded query sets and write results_<br/>
- -z [quorum] _Zookeeper quorum_</br>
- -m _Enable monitor for statistics_<br/>
- -monitorFrequency [frequency in Ms] _Frequency at which the monitor will snopshot stats to log file. <br/>
- -drop _Regex drop all tables with schema name as PHERF. Example drop Event tables: -drop .*(EVENT).* Drop all: -drop .* or -drop all_<br/>
- -scenarioFile _Regex or file name of a specific scenario file to run._ <br />
- -schemaFile _Regex or file name of a specific schema file to run._ <br />
- -export Exports query results to CSV files in CSV_EXPORT directory <br />
- -diff Compares results with previously exported results <br />
- -hint _Executes all queries with specified hint. Example SMALL_ <br />
- -rowCountOverride
- -rowCountOverride [number of rows] _Specify number of rows to be upserted rather than using row count specified in schema_ </br>

## Running from IDE
Ex. Load data and execute queries. Specify the following as your IDE debug arguments:<br/> 
`-drop -l -q -z localhost`

## Running from command line
Ex. Drop existing tables, load data, and execute queries:<br/>  
`java -jar pherf-1.0-SNAPSHOT-jar-with-dependencies.jar -drop -l -q -z localhost`

## Adding Rules for Data Creation
Review [test_scenario.xml](/src/test/resources/scenario/test_scenario.xml) 
for syntax examples.<br />

* Rules are defined as `<columns />` and are applied in the order they appear in file.
* Rules of the same type override the values of a prior rule of the same type. If `<userDefined>true</userDefined>` is 
set, rule will only
apply override when type and name match the column name in Phoenix.
* `<prefix>` tag is set at the column level. It can be used to define a constant string appended to the beginning of 
CHAR and VARCHAR data type values. 
* **Required field** Supported Phoenix types: VARCHAR, CHAR, DATE, DECIMAL, INTEGER
    * denoted by the `<type>` tag
* User defined true changes rule matching to use both name and type fields to determine equivalence.
    * Default is false if not specified and equivalence will be determined by type only. **An important note here is that you can still override rules without the user defined flag, but they will change the rule globally and not just for a specified column.**
* **Required field** Supported Data Sequences
    * RANDOM:       Random value which can be bound by other fields such as length.
    * SEQUENTIAL:   Monotonically increasing long prepended to random strings.
        * Only supported on VARCHAR and CHAR types
    * LIST:         Means pick values from predefined list of values
* **Required field** Length defines boundary for random values for CHAR and VARCHAR types.
    * denoted by the `<length>` tag
* Column level Min/Max value defines boundaries for numerical values. For DATES, these values supply a range between 
which values are generated. At the column level the granularity is a year. At a specific data value level, the 
granularity is down to the Ms.
    * denoted by the `<minValue>` tag
    * denoted by the `<maxValue>` tag
* Null chance denotes the probability of generating a null value. From \[0-100\]. The higher the number, the more likely
the value will be null.
    * denoted by `<nullChance>`
* Name can either be any text or the actual column name in the Phoenix table.
    * denoted by the `<name>`
* Value List is used in conjunction with LIST data sequences. Each entry is a DataValue with a specified value to be 
used when generating data. 
    * Denoted by the `<valueList><datavalue><value/></datavalue></valueList>` tags
    * If the distribution attribute on the datavalue is set, values will be created according to
that probability. 
    * When distribution is used, values must add up to 100%. 
    * If distribution is not used, values will be randomly picked from the list with equal distribution.

## Defining Scenario
Scenario can have multiple querySets. Consider following example, concurrency of 1-4 means that each query will be 
executed starting with concurrency level of 1 and reach up to maximum concurrency of 4. Per thread, query would be 
executed to a minimum of 10 times or 10 seconds (whichever comes first). QuerySet by defult is executed serially but you
 can change executionType to PARALLEL so queries are executed concurrently. Scenarios are defined in XMLs stored 
 in the resource directory.

```

<scenarios>
    <!--Minimum of executionDurationInMs or numberOfExecutions. Which ever is reached first -->
    <querySet concurrency="1-4" executionType="PARALLEL" executionDurationInMs="10000" numberOfExecutions="10">
        <query id="q1" verifyRowCount="false" statement="select count(*) from PHERF.TEST_TABLE"/>
        <query id="q2" tenantId="1234567890" ddl="create view if not exists 
        myview(mypk varchar not null primary key, mycol varchar)" statement="upsert select ..."/>
    </querySet>
    <querySet concurrency="3" executionType="SERIAL" executionDurationInMs="20000" numberOfExecutions="100">
        <query id="q3" verifyRowCount="false" statement="select count(*) from PHERF.TEST_TABLE"/>
        <query id="q4" statement="select count(*) from PHERF.TEST_TABLE WHERE TENANT_ID='00D000000000062'"/>
    </querySet>
</scenario>
        
```

## Results
Results are written real time in _results_ directory. Open the result that is saved in .jpg format for real time 
visualization.

## Testing
Default quorum is localhost. If you want to override set the system variable.

Run unit tests: `mvn test -DZK_QUORUM=localhost`<br />
Run a specific method: `mvn -Dtest=ClassName#methodName test` <br />
To test on a real cluster: `./pherf.sh -drop all -l -q -z localhost -schemaFile .*user_defined_schema.sql -scenarioFile .*user_defined_scenario.xml`

More to come...
