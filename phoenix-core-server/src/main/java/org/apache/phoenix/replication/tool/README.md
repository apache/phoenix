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

# Phoenix Replication Log File Analyzer

A command-line tool for analyzing Phoenix Replication Log files.

This tool can read a log file or a directory of log files, print the header, trailer (if present),
and block headers, optionally decode and print the LogRecord contents in a human-readable format,
verify checksums for each block, and report any corruption or format violations.

## Usage

```bash
hadoop jar phoenix-server.jar org.apache.phoenix.replication.tool.LogFileAnalyzer [options] \
    <log-file-or-directory>
```

### Options

- `-h, --help`: Print help message
- `-v, --verbose`: Print verbose output including block headers
- `-c, --check`: Verify checksums and report any corruption
- `-d, --decode`: Decode and print LogRecord contents in human-readable format

### Examples

1. Basic analysis of a log file:

```bash
hadoop jar phoenix-server.jar org.apache.phoenix.replication.tool.LogFileAnalyzer \
    /my/log.plog
```

1. Analyze with record decoding:

```bash
hadoop jar phoenix-server.jar org.apache.phoenix.replication.tool.LogFileAnalyzer -d \
    /my/log.plog
```

1. Verify checksums and report corruption:

```bash
hadoop jar phoenix-server.jar org.apache.phoenix.replication.tool.LogFileAnalyzer -c \
    /my/log.plog
```

1. Analyze all log files in a directory with verbose output:

```bash
hadoop jar phoenix-server.jar org.apache.phoenix.replication.tool.LogFileAnalyzer -v \
    /my/logs/
```
