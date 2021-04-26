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


dev/misc_utils are supposed to contain multiple utility files.

As of this writing, we have git_jira_fix_version_check.py script
that takes care of identifying all git commits with commit
messages with any of these issues:
1. commit is reverted as per commit message
2. commit does not contain Jira number format PHOENIX-XXXX in message
3. Jira does not have expected fixVersion
4. Jira has expected fixVersion, but it is not yet resolved

Moreover, this script also finds any resolved Jira with expected
fixVersion but without any corresponding commit present.

This should be useful as part of RC preparation.

git_jira_fix_version_check supports python3 and it required
installation of jira:
```
pip3 install jira
```
The script also requires below inputs:
```
1. First commit hash to start excluding commits from history:
   Usually we can provide latest commit hash from last tagged release
   so that the script will only loop through all commits in git commit
   history before this commit hash. e.g for 4.16 release, we can provide
   git hash: a2adf5e572c5a4bcccee7f8ac43bad6b84293ec6
   because this was the last commit hash in last
   4.15 release tag: https://github.com/apache/phoenix/releases/tag/v4.15.0-HBase-1.5
   For 5.1.0, git hash: 8a819c6c3b4befce190c6ac759f744df511de61d
   as it was the last commit hash in 5.0 release tag:
   https://github.com/apache/phoenix/releases/tag/v5.0.0-HBase-2.0

2. Fix Version:
   Exact fixVersion that we would like to compare all Jira's fixVersions
   with. e.g for 4.16 first release, it should be 4.16.0
   for first release of 5.1, it should be 5.1.0

3. JIRA Project Name:
   The exact name of Project as case-sensitive e.g PHOENIX / OMID / TEPHRA

4. Path of project's working dir with release branch checked-in:
   Path of project from where we want to compare git hashes from. Local fork
   of the project should be up-to date with upstream and expected release
   branch should be checked-in.

5. Jira server url (default url: https://issues.apache.org/jira):
   Default value of server points to ASF Jiras but this script can be
   used outside of ASF Jira too.
```


Example of script execution:
```
$ python3 dev/misc_utils/git_jira_fix_version_check.py 
JIRA Project Name (e.g PHOENIX / OMID / TEPHRA etc): PHOENIX
First commit hash to start excluding commits from history: a2adf5e572c5a4bcccee7f8ac43bad6b84293ec6
Fix Version: 4.16.0
Jira server url (default: https://issues.apache.org/jira): 
Path of project's working dir with release branch checked-in: /Users/{USER}/Documents/phoenix

Check git status output and verify expected branch

On branch 4.16
Your branch is up to date with 'origin/4.16'.

nothing to commit, working tree clean


Jira/Git commit message diff starting: ##############################################
Jira not present with version: 4.16.0. 	 Commit: 96f7a28aacff1828c425fe50f7fef43641910e1f PHOENIX-6120 Addendum for license header
Jira not present with version: 4.16.0. 	 Commit: 67268793412789e3806664f90845e074b1f21a36 PHOENIX-6219 GlobalIndexChecker doesn't work for SingleCell indexes
Jira not present with version: 4.16.0. 	 Commit: ad70231581506dc954f6d27df43778343de8fbc0 PHOENIX-6120 Change IndexMaintainer for SINGLE_CELL_ARRAY_WITH_OFFSETS indexes
Jira not present with version: 4.16.0. 	 Commit: 182a6015ee5aa17110a17f862cf98f0934d1aca1 PHOENIX-6220 CREATE INDEX shouldn't ignore IMMUTABLE_STORAGE_SCHEME and COLUMN_ENDCODED_BYTES
Jira not present with version: 4.16.0. 	 Commit: 8a764813e4e4c9aa421923dc530a52d1bf8b4301 PHOENIX-6276: Log when hconnection is getting closed in ConnectionQueryServicesImpl
Jira is not resolved yet? 		 Commit: 237b7248dafe51087a8a95aa5bc67e5ce141e89d PHOENIX-6288 Minicluster startup problems on Jenkins
Jira not present with version: 4.16.0. 	 Commit: 3b6ea0240de9d4a0ad4747c3cd43cac5c949cc23 Merge PHOENIX-6182: IndexTool to verify and repair every index row (#1022)
Commit seems reverted. 			 Commit: a3b6d0b410b6eab43cef9fa4fa9074af592f8c81 Revert "PHOENIX-5140 TableNotFoundException occurs when we create local asynchronous index"
WARN: Jira not found. 			 Commit: 355d95a4762c3ccac1be35659f3c02c385e17b3b Update jacoco plugin version to 0.8.5.
WARN: Jira not found. 			 Commit: daa6816dcb3ac035bf8553e6bf2ff8a18e80e6e4 Local indexes get out of sync after changes for global consistent indexes.
...
...
...
Found first commit hash after which git history is redundant. commit: 02d5935cbbd75ad2491413042e5010bb76ed57c8
Exiting successfully
Jira/Git commit message diff completed: ##############################################

Any resolved Jira with fixVersion 4.16.0 but corresponding commit not present
Starting diff: ##############################################
PHOENIX-6259 is marked resolved with fixVersion 4.16.0 but no corresponding commit found
PHOENIX-6258 is marked resolved with fixVersion 4.16.0 but no corresponding commit found
PHOENIX-6255 is marked resolved with fixVersion 4.16.0 but no corresponding commit found
Completed diff: ##############################################


```


