#!/usr/bin/env python
############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
############################################################################


import os
import re
import subprocess

from jira import JIRA

jira_project_name = input("JIRA Project Name (e.g PHOENIX / OMID / TEPHRA etc): ")
# Define project_jira_key with - appended. e.g for PHOENIX Jiras,
# project_jira_key should be PHOENIX-
project_jira_key = jira_project_name + '-'

first_exclude_commit_hash = input("First commit hash to start excluding commits from history: ")
fix_version = input("Fix Version: ")

jira_server_url = input(
    "Jira server url (default: https://issues.apache.org/jira): ") \
        or "https://issues.apache.org/jira"

jira = JIRA({"server": jira_server_url})

local_project_dir = input("Path of project's working dir with release branch checked-in: ")
os.chdir(local_project_dir)

git_status_msg = subprocess.check_output(['git', 'status']).decode("utf-8")
print('\nCheck git status output and verify expected branch\n')
print(git_status_msg)

print('\nJira/Git commit message diff starting: ##############################################')

issue_set_from_commit_msg = set()

for commit in subprocess.check_output(['git', 'log', '--pretty=oneline']).decode(
        "utf-8").splitlines():
    if commit.startswith(first_exclude_commit_hash):
        print("Found first commit hash after which git history is redundant. commit: "
              + first_exclude_commit_hash)
        print("Exiting successfully")
        break
    if re.search('revert', commit, re.IGNORECASE):
        print("Commit seems reverted. \t\t\t Commit: " + commit)
        continue
    if project_jira_key not in commit:
        print("WARN: Jira not found. \t\t\t Commit: " + commit)
        continue
    jira_num = ''
    for c in commit.split(project_jira_key)[1]:
        if c.isdigit():
            jira_num = jira_num + c
        else:
            break
    issue = jira.issue(project_jira_key + jira_num)
    expected_fix_version = False
    for version in issue.fields.fixVersions:
        if version.name == fix_version:
            expected_fix_version = True
            break
    if not expected_fix_version:
        print("Jira not present with version: " + fix_version + ". \t Commit: " + commit)
        continue
    if issue.fields.status is None or issue.fields.status.name not in ('Resolved', 'Closed'):
        print("Jira is not resolved yet? \t\t Commit: " + commit)
    else:
        # This means Jira corresponding to current commit message is resolved with expected
        # fixVersion.
        # This is no-op by default, if needed, convert to print statement.
        issue_set_from_commit_msg.add(project_jira_key + jira_num)

print('Jira/Git commit message diff completed: ##############################################')

print('\nAny resolved Jira with fixVersion ' + fix_version
      + ' but corresponding commit not present')
print('Starting diff: ##############################################')
all_issues_with_fix_version = jira.search_issues(
    'project=' + jira_project_name + ' and status in (Resolved,Closed) and fixVersion='
    + fix_version)

for issue in all_issues_with_fix_version:
    if issue.key not in issue_set_from_commit_msg:
        print(issue.key + ' is marked resolved with fixVersion ' + fix_version
            + ' but no corresponding commit found')

print('Completed diff: ##############################################')
