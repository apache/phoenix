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


import inspect
import os
import re
import subprocess

from jira import JIRA

first_exclude_commit_hash = input("First commit hash to start excluding commits from history: ")
fix_version = input("Fix Version: ")

jira = JIRA({"server": "https://issues.apache.org/jira"})
os.chdir(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))
os.chdir('..')
os.chdir('..')
git_status_msg = subprocess.check_output(['git', 'status']).decode("utf-8")
print('\nCheck git status output and verify expected branch')
print(git_status_msg)

print('\nJira/Git commit message diff starting: ##############################################')
for commit in subprocess.check_output(['git', 'log', '--pretty=oneline']).decode("utf-8").splitlines():
    if commit.startswith(first_exclude_commit_hash):
        print("Found first commit hash after which git history is redundant. commit: " + first_exclude_commit_hash)
        print("Exiting successfully")
        break
    if re.search('revert', commit, re.IGNORECASE):
        print("Commit seems reverted. \t\t\t Commit: " + commit)
        continue
    if 'PHOENIX-' not in commit:
        print("WARN: Jira not found. \t\t\t Commit: " + commit)
        continue
    jira_num = ''
    for c in commit.split('PHOENIX-')[1]:
        if c.isdigit():
            jira_num = jira_num + c
        else:
            break
    issue = jira.issue('PHOENIX-' + jira_num)
    expected_fix_version = False
    for version in issue.fields.fixVersions:
        if version.name == fix_version:
            expected_fix_version = True
            break
    if not expected_fix_version:
        print("Jira not present with version: " + fix_version + ". \t Commit: " + commit)
        continue
    if issue.fields.resolution is None or issue.fields.resolution.name != 'Fixed':
        print("Jira is not resolved yet? \t\t Commit: " + commit)
    else:
        # This means Jira corresponding to current commit message is resolved with expected fixVersion
        # Noop by default, if needed, convert to print statement.
        pass
print('Jira/Git commit message diff completed: ##############################################')


#
# If we have result from "git log --pretty=oneline" stored in file, we can use below script and
# provide file location alone.
#
# with open('file_location_with_git_log') as f:
#     content = f.readlines()
# content_list = [x.strip() for x in content]
#
# for content in content_list:
#     if re.search('revert', content, re.IGNORECASE):
#         print("Commit seems reverted. \t\t\t\t Commit: " + content)
#         continue
#     if 'PHOENIX-' not in content:
#         print("WARN: Jira not found. \t\t\t\t Commit: " + content)
#         continue
#     jira_num = ''
#     for c in content.split('PHOENIX-')[1]:
#         if c.isdigit():
#             jira_num = jira_num + c
#         else:
#             break
#     issue = jira.issue('PHOENIX-' + jira_num)
#     expected_fix_version = False
#     for version in issue.fields.fixVersions:
#         if version.name == '4.16.0':
#             expected_fix_version = True
#     if not expected_fix_version:
#         print("Jira not present with version: 4.16.0. \t Commit: " + content)
#         continue
#     if issue.fields.resolution is None or issue.fields.resolution.name != 'Fixed':
#         print("Jira is not resolved yet? \t\t\t\t Commit: " + content)
