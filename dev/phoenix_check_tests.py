#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Checks that the CI test jobs run every unit test and integration test (IT) class.

The CI splits the tests into several jobs (see dev/phoenix-test-group-args.sh):
* one job for the surefire unit tests under src/test/java;
* one job per failsafe execution (test group) in the root pom.xml, for the ITs under
  src/it/java. The two large groups are split again by the first letter of the class name.

Usage:
  phoenix_check_tests.py names <source root>
      Checks, without running anything, that every IT class in a test group can be selected by
      one of the jobs: its name ends in IT and starts with a letter A-Z. IT classes in no test
      group are listed as a warning: Maven never runs them, in CI or locally.
  phoenix_check_tests.py coverage <source root> <class lists dir>
      Checks that every test class ran in exactly one job, and lists every test class that ran in
      no job, with the reason. Each job writes the classes it ran to a file in <class lists dir>
      (see dev/phoenix-list-tests.sh).

Classes with a class-level @Ignore, and IT classes in no test group, are not expected to run.
They are listed, but do not fail the coverage check.
"""

import os
import re
import sys
from typing import Dict, List, Optional, Set

# The failsafe executions in the root pom.xml. A class runs only if it, or a class it extends, is
# in one of these JUnit categories.
TEST_GROUPS = {'ParallelStatsEnabledTest', 'ParallelStatsDisabledTest', 'NeedsOwnMiniClusterTest'}

# Default surefire includes.
UNIT_TEST_NAME = re.compile(r'^(Test.*|.*Test|.*Tests|.*TestCase)$')

# The IT jobs select classes with -Dit.test=<letter>*IT, for every letter A-Z.
IT_NAME = re.compile(r'^[A-Z][A-Za-z0-9_$]*IT$')

# Written by dev/phoenix-list-tests.sh when Yetus decided that no tests were needed.
NO_TESTS_MARKER = '# no tests run'

# Written by dev/phoenix-list-tests.sh when the Maven build failed. Maven then skips the modules
# after the failed one, so some classes are expected not to have run.
BUILD_FAILED_MARKER = '# build failed'


class TestClass:  # pylint: disable=too-few-public-methods
    """A top level class in a test source directory."""

    def __init__(self, name: str, test_dir: str, source: str):
        self.name = name
        self.test_dir = test_dir
        simple_name = self.simple_name
        self.abstract = re.search(r'\babstract\s+class\s+' + re.escape(simple_name) + r'\b',
                                  source) is not None
        declaration = re.search(r'\bclass\s+' + re.escape(simple_name)
                                + r'\b(?:\s*<[^{]*?>)?\s*(?:extends\s+([\w.]+))?', source)
        package = name.rsplit('.', 1)[0] if '.' in name else ''
        # The fully qualified name of the class it extends, if it can be found from the imports
        # or the package. Otherwise its simple name.
        self.parent = None
        if declaration and declaration.group(1):
            parent = declaration.group(1)
            simple = parent.rsplit('.', 1)[-1]
            imported = re.search(r'^import\s+([\w.]+\.' + re.escape(simple) + r')\s*;', source,
                                 re.MULTILINE)
            if '.' in parent:
                self.parent = parent
            elif imported:
                self.parent = imported.group(1)
            else:
                self.parent = (package + '.' if package else '') + simple
        self.categories = [c.rsplit('.', 1)[-1]
                           for c in re.findall(r'([\w.]+)\.class',
                                               ' '.join(re.findall(r'^\s*@Category\(([^)]*)\)',
                                                                   source, re.MULTILINE)))]
        self.has_tests = re.search(r'^\s*@Test\b', source, re.MULTILINE) is not None \
            or self.parent in ('TestCase', 'junit.framework.TestCase')
        self.ignored = re.search(r'^@Ignore\b', source, re.MULTILINE) is not None

    @property
    def simple_name(self) -> str:
        """The name of the class without its package."""
        return self.name.rsplit('.', 1)[-1]


UNIT_TEST_DIR = os.path.join('src', 'test', 'java')
IT_DIR = os.path.join('src', 'it', 'java')


def find_test_classes(source_root: str) -> Dict[str, TestClass]:
    """Find the top level classes in the test source directories of every module, by name."""
    classes = {}
    for dirpath, dirnames, filenames in os.walk(source_root):
        dirnames[:] = [d for d in dirnames if d != 'target' and not d.startswith('.')]
        test_dir = next((d for d in (UNIT_TEST_DIR, IT_DIR)
                         if os.sep + d + os.sep in dirpath + os.sep), None)
        if test_dir is None:
            continue
        for filename in filenames:
            if not filename.endswith('.java'):
                continue
            path = os.path.join(dirpath, filename)
            with open(path, encoding='utf-8', errors='replace') as stream:
                source = stream.read()
            package = re.search(r'^package\s+([\w.]+)\s*;', source, re.MULTILINE)
            name = (package.group(1) + '.' if package else '') + filename[:-len('.java')]
            test_class = TestClass(name, test_dir, source)
            classes[test_class.name] = test_class
    return classes


def inherited(classes: Dict[str, TestClass], test_class: TestClass, attribute: str):
    """The value of an attribute of the class, or of the nearest class it extends that has it."""
    seen = set()
    current: Optional[TestClass] = test_class
    while current is not None and current.name not in seen:
        value = getattr(current, attribute)
        if value:
            return value
        seen.add(current.name)
        current = classes.get(current.parent) if current.parent else None
    return None


def runnable(classes: Dict[str, TestClass], test_dir: str) -> List[TestClass]:
    """The concrete classes in test_dir that have test methods, directly or by inheritance."""
    return [c for c in classes.values()
            if c.test_dir == test_dir and not c.abstract and inherited(classes, c, 'has_tests')]


def expected_unit_tests(classes: Dict[str, TestClass]) -> Set[str]:
    """The classes that surefire runs."""
    return {c.name for c in runnable(classes, UNIT_TEST_DIR)
            if UNIT_TEST_NAME.match(c.simple_name)}


def in_test_group(classes: Dict[str, TestClass], test_class: TestClass) -> bool:
    """Whether the class, or a class it extends, is in one of the failsafe test groups."""
    return bool(set(inherited(classes, test_class, 'categories') or []) & TEST_GROUPS)


def it_problems(classes: Dict[str, TestClass]) -> Dict[str, str]:
    """The IT classes in a test group that no CI job can select, with the reason."""
    problems = {}
    for test_class in runnable(classes, IT_DIR):
        if test_class.ignored or not in_test_group(classes, test_class):
            continue
        if not test_class.simple_name.endswith('IT'):
            problems[test_class.name] = 'the class name does not end in IT'
        elif not IT_NAME.match(test_class.simple_name):
            problems[test_class.name] = 'the class name does not start with a letter A-Z'
    return problems


def not_in_test_group(classes: Dict[str, TestClass]) -> Set[str]:
    """The IT classes that Maven never runs, because they are in no test group."""
    return {c.name for c in runnable(classes, IT_DIR)
            if not c.ignored and not in_test_group(classes, c)}


def expected_its(classes: Dict[str, TestClass]) -> Set[str]:
    """The IT classes that the CI jobs should run."""
    return {c.name for c in runnable(classes, IT_DIR) if in_test_group(classes, c)}


def ignored_classes(classes: Dict[str, TestClass]) -> Set[str]:
    """Classes annotated with @Ignore. JUnit may skip them without reporting them."""
    return {c.name for c in classes.values() if c.ignored}


# The reason given for a test class that should have run, but did not.
UNEXPECTED = 'although it should have run'


def not_run_reasons(classes: Dict[str, TestClass], ran: Set[str]) -> Dict[str, str]:
    """Every concrete test class that ran in no job, with the reason it did not run."""
    unit_tests = expected_unit_tests(classes)
    problems = it_problems(classes)
    no_group = not_in_test_group(classes)
    reasons = {}
    for test_class in runnable(classes, UNIT_TEST_DIR) + runnable(classes, IT_DIR):
        name = test_class.name
        if name in ran:
            continue
        if test_class.ignored:
            reasons[name] = 'because of a class-level @Ignore'
        elif name in no_group:
            reasons[name] = ('because Maven never runs it: no @Category with one of '
                             + ', '.join(sorted(TEST_GROUPS)))
        elif name in problems:
            reasons[name] = 'because ' + problems[name]
        elif test_class.test_dir == UNIT_TEST_DIR and name not in unit_tests:
            reasons[name] = 'because surefire does not select its name (Test*, *Test, *Tests, ' \
                            '*TestCase)'
        else:
            reasons[name] = UNEXPECTED
    return reasons


def print_list(title: str, names, notes: Optional[Dict[str, str]] = None) -> None:
    """Print a titled list of class names."""
    print(title)
    for name in sorted(names):
        print('  ' + name + (' (' + notes[name] + ')' if notes and name in notes else ''))
    print()


def check_names(source_root: str) -> int:
    """Check that every IT class can be selected by one of the CI jobs."""
    classes = find_test_classes(source_root)
    no_group = not_in_test_group(classes)
    if no_group:
        print_list('WARNING: Maven never runs these IT classes, because they have no @Category '
                   'with one of ' + ', '.join(sorted(TEST_GROUPS)) + ':', no_group)
    problems = it_problems(classes)
    if problems:
        print_list('ERROR: no CI job runs these IT classes:', problems, problems)
        return 1
    print('Every IT class in a test group is selected by one of the CI test jobs.')
    return 0


def read_class_lists(lists_dir: str) -> Dict[str, List[str]]:
    """The classes run by each job, from the files written by dev/phoenix-list-tests.sh."""
    lists = {}
    for filename in sorted(os.listdir(lists_dir)):
        path = os.path.join(lists_dir, filename)
        if os.path.isfile(path):
            with open(path, encoding='utf-8') as stream:
                lists[filename] = [line.strip() for line in stream if line.strip()]
    return lists


def jobs_by_class(lists: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """The jobs that ran each test class."""
    jobs: Dict[str, List[str]] = {}
    for job, names in lists.items():
        # Nested test classes are reported as Outer$Inner.
        for name in {n.split('$', 1)[0] for n in names if not n.startswith('#')}:
            jobs.setdefault(name, []).append(job)
    return jobs


def check_coverage(source_root: str, lists_dir: str) -> int:
    """Check that every test class ran in exactly one job, and list the classes that did not run."""
    lists = read_class_lists(lists_dir)
    if not lists:
        print('ERROR: no test class lists found in ' + lists_dir + '.')
        return 1
    no_tests = [job for job, names in lists.items() if NO_TESTS_MARKER in names]
    if len(no_tests) == len(lists):
        print('Yetus ran no tests for this change, so the test coverage was not checked.')
        return 0
    if no_tests:
        print_list('ERROR: these jobs ran no tests, but the other jobs did:', no_tests)
        return 1

    ran = jobs_by_class(lists)
    classes = find_test_classes(source_root)
    expected = expected_unit_tests(classes) | expected_its(classes)
    print('Test classes run: ' + str(len(ran)) + ' in ' + str(len(lists)) + ' jobs, of '
          + str(len(expected)) + ' test classes in the source tree.')
    print()

    status = 0
    duplicates = {name: ', '.join(jobs) for name, jobs in ran.items() if len(jobs) > 1}
    if duplicates:
        print_list('ERROR: these test classes ran in more than one job:', duplicates, duplicates)
        status = 1
    failed_builds = [job for job, names in lists.items() if BUILD_FAILED_MARKER in names]
    reasons = not_run_reasons(classes, set(ran))
    unexpected = [name for name, reason in reasons.items() if reason == UNEXPECTED]
    if reasons:
        print('Test classes that did not run in any job: ' + str(len(reasons)) + '.')
        print()
        by_reason: Dict[str, List[str]] = {}
        for name, reason in reasons.items():
            by_reason.setdefault(reason, []).append(name)
        for reason in sorted(by_reason, key=lambda r: (r == UNEXPECTED, r)):
            print_list('Not run, ' + reason + ':', by_reason[reason])
    if unexpected and not failed_builds:
        print('ERROR: ' + str(len(unexpected)) + ' test classes that should run did not run in '
              'any job. A test JVM may have crashed, or the class is not selected by any job.')
        status = 1
    elif unexpected:
        print('WARNING: the build failed in ' + ', '.join(failed_builds) + ', so Maven may have '
              'skipped the ' + str(len(unexpected)) + ' test classes that should have run.')
    if status == 0 and not unexpected:
        print('Every test class that should run ran in exactly one job.')
    return status


def main() -> int:
    """Run the check given on the command line."""
    if len(sys.argv) == 3 and sys.argv[1] == 'names':
        return check_names(sys.argv[2])
    if len(sys.argv) == 4 and sys.argv[1] == 'coverage':
        return check_coverage(sys.argv[2], sys.argv[3])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == '__main__':
    sys.exit(main())
