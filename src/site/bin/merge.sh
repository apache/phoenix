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

current_dir=$(cd $(dirname $0);pwd)
cd $current_dir
SITE_TARGET="../../../target/site"
java -jar merge.jar ../language_reference_source/index.html $SITE_TARGET/language/index.html
java -jar merge.jar ../language_reference_source/functions.html $SITE_TARGET/language/functions.html
java -jar merge.jar ../language_reference_source/datatypes.html $SITE_TARGET/language/datatypes.html
cd $SITE_TARGET

grep -rl class=\"nav-collapse\" . | xargs sed -i 's/class=\"nav-collapse\"/class=\"nav-collapse collapse\"/g';grep -rl class=\"active\" . | xargs sed -i 's/class=\"active\"/class=\"divider\"/g'
grep -rl "dropdown active" . | xargs sed -i 's/dropdown active/dropdown/g'
