/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

pipeline {
    agent none

    options {
        buildDiscarder(logRotator(daysToKeepStr: '15', artifactDaysToKeepStr: '5'))
        timestamps()
    }

    stages {
        stage('MatrixBuild') {
            matrix {

                agent {
                    dockerfile {
                        dir 'dev/docker'
                        filename 'Dockerfile.multibranch'
                        label 'Hadoop'
                    }
                }

                axes {
                    axis {
                        name 'HBASE_PROFILE'
                        values '2.4', '2.5', '2.6'
                    }
                }

                environment {
                    MAVEN_OPTS = '-Xmx3G'
                }

                stages {

                    stage('RebuildHBase') {
                        options {
                            timeout(time: 30, unit: 'MINUTES')
                        }
                        environment {
                            //HBase 2.2.7 cannot be built with Java 11
                            PATH = "/usr/lib/jvm/java-8-openjdk-amd64/bin:$PATH"
                            JAVA_HOME = "/usr/lib/jvm/java-8-openjdk-amd64"
                            HBASE_VERSION = sh(returnStdout: true, script: "mvn help:evaluate -Dexpression=hbase-${HBASE_PROFILE}.runtime.version -q -DforceStdout").trim()
                        }
                        steps {
                            sh "dev/rebuild_hbase.sh ${HBASE_VERSION}"
                        }
                    }

                    stage('BuildAndTest') {
                        options {
                            timeout(time: 5, unit: 'HOURS')
                        }
                        steps {
                            dir("HBASE_${HBASE_PROFILE}") {
                                checkout scm
                                sh """#!/bin/bash
                                    ulimit -a
                                    mvn clean verify -DPhoenixPatchProcess -Dskip.code-coverage -Dhbase.profile=${HBASE_PROFILE} -B
                                """
                            }
                        }
                        post {
                            always {
                                sh "find HBASE_${HBASE_PROFILE}/ -name \\*.txt -exec gzip {} \\;"
                                archiveArtifacts artifacts: "HBASE_${HBASE_PROFILE}/**/target/surefire-reports/*.txt.gz,**/target/failsafe-reports/*.txt.gz,**/target/surefire-reports/*.dumpstream,**/target/failsafe-reports/*.dumpstream,**/target/surefire-reports/*.dump,**/target/failsafe-reports/*.dump"
                                junit "HBASE_${HBASE_PROFILE}/**/target/surefire-reports/TEST-*.xml"
                                junit "HBASE_${HBASE_PROFILE}/**/target/failsafe-reports/TEST-*.xml"
                            }
                        }
                    }
                }

                post {

                    always {
                        emailext(
                            subject: "Apache-Phoenix | ${BRANCH_NAME} | HBase ${HBASE_PROFILE} | Build ${BUILD_DISPLAY_NAME} ${currentBuild.currentResult}",
                            to: 'commits@phoenix.apache.org',
                            replyTo: 'commits@phoenix.apache.org',
                            mimeType: 'text/html',
                            recipientProviders: [
                                [$class: "DevelopersRecipientProvider"],
                                [$class: 'CulpritsRecipientProvider'],
                                [$class: 'RequesterRecipientProvider']],
                            body: """
<a href="http://phoenix.apache.org"><img src='http://phoenix.apache.org/images/phoenix-logo-small.png'/></a>
<br><b>${BRANCH_NAME}</b> branch <b> HBase ${HBASE_PROFILE} </b> build ${BUILD_DISPLAY_NAME} status <b>${currentBuild.currentResult}</b><hr/>
<b>Build ${BUILD_DISPLAY_NAME}</b> ${BUILD_URL}
<hr/>
"""
                       )
                    }

                    cleanup {
                        deleteDir()
                    }
                }
            }
        }
    }
}