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
        buildDiscarder(logRotator(daysToKeepStr: '30'))
        timeout(time: 6, unit: 'HOURS')
        timestamps()
    }

    stages {
        stage('MatrixBuild') {
            matrix {
                agent {
                    label 'Hadoop'
                }

                axes {
                    axis {
                        name 'HBASE_PROFILE'
                        values '1.3', '1.4', '1.6'
                    }
                }

                tools {
                    maven "Maven (latest)"
                    jdk "JDK 1.8 (latest)"
                }

                environment {
                    MAVEN_OPTS = '-Xmx3G'
                }

                stages {

                    stage('BuildAndTest') {
                        steps {
                            sh "mvn clean verify -Dhbase.profile=${HBASE_PROFILE} -B"
                        }
                        post {
                            always {
                                junit '**/target/surefire-reports/TEST-*.xml'
                                junit '**/target/failsafe-reports/TEST-*.xml'
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