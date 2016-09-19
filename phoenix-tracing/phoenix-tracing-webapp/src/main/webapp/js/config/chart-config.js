/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

angular.module('TimeLineCtrl')
  .constant('chartModels', {
      'timeLineModel': {
        "type": "Timeline",
        "displayed": true,
        "data": {
          "cols": [{
            "id": "TraceID",
            "label": "TraceID",
            "type": "string",
            "p": {}
          }, {
            "id": "Label",
            "label": "Label",
            "type": "string",
            "p": {}
          }, {
            "id": "Start",
            "label": "Start",
            "type": "date",
            "p": {}
          }, {
            "id": "End",
            "label": "End",
            "type": "date",
            "p": {}
          }],
          "rows": [{
            "c": [{
              "v": "Trace 01"
            }, {
              "v": "Writing mutation batch for table: MY_TABLE1"
            }, {
              "v": new Date(1434196101623)
            }, {
              "v": new Date(1434196101784)
            }]
          }]
        },
        "options": {},
        "formatters": {}
      }
    });
