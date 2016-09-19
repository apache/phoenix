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


//beforeEach(module('TracingAppCtrl'));
describe('The Tracing Web App ', function() {

  describe('Controller: TracingAppCtrl', function() {

    // load the controller's module
    beforeEach(module('TracingAppCtrl'));


    //testing all the main controllers in app
    var TraceListCtrl, scope;
    it('Controllers should to be defined', function() {
      expect(TraceCtrl).toBeDefined();
      expect(UICtrl).toBeDefined();
      expect(TimeLineCtrl).toBeDefined();
      expect(SearchCtrl).toBeDefined();
    });
  });

});
