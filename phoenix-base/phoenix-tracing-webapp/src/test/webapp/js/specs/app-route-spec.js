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


describe('The Tracing Web App Routes', function() {

  describe('Checking Ctrls and Partials', function() {
    // load the module
    beforeEach(module('TracingAppCtrl'));

    it('/about',
      inject(function($route) {
        expect($route.routes['/about'].templateUrl).toEqual('partials/about.html');
      }));

    it('/count-chart',
      inject(function($route) {
        expect($route.routes['/count-chart'].controller).toBe('TraceCountChartCtrl');
        expect($route.routes['/count-chart'].templateUrl).toEqual('partials/chart.html');
      }));

    it('/trace-distribution',
      inject(function($route) {
        expect($route.routes['/trace-distribution'].controller).toBe('TraceDistChartCtrl');
        expect($route.routes['/trace-distribution'].templateUrl).toEqual('partials/chart.html');
      }));

    it('/trace-timeline',
      inject(function($route) {
        expect($route.routes['/trace-timeline'].controller).toBe('TraceTimeLineCtrl');
        expect($route.routes['/trace-timeline'].templateUrl).toEqual('partials/google-chart.html');
      }));

    it('/list',
      inject(function($route) {
        expect($route.routes['/list'].controller).toBe('TraceListCtrl');
        expect($route.routes['/list'].templateUrl).toEqual('partials/list.html');
      }));

    it('/home',
      inject(function($route) {
        expect($route.routes['/home'].templateUrl).toEqual('partials/home.html');
      }));

    it('/help',
      inject(function($route) {
        expect($route.routes['/help'].templateUrl).toEqual('partials/help.html');
      }));

    it('/',
      inject(function($route) {
        expect($route.routes[null].redirectTo).toEqual('/home');
      }));

  });
});
