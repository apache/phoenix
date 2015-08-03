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