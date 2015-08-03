'use strict';


//beforeEach(module('TracingAppCtrl'));
describe('The Tracing Web App ', function() {

  describe("Testing TestSpecs", function() {
    it("contains spec with an expectation", function() {
      expect(true).toBe(true);
    });

    it("checking the variables and `undefined`", function() {
      var trace = {
        tabel: 'SYSTEM.TRACING_STATS'
      };
      expect(trace.tabel).toBeDefined();
      expect(trace.system).not.toBeDefined();
    });

  });

  describe('Controller: TracingAppCtrl', function() {

    // load the controller's module
    beforeEach(module('TracingAppCtrl'));

    describe('Testing Routes', function () {

// load the controller's module
//beforeEach(module('TracingAppCtrl'));

it('should test routes',
inject(function ($route) {

  //expect($route.routes['/about'].controller).toBe();
  expect($route.routes['/about'].templateUrl).toEqual('partials/about.html');
  //expect($route.routes[null].redirectTo).toEqual('/');
}));

});

    //TODO - Write tets for controller in TracingApp
    var TraceListCtrl, scope;
    it('Chart Controller should to be defined', function() {
      expect(TraceListCtrl).toBeUndefined();
    });
  });




});