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