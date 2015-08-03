'use strict';


describe('TracingCtrl', function(){

  beforeEach(module('TracingCtrl'));
beforeEach(inject(function($rootScope, $controller){
  scope=$rootScope.$new();
  traceListCtrl = $controller('TraceListCtrl', {
    $scope: scope
  });
}));

  it('should to be defined', function() {
    expect(scope).toBeDefined();
  });

  it('scope level variable should to be defined', function() {
    expect(scope.tracesLimit).toBeDefined();
    expect(scope.traces).toBeDefined();
    expect(scope.loadTrace).toBeDefined();
  });

    it('tracesLimit value', function() {
    expect(scope.tracesLimit).toBe(100);
  });
});