'use strict';


describe('TracingCtrl', function() {

  beforeEach(module('TracingCtrl'));
  beforeEach(inject(function($rootScope, $controller) {
    scope = $rootScope.$new();
    traceListCtrl = $controller('TraceListCtrl', {
      $scope: scope
    });
  }));


  beforeEach(inject(function($injector) {
    // Set up the mock http service responses
    $httpBackend = $injector.get('$httpBackend');
    // backend definition common for trace of phoenix
    authRequestHandler = $httpBackend.when('GET', '../trace?action=getall&limit=100')
      .respond([{
        "start_time": 1438582622482,
        "trace_id": -9223359832482284828,
        "hostname": "pc",
        "span_id": -876665211183522462,
        "parent_id": -4694507801564472011,
        "end_time": 1438582622483,
        "count": 0,
        "description": "Committing mutations to tables"
      }]);
    // Get hold of a scope (i.e. the root scope)
    $rootScope = $injector.get('$rootScope');
    // The $controller service is used to create instances of controllers
    var $controller = $injector.get('$controller');

    createController = function() {
      return $controller('TraceListCtrl', {
        '$scope': $rootScope
      });
    };
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

  it('changing traces limit value', function() {
    scope.tracesLimit = 25;
    expect(scope.tracesLimit).toBe(25);
    scope.tracesLimit = 124;
    expect(scope.tracesLimit).toBe(124);
  });

  it('should fetch trace from phoenix', function() {
    $httpBackend.expectGET('../trace?action=getall&limit=100');
    var controller = createController();
    $httpBackend.flush();
  });

  it('updating trace list after retriving phoenix trace', function() {
    var controller = createController();
    $httpBackend.flush();
    expect($rootScope.traces).toBeDefined();
    expect($rootScope.traces.length).toBe(1);
  });

  it('checking trace list attributes of phoenix trace', function() {
    var controller = createController();
    $httpBackend.flush();
    expect($rootScope.traces[0].start_time).toBe(1438582622482);
    expect($rootScope.traces[0].trace_id).toBe(-9223359832482284828);
    expect($rootScope.traces[0].hostname).toBe('pc');
    expect($rootScope.traces[0].span_id).toBe(-876665211183522462);
    expect($rootScope.traces[0].parent_id).toBe(-4694507801564472011);
    expect($rootScope.traces[0].end_time).toBe(1438582622483);
    expect($rootScope.traces[0].count).toBe(0);
    expect($rootScope.traces[0].description).toBe('Committing mutations to tables');
  });

});