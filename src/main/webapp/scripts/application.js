var simpleApp = angular.module('simpleApp', ['ui.bootstrap', 'ngResource', '$strap.directives']);

simpleApp.config(function ($routeProvider) {
    $routeProvider
		.when('/view1', {
		    controller: 'simpleController',
		    templateUrl: 'views/view1.html'
		})
		.when('/view2', {
		    controller: 'simpleController',
		    templateUrl: 'views/view2.html'
		})
		.when('/view3', {
		    controller: 'simpleController',
		    templateUrl: 'views/view3.html'
		})
		.otherwise({ redirectTo: '/view1' })
});

simpleApp.factory('userService', function ($resource) {
    return $resource('api/users/:verb/:name', {}, {});
});

function NavBarCtrl($scope) {
    $scope.tabs = [
        { "view": "/view1", title: "View #1" },
        { "view": "/view2", title: "View #2" },
        { "view": "/view3", title: "Test View" }
    ];
}

