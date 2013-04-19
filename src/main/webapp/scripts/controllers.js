simpleApp.controller('MainCtrl', function ($scope, $window, $location) {
    $scope.$location = $location;
});

simpleApp.controller('simpleController', function ($scope, userService) {
    $scope.customers = [];

    init();

    function init() {
        //$scope.customers = simpleFactory.getCustomers();
        $scope.customers = userService.query({ verb: 'find', name: '*' });
    }
    $scope.addCustomer = function () {
        //var user = new userService({ Id: '0123' });
        var user = new userService();
        user.name = $scope.newCustomer.name;
        user.city = $scope.newCustomer.city;
        user.$save(); 
        $scope.customers.push({
            name: $scope.newCustomer.name,
            city: $scope.newCustomer.city
        });
    };
});

function AlertDemoCtrl($scope) {
    $scope.alerts = [
    { type: 'error', msg: 'Oh snap! Change a few things up and try submitting again.' },
    { type: 'success', msg: 'Well done! You successfully read this important alert message.' }
  ];

    $scope.addAlert = function () {
        $scope.alerts.push({ msg: "Another alert!" });
    };

    $scope.closeAlert = function (index) {
        $scope.alerts.splice(index, 1);
    };

}

/*
$(function(){
    $('.datepicker').datepicker();
});
*/