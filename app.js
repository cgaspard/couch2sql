var config = require("./config.js");
var replicatorFactory = require("./replicator");

var application = {
  run: function() {
    var replicator = replicatorFactory(config, function(success) {
      if(!success) {
        console.log("Database is not ready, we cannot start the replicator");
        return;
      }
      console.log("Database is ready, starting the couchdb replication");
      replicator.listen();
    });
    
  },
  pause: function() {

  }
}

application.run();

