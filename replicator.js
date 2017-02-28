var follow = require("follow");

function replicatorFactory(opts, readyCallback) {

    var config = opts;
    var db = require(`./drivers/${config.db.type}`)(config);
    var docstoprocess = [];
    var processing = false;

    var repObj = {

        listen: function() {
            /// Start following the couchdb
            var opts = {
                appenders: [
                    { type: "console" }
                ],
                replaceConsole: true
            }; // Same options paramters as before
            var feed = new follow.Feed(opts);

            // You can also set values directly.
            feed.db            = config.couchdb.serverurl;
            feed.since         = config.couchdb.sequence;
            feed.heartbeat     = 30    * 1000
            feed.inactivity_ms = 86400 * 1000;
            feed.include_docs  = true;
            

            // feed.filter = function(doc, req) {
            //   // req.query is the parameters from the _changes request and also feed.query_params.
            //   if(doc.docType === "user") {
            //     return false;
            //   }
            //   return true;
            // }

            feed.on("stop", function() {
                console.log("Feed Stopped");
            })

            feed.on('confirm', function(dbObject) {
                console.log("Confirmed:", dbObject);
            })

            feed.on("catchup", function(seq) {
                console.log("Feed has caught up to", seq);
            })

            feed.on('change', function(change) {
                // console.log("Change " + change.seq + " has " + Object.keys(change.doc).length + " fields");
                docstoprocess.push(change);
                repObj.processDocs();
            });

            feed.on('error', function(er) {
              console.error('Since Follow always retries on errors, this must be serious');
              //throw er;
            })

            feed.follow(); 
       },

       processDocs: function() {
        if(docstoprocess.length === 0 || processing) { return; }
        processing = true;
        console.log(docstoprocess.length, " docs to process");
        var changeToReplicate = docstoprocess.shift();
        var sequenceToUpdate = changeToReplicate.seq;
        db.replicateObject(changeToReplicate.doc)
            .then(function(result) {
                repObj.saveSequence(sequenceToUpdate)
                .then(function() {
                    processing = false;
                    repObj.processDocs();
                })
                .catch(function(err) {
                    processing = false;
                    console.log(err);
                    repObj.processDocs();                    
                });

            })
            .catch(function(err) {
                processing = false;
                console.log(err);
                repObj.processDocs();
        });
       },

       loadSequence: function() {
           return new Promise(function(resolve, reject) {
            if(config.db.storesequence) {
                db.loadSequence()
                .then(function(seq) { 
                    config.couchdb.sequence = seq; 
                    resolve(seq); })
                .catch(function(err) {
                    repObj.loadSequenceFromFile()
                    .then(function(seq) { config.couchdb.sequence = seq; resolve(config.couchdb.sequence); })
                    .catch(function(err) { console.log(err); resolve(true); });
                });
            } else {
                repObj.loadSequenceFromFile()
                .then(function(seq) { config.couchdb.sequence = seq; resolve(config.couchdb.sequence); })
                .catch(function(err) { resolve(); })
            }   
           });
       },

       loadSequenceFromFile: function() {
            return new Promise(function(resolve, reject) {
                var fs = require("fs"); 
                fs.exists(config.couchdb.sequencefile, function(result) {
                    if(!result) { reject("File does not exist."); return; }
                    fs.readFile(config.couchdb.sequencefile, function(err, fileContent) {
                        if(err) { reject(err); return; }
                        resolve(fileContent.toString());    
                    });
                });
                
            })           
       },

       saveSequence: function(sequence) {
        return new Promise(function(resolve, reject) {
            if(config.db.storesequence) {
                db.storeSequence(sequence)
                .then(function(seq) { 
                    resolve(seq); })
                .catch(function(err) { 
                    reject(err); 
                });
            } else {
                var fs = require("fs"); 
                fs.writeFile(config.couchdb.sequencefile, sequence, function(err) {
                    if(err) { reject(err); return; }
                    config.couchdb.sequence = fileContent; 
                    resolve(fileContent);    
                });
            }   
           });           
       }
    };

    db.init().then(repObj.loadSequence).then(readyCallback).catch(function(err) { console.error("Exiting:", err); process.exit(99) });
    return repObj;
}

module.exports = replicatorFactory;



