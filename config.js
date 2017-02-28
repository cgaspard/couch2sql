var config = {
    /// This is the targe sql db.
    "db": {
        type: "mysql",
        host:"127.0.0.1",
        port:"3306",
        tableindicator: "docType",
        user: "dbuser",
        password: "dbpassword",
        name : 'dbname',
        storesequence: true,
    },
    /// This is the source couchdb
    "couchdb": {
        hostname: "127.0.0.1",
        sequencefile: "sequence.js",
        serverurl: "http://mygait:addison123@localhost:5984/etracker",
        serverport: 5984,
        sequence: "0",
        name: "dbname",
        secure: false,
        cache: false,
        auth: {
            username: "dbuser",
            password: "dbpassword"
        }
    }
}

module.exports = config;