var sqliteReplicator =  function(opts, readyCallBack) {
    var options = opts;

    var sqlite      = require('sqlite');
    var connection = sqlite.createConnection(options.db);

    connection.connect();

    connection.on("error", function(err) {
        console.log("sqlite connection error:", err);
    });
    
    var sqls = {
        loadDatabasesIntoMemory: "show databases;",
        gettablenames: "show tables;",
        getcolumnnamesfortable: "show columns from {0} in {1};",
        /// Field, Type, Null, Key, Default, Extra
        // createtable: "CREATE TABLE {0} (name VARCHAR(20), owner VARCHAR(20));"
        createtable: "CREATE TABLE {0} ({1});",
        createsequencetable: `CREATE TABLE IF NOT EXISTS \`rep_sequence\` (\`id\` INT(10) NOT NULL PRIMARY KEY, \`sequence\` VARCHAR(512)) ENGINE=MEMORY;`,
        getsequence: "SELECT sequence from rep_sequence limit 1;",
        updatesequence: "REPLACE INTO rep_sequence VALUES (1, {0})"
    }
    var databases = [];
    var tables = {};

    /// Query the database so we can get all the columns for a particular table
    function loadTableIntoMemory(jsonObject) {
        return new Promise(function(resolve, reject) {
            var tableName = jsonObject[options.db.tableindicator]
            var sql = sqls.getcolumnnamesfortable.replace("{0}", tableName).replace("{1}", options.db.name);
            connection.query(sql, function(err, result) {
                if(err) {
                    /// table doesnt exist create it
                    if(err.message.indexOf("ER_NO_SUCH_TABLE" > -1)) {
                        reject("Table doesnt exist");
                        return;
                    } else {
                        reject(err);
                        return;
                    }
                }
                result.forEach(function(row) {
                    if(row.Type.toLowerCase().indexOf("varchar") > -1) {
                        row.Size = row.Type.match(/\((.*)\)/)[1];
                    } else if(row.Type.toLowerCase().indexOf("longtext") > -1) {
                        row.Size = 4294967295;
                    } else if(row.Type.toLowerCase().indexOf("medium") > -1) {
                        row.Size = 16777215;
                    } else if(row.Type.toLowerCase().indexOf("text") > -1) {
                        row.Size = 65535;
                    } else {
                        row.Size = 0;
                    }
                })
                tables[tableName] = result;
                resolve(tables[tableName]);
            });
        });
    }

    async function createTable(jsonObject) {
        var tableName = jsonObject[options.db.tableindicator]
        var createTableSQL = sqls.createtable.replace("{0}", "`" + tableName + "`");
        var fieldsString = "";
        for(var prop in jsonObject) {
            /// for each field in the object, create a sql field for it
            var fieldAddition = getCreateFieldString(prop.toLowerCase(), jsonObject[prop]);
            if(prop === "_id") {
                fieldAddition += " NOT NULL PRIMARY KEY";
            }
            if(fieldAddition != "") {
                fieldsString += fieldAddition + ", ";
            }
        }
        fieldsString =fieldsString.replace(new RegExp(", " + '$'), '');
        createTableSQL = createTableSQL.replace("{1}", fieldsString);
        await executeSQL(createTableSQL);
        await loadTableIntoMemory(jsonObject);
        return jsonObject;
    }

    /// Check to see if our database exists
    function databaseExists(dbName) {
        return new Promise(function(resolve, reject) {
            if(databases.indexOf(dbName) > -1) {
                resolve(true);
            }
            resolve(false);
        });
    }

    /// Load a list of our databases
    function loadDatabasesIntoMemory() {
        return new Promise(function(resolve, reject) {
            connection.query('show databases;', function (error, results, fields) {
                if (error) { 
                    reject(error); 
                    console.error(error);
                    return; 
                }
                // console.log('The solution is: ', results);
                var foundDBs = [];
                results.forEach(function(row) {
                    foundDBs.push(row.Database);
                    resolve(foundDBs);
                });
            });

        });
    }

    /// Let sqlite know what database we're workig with
    async function useDatabase(dbName) {
        return new Promise(function(resolve, reject){
            connection.end();
            options.db.database = dbName;
            connection = sqlite.createConnection(options.db);
            connection.connect(function(err) {
                if(err) { reject(err); return; }
                resolve(true);
            });
        });
    }

    /// Create the database
    function createDatabase(dbName) {
        return new Promise(function(resolve, reject) {
            connection.query("create database " + dbName, function(err, result) {
                if(err) { reject(err); return; }
                resolve(dbName);
            });
        });
    }

    function isTableInMemory(tableName) {
        return new Promise(function(resolve, reject) {
            if(tables.indexOf(gettablenames) > -1) {
                resolve(true);
            }
            resolve(false);
        });
    }

    function getColumnForProperty(tableName, columnName) {
        var table = tables[tableName];
        for(var i = 0; i < table.length; i++) {
            var row = table[i];
            if(row.Field.toLowerCase() === columnName.toLowerCase()) {
                return row;
            }
        }
        return null;
    }

    function isColumnToSmall(value, columnRow) {
        var valueLength = getObjectSize(value);
        if(columnRow.Size < valueLength) {
            return true;
        }
        return false;
    }
    
    async function alterTable(jsonObject) {
        var tableName = jsonObject[options.db.tableindicator];
        for(var prop in jsonObject) {
            var columnRow = getColumnForProperty(tableName, prop);
            if(columnRow !== null) {
                /// Only modify the column if we need to adjust its size
                /// or if columnRow is null, then we need to add it
                if(!isColumnToSmall(jsonObject[prop], columnRow)) { continue; }
            }
            var fieldName = "";
            var modStatement = "MODIFY";
            if(columnRow === null) {
                modStatement = "ADD COLUMN";
                fieldName = prop.toLowerCase();
            } else {
                fieldName = columnRow.Field.toLowerCase();
            }
            var fieldCreateString = getCreateFieldString(prop, jsonObject[prop]);

            var sql = "";
            // if(jsonObject[prop])) {
            //     case "string": {
            sql = `ALTER TABLE ${tableName} ${modStatement} ${fieldCreateString}`;
            console.log("ALTER:", sql);
            await executeSQL(sql);

            //         break;
            //     }                
            // }
        }
        return loadTableIntoMemory(jsonObject);
    }

    function createTableIfNotExists(jsonObject) {
        return new Promise(function(resolve, reject) {
            var tableName = jsonObject[options.db.tableindicator];
            if(tables[tableName] !== undefined) {
                /// Table Already exists
                /// TODO: handle any alters we might need to make
                resolve(jsonObject);
            } else {
                loadTableIntoMemory(jsonObject).then(function(tableObj) {
                    resolve(jsonObject);
                }).catch(function(err) {
                    /// If we errored loading the table, then we need to create it.
                    createTable(jsonObject).then(function(jsonObject){
                        resolve(jsonObject);
                    }).catch(function(err) {
                        reject(err);
                    });
                });
            }
        })
    }
    function executeSQL(sql) {
        return new Promise(function(resolve, reject) {
            if(sql === undefined) { resolve(true); }
            
            connection.query(sql,function(err, results, fields) {
                if(err) {
                    console.error("Error SQL:", sql);
                    reject(err);
                    return;
                }
                resolve({results: results, fields: fields});
            });
        });
    }

    function getCreateFieldString(name, value) {
        name = name.toLowerCase();
        var size = getObjectSize(value);
        if(value === null || value === undefined) {
            return `\`${name}\` varchar(${size})`;
        } else if(size > 21845 && size < 65535) {
            return `\`${name}\` text`;
        } else if (size > 65535 && size < 16777215) {
            return `\`${name}\` mediumtext`;
        } else if (size > 16777215 && size < 4294967295) {
            return `\`${name}\` longtext`;
        } else {
            return `\`${name}\` varchar(${size})`;
        }
    }

    function getDatabaseValueForObject(obj) {
        if(obj === undefined || obj === null) {
            return null;
        } else if(typeof(obj) === "object") {
            return JSON.stringify(obj);
        } else {
            return obj.toString();
        }        
    }

    function getObjectSize(obj) {
        if(obj === null || obj === undefined) {
            return 0;
        } else if(typeof(obj) === "object") {
            return JSON.stringify(getDatabaseValueForObject(obj)).length;
        } else {
            return getDatabaseValueForObject(obj).length;
        }        
    }

    function getColumnValueFromObject(object, propname) {
        for(var prop in object) {
            if(propname.toLowerCase() === prop.toLowerCase()) {
                return getDatabaseValueForObject(object[prop]);
            }
        }
    }

    async function getInsertOrUpdateSQL(jsonObject) {
        await alterTable(jsonObject);
        var tableName = jsonObject[options.db.tableindicator]
        var table = tables[tableName];
        var sql = `REPLACE INTO ${tableName} VALUES ({0})`;
        var valueString = "";
        for(var i = 0; i < table.length; i++) {
            var columnName = table[i].Field;
            var columnType = table[i].Type;
            var columnSize = table[i].Size;
            var value = getColumnValueFromObject(jsonObject, columnName);
            valueString += sqlite.escape(value);
            if(i < (table.length - 1)) {
                valueString += ',';
            }
        }
        return sql.replace("{0}", valueString);
    }

    return {
        init: async function() {
            try {
                /// Verify our databae exists, and create it if not
                var dbList = await loadDatabasesIntoMemory();
                databases = dbList;
                var foundDB = dbList.indexOf(options.db.name) > -1;
                if(!foundDB) {
                    var dbCreated = await createDatabase(options.db.name);
                    if(!dbCreated) {
                        throw "Unable to create database";
                    }
                }
                var usingDB = await useDatabase(options.db.name);
                return true;
            } catch(err) {
                console.error(err);
            }
        },

        replicateObject: async function(jsonObject) {
            try {
                if(jsonObject[options.db.tableindicator] !== undefined) {
                    await createTableIfNotExists(jsonObject);
                    await loadTableIntoMemory(jsonObject);
                    var executeSQLStatement = await getInsertOrUpdateSQL(jsonObject);
                    var result = await executeSQL(executeSQLStatement);
                    return true;
                } else {
                    throw "Object does not have the table indicator field.";
                }
            } catch(ex) {
                console.error(ex);
                return false;
            }
        },

        storeSequence: async function(sequence) {
            return new Promise(function(resolve, reject) {
                connection.query(sqls.updatesequence.replace("{0}", sqlite.escape(sequence)), function(err) {
                    if(err) { reject (err); return; }
                    resolve(true);
                })

            });
        },

        loadSequence: async function() {
            return new Promise(function(resolve, reject) {
                connection.query(sqls.createsequencetable, function(err) {
                    if(err) { reject (err); return; }
                    connection.query(sqls.getsequence, function(err, result) {
                        if(result.length > 0) {
                            resolve(result[0].sequence)
                        } else {
                            reject("No sequence to load");
                        }
                    })
                })
            });
        }
    }
}

module.exports = sqliteReplicator;