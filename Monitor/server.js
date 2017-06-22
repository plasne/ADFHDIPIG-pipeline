
// includes
const config = require("config");
const azure = require("azure-storage");
const express = require("express");
const big = require("big-number");

// global variables
const account = config.get("account");
const key = config.get("key");
const table_instance = config.get("table_instance");
const table_debug = config.get("table_debug");

// instantiate express
const app = express();
app.use(express.static("client"));

// instantiate the table service
const service = azure.createTableService(account, key);

// function to create tables if they don't exist
function createTableIfNotExists(name) {
    return new Promise((resolve, reject) => {
        service.createTableIfNotExists(name, (error, result, response) => {
            if (error) reject(error);
            resolve(result);
        });
    });
}

// function to get all logs from the logs table service
function queryForAllInstances(instanceId) {
    return new Promise((resolve_all, reject_all) => {
        const entries = [];
        const query = new azure.TableQuery().where("PartitionKey eq ?", instanceId);
        let execute = (continuationToken) => new Promise((resolve, reject) => {
            service.queryEntities(table_instance, query, continuationToken, (error, result, response) => {
                if (error) {
                    reject(error);
                } else {
                    Array.prototype.push.apply(entries, result.entries);
                    if (result.continuationToken == null) {
                        resolve();
                    } else {
                        execute(result.continuationToken).then(() => resolve(), error => reject(error));
                    }
                }
            });
        });
        execute(null).then(() => resolve_all(entries), error => reject_all(error));
    });
}

// function to get all logs from the associated logs table service
function queryForAssociatedLogs(apk, low_ark, high_ark) {
    return new Promise((resolve_all, reject_all) => {
        const entries = [];
        const query = new azure.TableQuery().where("PartitionKey eq ?", apk).and("RowKey ge ?", low_ark).and("RowKey le ?", high_ark);
        let execute = (continuationToken) => new Promise((resolve, reject) => {
            service.queryEntities(table_debug, query, continuationToken, (error, result, response) => {
                if (error) {
                    reject(error);
                } else {
                    Array.prototype.push.apply(entries, result.entries);
                    if (result.continuationToken == null) {
                        resolve();
                    } else {
                        execute(result.continuationToken).then(() => resolve(), error => reject(error));
                    }
                }
            });
        });
        execute(null).then(() => resolve_all(entries), error => reject_all(error));
    });
}

// redirect to index.html
app.get("/", (req, res) => {
    res.redirect("/index.html");
});

// get instance logs
app.get("/logs", (req, res) => {
    const instanceId = req.query.instanceId;
    if (instanceId) {

        // ensure the table exists
        createTableIfNotExists(table_instance).then(result => {

            // read all instance logs
            queryForAllInstances(instanceId).then(entries => {
                const output = entries.map(entry => {
                    return {
                        index: entry.RowKey._,
                        ts: entry.Timestamp._,
                        level: entry.Level._,
                        msg: entry.Message._,
                        apk: entry.AssociatedPK._,
                        ark: entry.AssociatedRK._
                    };
                });
                res.send(output);
            }, error => {
                console.error(error);
                res.status(500).send({ message: "Failed to read logs. Please try again in a bit.", ex: error });
            });

        }, error => {
            console.error(error);
        });

    } else {
        res.send([]); // empty array of objects
    }
});

// get the associated YARN/PIG logs
app.get("/associated", (req, res) => {
    const apk = req.query.apk;
    const ark = req.query.ark;
    if (apk && ark) {

        // ensure the table exists
        createTableIfNotExists(table_debug).then(result => {

            // read all instance logs
            const low_ark = big(ark).subtract(5 * 60 * 1000).toString(); // 5 min before
            const high_ark = big(ark).add(5 * 60 * 1000).toString(); // 5 min after
            queryForAssociatedLogs(apk, low_ark, high_ark).then(entries => {
                const output = entries.map(entry => {
                    const rowKey = entry.RowKey._.split("-");
                    return {
                        ts: entry.Timestamp._,
                        level: entry.Level._,
                        msg: entry.Message._
                    };
                });
                res.send(output);
            }, error => {
                console.error(error);
                res.status(500).send({ message: "Failed to read logs. Please try again in a bit.", ex: error });
            });

        }, error => {
            console.error(error);
        });

    } else {
        res.send([]); // empty array of objects
    }
});

// startup the server
app.listen(80, () => {

/*
    const long = big(2).power(63).subtract(1);
    const current = Date.parse("2017-06-19T00:00:00.000Z");
    const s = long.subtract(current).toString();
    console.log(s);
*/

    console.log("listening on port 80...");
});