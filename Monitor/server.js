
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
                if (error) reject(error);
                Array.prototype.push.apply(entries, result.entries);
                if (result.continuationToken == null) {
                    resolve();
                } else {
                    execute(result.continuationToken).then(() => resolve(), error => reject(error));
                }
            });
        });
        execute(null).then(() => resolve_all(entries), error => reject_all(error));
    });
}

// function to get all logs from the associated logs table service
function queryForAssociatedLogs(ts) {
    return new Promise((resolve_all, reject_all) => {
        const entries = [];
        const query = new azure.TableQuery().where("PartitionKey eq ?", instanceId);
        let execute = (continuationToken) => new Promise((resolve, reject) => {
            service.queryEntities(table_instance, query, continuationToken, (error, result, response) => {
                if (error) reject(error);
                Array.prototype.push.apply(entries, result.entries);
                if (result.continuationToken == null) {
                    resolve();
                } else {
                    execute(result.continuationToken).then(() => resolve(), error => reject(error));
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
                    const rowKey = entry.RowKey._.split("-");
                    return {
                        instance: rowKey[0],
                        index: rowKey[1],
                        ts: entry.Timestamp._,
                        level: entry.Level._,
                        msg: entry.Message._
                    };
                });
                console.log(output);
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
    const ts = req.query.ts;
    if (ts) {

        // ensure the table exists
        createTableIfNotExists(table_debug).then(result => {

            // read all instance logs
            queryForAssociatedLogs(ts).then(entries => {
                const output = entries.map(entry => {
                    const rowKey = entry.RowKey._.split("-");
                    return {
                        instance: rowKey[0],
                        index: rowKey[1],
                        ts: entry.Timestamp._,
                        level: entry.Level._,
                        msg: entry.Message._
                    };
                });
                console.log(output);
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

    const long = big(2).power(63).subtract(1);
    const current = Date.parse("2017-06-19T00:00:00.000Z");
    const s = long.subtract(current).toString();
    console.log(s);

    console.log("listening on port 80...");
});