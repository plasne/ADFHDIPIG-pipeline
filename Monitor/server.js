
// includes
const config = require("config");
const adal = require("adal-node");
const azure = require("azure-storage");
const request = require("request");
const express = require("express");
const big = require("big-number");
const promisePool = require('es6-promise-pool');
const moment = require("moment");

// global variables
const account = config.get("account");
const key = config.get("key");
const table_instance = config.get("table_instance");
const table_debug = config.get("table_debug");
const directory = config.get("directory");
const subscriptionId = config.get("subscriptionId");
const clientId = config.get("clientId");
const clientSecret = config.get("clientSecret");
const adf_version = config.get("adf_version");

// instantiate express
const app = express();
app.use(express.static("client"));

// extend string to convert to array
String.prototype.toArrayOfStrings = function() {
    return this.split(",").map(function(code) {
        return code.trim();
    });
}

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

// redirect to default page
app.get("/", (req, res) => {
    res.redirect("/default.html");
});

// get a list of pipelines
app.get("/pipelines", (req, res) => {

    // authenticate against Azure APIs
    const context = new adal.AuthenticationContext("https://login.microsoftonline.com/" + directory);
    context.acquireTokenWithClientCredentials("https://management.core.windows.net/", clientId, clientSecret, function(err, tokenResponse) {
        if (!err) {

            const resourceGroup = "pelasne-adf";
            const dataFactory = "pelasne-adf";

            // get the pipelines
            request.get({
                uri: `https://management.azure.com/subscriptions/${subscriptionId}/resourcegroups/${resourceGroup}/providers/Microsoft.DataFactory/datafactories/${dataFactory}/datapipelines?api-version=${adf_version}`,
                headers: { Authorization: "Bearer " + tokenResponse.accessToken },
                json: true
            }, (err, response, body) => {
                if (!err && response.statusCode == 200) {
                    res.send(response.body.value);
                } else {
                    if (err) { console.error("err(201): " + err) } else { console.error("err(202) [" + response.statusCode + "]: " + response.statusMessage); console.log(body); };
                }
            });

        } else {
            res.status(500).send("err(200): Server calls could not authenticate.");
        }
    });

});

// get a list of slices
app.get("/slices", (req, res) => {
    let datasets = req.query.datasets;
    if (datasets) {
        datasets = datasets.toArrayOfStrings();

        // authenticate against Azure APIs
        const context = new adal.AuthenticationContext("https://login.microsoftonline.com/" + directory);
        context.acquireTokenWithClientCredentials("https://management.core.windows.net/", clientId, clientSecret, function(err, tokenResponse) {
            if (!err) {

                const resourceGroup = "pelasne-adf";
                const dataFactory = "pelasne-adf";

                const now = new Date("2017-05-14T00:00:00Z");
                const startTimestamp = new Date(now - 96 * 60 * 60 * 1000).toISOString(); // 96 hours back
                const endTimestamp = now.toISOString();

                const slices = [];

                // build a pool of queries to get data on all the specified datasets
                let index = 0;
                const pool = new promisePool(() => {
                    if (index < datasets.length) {
                        const dataset = datasets[index];
                        index++;
                        return new Promise((resolve, reject) => {
                            request.get({
                                uri: `https://management.azure.com/subscriptions/${subscriptionId}/resourcegroups/${resourceGroup}/providers/Microsoft.DataFactory/datafactories/${dataFactory}/datasets/${dataset}/slices?start=${startTimestamp}&end=${endTimestamp}&api-version=${adf_version}`,
                                headers: { Authorization: "Bearer " + tokenResponse.accessToken },
                                json: true
                            }, (err, response, body) => {
                                if (!err && response.statusCode == 200) {
                                    response.body.value.forEach(slice => {
                                        slice.dataset = dataset;
                                        slices.push(slice);
                                    });
                                    resolve();
                                } else {
                                    reject( err || new Error(response.body) );
                                }
                            });
                        });
                    } else {
                        return null;
                    }
                }, 4);
                
                // process the queries 4 at a time and when done send the results
                pool.start().then(() => {
                    res.send(slices);
                }, error => {
                    res.status(500).send("err(300): " + error.message);
                });

            } else {
                res.status(500).send("err(200): Server calls could not authenticate.");
            }
        });

    } else {
        res.status(400).send("err(300): You must supply a dataset parameter.");
    }
});

// get details on a slice run
app.get("/slice", (req, res) => {
    const dataset = req.query.dataset;
    const start = parseInt(req.query.start);
    if (dataset && !isNaN(start)) {
        const startTimestamp = new moment(start).utc().format("YYYY-MM-DDTHH:mm:ss") + "Z";
        console.log(startTimestamp);

        // authenticate against Azure APIs
        const context = new adal.AuthenticationContext("https://login.microsoftonline.com/" + directory);
        context.acquireTokenWithClientCredentials("https://management.core.windows.net/", clientId, clientSecret, function(err, tokenResponse) {
            if (!err) {

                const resourceGroup = "pelasne-adf";
                const dataFactory = "pelasne-adf";

                request.get({
                    uri: `https://management.azure.com/subscriptions/${subscriptionId}/resourcegroups/${resourceGroup}/providers/Microsoft.DataFactory/datafactories/${dataFactory}/datasets/${dataset}/sliceruns?start=${startTimestamp}&api-version=${adf_version}`,
                    headers: { Authorization: "Bearer " + tokenResponse.accessToken },
                    json: true
                }, (err, response, body) => {
                    if (!err && response.statusCode == 200) {
                        res.send(response.body.value);
                    } else {
                        console.log( JSON.stringify(response.body) );
                        res.status(500).send("err(400): " + (err || JSON.stringify(response.body)));
                    }
                });

            } else {
                res.status(500).send("err(200): Server calls could not authenticate.");
            }
        });

    } else {
        res.status(400).send("err(300): You must supply a dataset and start parameter.");
    }
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