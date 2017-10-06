
// includes
const config = require("config");
const cmd = require("commander");
const moment = require("moment");
const fs = require("fs");
const adal = require("adal-node");
const request = require("request");

// options
cmd
    .version("0.1.0")
    .option("-s, --subscription <value>", "The Azure Subscription ID of the subscription containing ADF.")
    .option("-r, --resource-group <value>", "The name of the Resource Group containing ADF.")
    .option("-d, --data-factory <value>", "The name of the ADF instance.")
    .option("-f, --since <value>", "A timestamp; files after this time will be considered for upgrade.");

// parse the command line
//   NOTE: you may specify one or more paths after the options
cmd.parse(process.argv);
if (cmd.args.length < 1) cmd.args.push(".");

// carve up the files into buckets
//   NOTE: files must end in .json and must have "ls", "ds", or "pl" as the 3rd of a - separated filename
function getFileCollections() {

    // variables
    const col = {
        ls: [],
        ds: [],
        pl: []
    };
    const since = moment(cmd.since);

    // files the files in the paths
    try {
        const promises = [];
        for (let path of cmd.args) {
            const promise = new Promise((resolve, reject) => {
                fs.readdir(path, (error, files) => {
                    if (!error) {
                        for (let file of files) {
                            const filename = file.toLowerCase();
                            if (filename.endsWith(".json")) {                       // ends in .json
                                const filenameparts = filename.split("-");
                                if (filenameparts.length > 2) {                     // > 2 parts sep by -
                                    const full = `${path}/${filename}`;
                                    fs.stat(full, (error, stats) => {
                                        if (!error) {
                                            if (stats.mtimeMs >= since.valueOf()) { // >= since
                                                switch (filenameparts[3]) {
                                                    case "ls":                      // 3rd part is ls
                                                        col.ls.push(full);
                                                        break;
                                                    case "ds":                      // 3rd part is ds
                                                        col.ds.push(full);
                                                        break;
                                                    case "pl":                      // 3rd part is pl
                                                        col.pl.push(full);
                                                        break;
                                                }
                                            }
                                        } else {
                                            reject(error);
                                        }
                                    });
                                }
                            }
                        }
                        resolve();
                    } else {
                        reject(error);
                    }
                });
            });
            promises.push(promise);
        }
        return Promise.all(promises).then(_ => Promise.resolve(col));
    } catch (ex) {
        return Promise.reject(ex);
    }

}

// get an access token for Azure
function getToken() {
    return new Promise((resolve, reject) => {

        // variables
        const authority = config.get("authority");
        const directory = config.get("directory");
        const clientId = config.get("clientId");
        const clientSecret = config.get("clientSecret");
        
        // query for the token
        const context = new adal.AuthenticationContext(authority + directory);
        context.acquireTokenWithClientCredentials("https://management.azure.com/", clientId, clientSecret, (error, tokenResponse) => {
            if (!error) {
                resolve(tokenResponse.accessToken);
            } else {
                reject(error);
            }
        });

    });
}

// get the contents of the file as a JSON object
function getFile(filename) {
    return new Promise((resolve, reject) => {
        try {
            fs.readFile(filename, (error, data) => {
                if (!error) {
                    resolve(JSON.parse(data));
                } else {
                    reject(error);
                }
            });
        } catch (ex) {
            reject(ex);
        }
    });
}

// update the file on the server
function updateFile(type, name, body, token) {
    return new Promise((resolve, reject) => {

        // variables
        const version = config.get("version");

        // put the object
        request({
            method: "PUT",
            uri: `https://management.azure.com/subscriptions/${cmd.subscription}/resourcegroups/${cmd.resourceGroup}/providers/Microsoft.DataFactory/datafactories/${cmd.dataFactory}/${type}/${name}?api-version=${version}`,
            headers: {
                Authorization: `Bearer ${token}`
            },
            body: body,
            json: true
        }, (error, response, body) => {
            if (!error && response.statusCode >= 200 && response.statusCode <= 299) {
                resolve();
            } else if (error) {
                reject(error);
            } else {
                reject(new Error(`${response.statusCode}: ${response.statusMessage}`));
            }
        });

    });
}

// update
if (cmd.subscription && cmd.resourceGroup && cmd.dataFactory && cmd.since) {
    if (moment(cmd.since).isValid()) {
        (async _ => {
            try {

                // get the collection of files
                const col = await getFileCollections();
    
                // get an access token
                const token = await getToken();

                // upload all linkedServices
                for (let filename of col.ls) {
                    const contents = await getFile(filename);
                    console.log(`uploading linked service as ${contents.name} from ${filename}...`);
                    await updateFile("linkedservices", contents.name, contents, token);
                    console.log("updated successfully.");
                }
    
                // upload all datasets
                for (let filename of col.ds) {
                    const contents = await getFile(filename);
                    console.log(`uploading dataset as ${contents.name} from ${filename}...`);
                    await updateFile("datasets", contents.name, contents, token);
                    console.log("updated successfully.");
                }

                // upload all pipelines
                for (let filename of col.pl) {
                    const contents = await getFile(filename);
                    console.log(`uploading pipeline as ${contents.name} from ${filename}...`);
                    await updateFile("datapipelines", contents.name, contents, token);
                    console.log("updated successfully.");
                }

            } catch (ex) {
                console.error(ex);
            }
        })();
    } else {
        console.error("You must enter a valid date/time for --since, ex. 2017-01-01 or 2017-01-01T18:00");
    }
} else {
    console.error("You must specify all options.");
}
