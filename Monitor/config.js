
// includes
const config = require("config");
const cmd = require("commander");
const azure = require("azure-storage");

// global variables
/*
const jwtKey = config.get("external.jwtKey");
const directory = config.get("internal.directory");
const subscriptionId = config.get("internal.subscriptionId");
const clientId = config.get("internal.clientId");
const clientSecret = config.get("internal.clientSecret");
const adf_version = config.get("internal.adf_version");
*/

// setup command line arguments
cmd
    .version("0.1.0")
    .option("--assign", "when you specify domain, rights, and pipelines, this will create or modify an entry")
    .option("--account <value>", "could be a fully qualified account or simply a domain")
    .option("--rights <value>", "a comma delimited list of rights that will be given to users of that domain")
    .option("--pipelines <value>", "specifies the name of the database")
    .parse(process.argv);

// assign (create or modify an entry)
if (cmd.hasOwnProperty("assign")) {

    // instantiate the table service
    const account = config.get("storage.account");
    const key = config.get("storage.key");
    const service = azure.createTableService(account, key);

    // create the table if it doesn't exist
    const table = config.get("storage.table_customers");
    new Promise((resolve, reject) => {
        service.createTableIfNotExists(name, (error, result, response) => {
            if (error) reject(error);
            resolve(result);
        });
    }).then(result => {

        // find the specified entry
        new Promise((resolve_all, reject_all) => {
            const query = new azure.TableQuery().where("PartitionKey eq ?", cmd.account);
            new Promise((resolve, reject) => {
                service.queryEntities(table, query, null, (error, result, response) => {
                    if (!error) {
                        resolve(result.entries);
                    } else {
                        reject(error);
                    }
                });
            });
        });
    

        // read all instance logs
        queryForAllInstances(service, instanceId).then(entries => {
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

            const file = new stream.Readable();
            file.push("index,timestamp,level,message\n");
            output.forEach(line => {
                file.push(`${line.index},${line.ts},${line.level},"${line.msg}"\n`);
            });
            file.push(null);
            res.writeHead(200, {
                "Content-Type": "application/csv",
                "Content-Disposition": "attachment; filename=\"" + instanceId + ".csv\""
            });
            file.pipe(res);

        }, error => {
            console.error(error);
            res.status(500).send({ message: "Failed to read logs. Please try again in a bit.", ex: error });
        });

    }, error => {
        console.error(error);
    });


}