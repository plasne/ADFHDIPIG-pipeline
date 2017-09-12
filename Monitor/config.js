
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
    .option("--list", "list the current assignments")
    .option("--assign", "when you specify --account, --rights, --resource-group, --data-factory, and --pipelines, this will create or modify an entry")
    .option("--account <value>", "could be a fully qualified account or simply a domain")
    .option("--rights <value>", "a comma delimited list of rights that will be given to users of that domain")
    .option("--resource-group <value>", "the resource group containing the data factory")
    .option("--data-factory <value>", "the name of the data factory")
    .option("--pipelines <value>", "specifies the name of the database")
    .parse(process.argv);

// assign (create or modify an entry)
if (cmd.hasOwnProperty("assign")) {
    if (cmd.account && cmd.rights && cmd.resourceGroup && cmd.dataFactory && cmd.pipelines) {

        // instantiate the table service
        const account = config.get("storage.account");
        const key = config.get("storage.key");
        const service = azure.createTableService(account, key);

        // create the table if it doesn't exist
        const table = config.get("storage.table_customers");
        new Promise((resolve, reject) => {
            service.createTableIfNotExists(table, (error, result, response) => {
                if (!error) {
                    resolve();
                } else {
                    reject(error);
                }
            });
        }).then(result => {

            // find the specified entry
            new Promise((resolve, reject) => {
                const query = new azure.TableQuery().where("PartitionKey eq 'access' and RowKey eq ?", cmd.account);
                service.queryEntities(table, query, null, (error, result, response) => {
                    if (!error) {
                        resolve(result.entries);
                    } else {
                        reject(error);
                    }
                });
            }).then(entries => {

                // insert or replace the entry
                const entry = {
                    PartitionKey: { "_": "access" },
                    RowKey: { "_": cmd.account },
                    Rights: { "_": cmd.rights },
                    ResourceGroup: { "_": cmd.resourceGroup },
                    DataFactory: { "_": cmd.dataFactory },
                    Pipelines: { "_": cmd.pipelines }
                };
                new Promise((resolve, reject) => {
                    if (entries.length > 0) {
                        service.replaceEntity(table, entry, (error, result, response) => {
                            if (!error) { resolve(); } else { reject(error); }
                        });
                    } else {
                        service.insertEntity(table, entry, (error, result, response) => {
                            if (!error) { resolve(); } else { reject(error); }
                        });
                    }
                }).then(() => {
                    console.log("Successfully assigned.");
                }, error => {
                    console.error(`Insert/update: ${error}`);
                });

            }, error => {
                console.error(`Find existing entries: ${error}`);
            });
        
        }, error => {
            console.error(`Create the table: ${error}`);
        });

    } else {
        console.error("You must specify --account, --rights, --resource-group, --data-factory, --pipelines.");
    }
}

// list all assignments
if (cmd.hasOwnProperty("list")) {

    // instantiate the table service
    const account = config.get("storage.account");
    const key = config.get("storage.key");
    const service = azure.createTableService(account, key);

    // return all rows
    const table = config.get("storage.table_customers");
    new Promise((resolve_all, reject_all) => {
        const entries = [];
        const query = new azure.TableQuery().where("PartitionKey eq 'access'");
        let execute = (continuationToken) => new Promise((resolve, reject) => {
            service.queryEntities(table, query, continuationToken, (error, result, response) => {
                if (!error) {
                    Array.prototype.push.apply(entries, result.entries);
                    if (result.continuationToken == null) {
                        resolve();
                    } else {
                        execute(result.continuationToken).then(() => resolve(), error => reject(error));
                    }
                } else {
                    reject(error);
                }
            });
        });
        execute(null).then(() => resolve_all(entries), error => reject_all(error));
    }).then(entries => {

        // deserialize
        const rows = entries.map(entry => {
            return {
                account: entry.RowKey._,
                rights: entry.Rights._,
                resourceGroup: entry.ResourceGroup._,
                dataFactory: entry.DataFactory._,
                pipelines: entry.Pipelines._
            };
        });

        // display
        console.log("Account                                Rights                       Resource Group     Data Factory       Pipelines");
        console.log("-------                                ------                       --------------     ------------       ---------");
        for (let row of rows) {
            const a = (row.account + "                                        ").substring(0, 39);
            const b = (row.rights + "                              ").substring(0, 29);
            const c = (row.resourceGroup + "                    ").substring(0, 19);
            const d = (row.dataFactory + "                    ").substring(0, 19);
            const e = (row.pipelines).substring(0, 29);
            console.log(`${a}${b}${c}${d}${e}`);
        }

    }, error => {
        console.error(`Read the table: ${error}`);
    });

}