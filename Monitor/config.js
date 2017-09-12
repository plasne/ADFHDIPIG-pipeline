
// includes
const config = require("config");
const cmd = require("commander");

// global variables
/*
const jwtKey = config.get("external.jwtKey");
const directory = config.get("internal.directory");
const subscriptionId = config.get("internal.subscriptionId");
const clientId = config.get("internal.clientId");
const clientSecret = config.get("internal.clientSecret");
const adf_version = config.get("internal.adf_version");
*/

// @blah.onmicrosoft.com, rights, pipelines

// setup command line arguments
cmd
    .version("0.1.0")
    .option("--init", "describes any initialization tasks")
    .option("--create", "create a database")
    .option("--db <value>", "specifies the name of the database")
    .option("--populate <value>", "populate the database with a specified dataset")
    .option("--status", "gets the feed status for all coins from a running bot")
    .option("--debug", "toggles the debug status of the running bot")
    .option("--score", "scores all the models on the last 3 months of data")
    .parse(process.argv);

