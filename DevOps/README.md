# DevOps Scripts

This section contains a collection of devops scripts that can help with the deployment or maintenance of solutions.

## update.ps1

The [update.ps1](update.ps1) script is a PowerShell script that:

* looks at all .json files in the current directory, and...
* if they are equal to or later than the specified timestamp, and...
* if they have at least 3 parts to their filename separated by -, and...
* the 3rd part is one of the following: "ls", "ds", or "pl", then...
* the files will be updated in the specified Azure Data Factory.

The parameters are:

* SubscriptionId - The Azure Subscription ID of the subscription containing the ADF.
* ResourceGroup - The name of the Resource Group containing the ADF.
* DataFactory - The name of the Azure Data Factory.
* Since - A timestamp; anything equal to or after this timestamp will be considered for update.

An example:

```bash
.\update.ps1 -SubscriptionId 11111111-1111-1111-1111-1111111111111 -ResourceGroup myrg -DataFactory myadf -Since (Get-Date).AddDays(-3)
```

## update.js

This [update.js](update.js) script is a Node.js script that does the exact same thing as the above PowerShell script. This is provided in case you need to run on a non-Windows system. There is additional setup required for this solution, so if you can use PowerShell, you should.

The parameters are:

```
-V, --version                 output the version number
-s, --subscription <value>    The Azure Subscription ID of the subscription containing ADF.
-r, --resource-group <value>  The name of the Resource Group containing ADF.
-d, --data-factory <value>    The name of the ADF instance.
-f, --since <value>           A timestamp; files after this time will be considered for upgrade.
-h, --help                    output usage information
```

An example:

```bash
node update.js --subscription 11111111-1111-1111-1111-1111111111111 --resource-group myrg --data-factory myadf --since 2017-01-01T18:00 .
```