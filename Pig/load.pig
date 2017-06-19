REGISTER lib/input.jar;
REGISTER lib/json-simple-1.1.jar;
REGISTER /usr/local/customize/azure-api-0.4.4.jar;
REGISTER /usr/hdp/2.5.4.0-121/pig/piggybank.jar;

raw = LOAD '/user/plasne' USING input.LoadCsvOrEmpty('input', '/user/plasne/validate.json');
DUMP raw;
DESCRIBE raw;