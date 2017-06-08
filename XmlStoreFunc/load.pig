REGISTER input.jar;
REGISTER /usr/hdp/2.5.4.0-121/pig/piggybank.jar;

raw = LOAD '/user/plasne' USING input.LoadCsvOrEmpty('input', '3')
    AS (id:int, v:int, cost:double, account:int, routing:int);
DUMP raw;