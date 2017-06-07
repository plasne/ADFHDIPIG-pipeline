REGISTER input.jar;
REGISTER /usr/hdp/2.5.4.0-121/pig/piggybank.jar;

raw = LOAD '/user/plasne/input' USING input.LoadCsvOrEmpty();
DUMP raw;