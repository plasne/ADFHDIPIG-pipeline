REGISTER input.jar;
REGISTER json-simple-1.1.jar;

raw = LOAD '/user/plasne/input' USING input.LoadCsvOrEmpty();
DUMP raw;