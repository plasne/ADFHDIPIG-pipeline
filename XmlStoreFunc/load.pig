REGISTER input.jar;
REGISTER json-simple-1.1.jar;

raw = LOAD '/user/plasne/CUSTOMER.201705041622.csv' USING input.LoadCsvOrEmpty;
