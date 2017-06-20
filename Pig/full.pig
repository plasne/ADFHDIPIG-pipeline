REGISTER lib/input.jar;
REGISTER lib/output.jar;
REGISTER lib/json-simple-1.1.jar;
REGISTER /usr/local/customize/azure-api-0.4.4.jar;
REGISTER /usr/hdp/2.5.4.0-121/pig/piggybank.jar;

in = LOAD '/user/plasne' USING input.LoadCsvOrEmpty('customer-20170620T1100', 'input', 'empty', '/user/plasne/validate.json');
DESCRIBE in;

transform = FOREACH in GENERATE CUSTOMER_DESC, CUSTOMER_DEST_LOC, CUSTOMER_DEST_LOC_DESC,
  CUSTOMER_ID, CUSTOMER_SEGMENT_DESC, CUSTOMER_SEGMENT_ID,
  ToString(ToDate(extime, 'yyyyMMdd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as Extraction_Time:chararray,
  SALES_REP,
  CONCAT('(', QTY1, ',', VAL1, ';', QTY2, ',', VAL2, ')') as scale:chararray;

STORE transform INTO '/user/plasne/output' USING output.XML('/user/plasne/config.json');