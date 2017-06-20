REGISTER lib/input.jar;
REGISTER lib/output.jar;
REGISTER lib/json-simple-1.1.jar;
REGISTER /usr/local/customize/azure-api-0.4.4.jar;
REGISTER /usr/hdp/2.5.4.0-121/pig/piggybank.jar;

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

in = LOAD '/user/plasne/input' USING input.LoadCsvOrEmpty('customer-20170620T1100', 'input', 'empty', '/user/plasne/validate.json')
  AS (CUSTOMER_SEGMENT_ID:int, CUSTOMER_SEGMENT_DESC:chararray, CUSTOMER_ID:int, CUSTOMER_DESC:chararray,
  CUSTOMER_DEST_LOC:chararray, CUSTOMER_DEST_LOC_DESC:chararray, SALES_REP:chararray, extime:chararray,
  QTY1:chararray, VAL1:chararray, QTY2:chararray, VAL2:chararray);

DESCRIBE in;

transform = FOREACH in GENERATE CUSTOMER_DESC, CUSTOMER_DEST_LOC, CUSTOMER_DEST_LOC_DESC,
  CUSTOMER_ID, CUSTOMER_SEGMENT_DESC, CUSTOMER_SEGMENT_ID,
  ToString(ToDate(extime, 'yyyyMMdd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as Extraction_Time:chararray,
  SALES_REP,
  CONCAT('(', QTY1, ',', VAL1, ';', QTY2, ',', VAL2, ')') as scale:chararray;

STORE transform INTO '/user/plasne/output' USING output.XML('/user/plasne/config.json');