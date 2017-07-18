REGISTER wasbs://pelasne-pig-2017-03-09t22-26-52-136z@pelasnepigstore.blob.core.windows.net/user/plasne/input.jar;
REGISTER wasbs://pelasne-pig-2017-03-09t22-26-52-136z@pelasnepigstore.blob.core.windows.net/user/plasne/output.jar;
REGISTER /usr/local/customize/azure-api-0.4.4.jar;
REGISTER /usr/hdp/2.6.0.10-29/pig/lib/json-simple-1.1.jar;
REGISTER /usr/hdp/2.6.0.10-29/pig/piggybank.jar;

in = LOAD '$storage/$root' USING input.LoadCsvOrEmpty('$input', '$input/customer-csv', 'empty', '/user/plasne/validate.json');
DESCRIBE in;

transform = FOREACH in GENERATE CUSTOMER_DESC, CUSTOMER_DEST_LOC, CUSTOMER_DEST_LOC_DESC,
  CUSTOMER_ID, CUSTOMER_SEGMENT_DESC, CUSTOMER_SEGMENT_ID,
  ToString(ToDate(extime, 'yyyyMMdd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as Extraction_Time:chararray,
  SALES_REP,
  CONCAT('(', QTY1, ',', VAL1, ';', QTY2, ',', VAL2, ')') as scale:chararray;

STORE transform INTO '$storage/$root/$output' USING output.XML('/user/plasne/config.json');