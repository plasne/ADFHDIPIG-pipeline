REGISTER wasbs://pelasne-pig-2017-03-09t22-26-52-136z@pelasnepigstore.blob.core.windows.net/user/plasne/output.jar;
REGISTER /usr/hdp/2.5.4.0-121/pig/lib/json-simple-1.1.jar;
REGISTER /usr/hdp/2.5.4.0-121/pig/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
in = LOAD '/$input/customer-csv' USING CSVLoader(',')
  AS (CUSTOMER_SEGMENT_ID:int, CUSTOMER_SEGMENT_DESC:chararray, CUSTOMER_ID:int, CUSTOMER_DESC:chararray,
  CUSTOMER_DEST_LOC:chararray, CUSTOMER_DEST_LOC_DESC:chararray, SALES_REP:chararray, extime:chararray,
  QTY1:chararray, VAL1:chararray, QTY2:chararray, VAL2:chararray);
transform = FOREACH in GENERATE CUSTOMER_DESC, CUSTOMER_DEST_LOC, CUSTOMER_DEST_LOC_DESC,
  CUSTOMER_ID, CUSTOMER_SEGMENT_DESC, CUSTOMER_SEGMENT_ID,
  ToString(ToDate(extime, 'yyyyMMdd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as Extraction_Time:chararray,
  SALES_REP,
  CONCAT('(', QTY1, ',', VAL1, ';', QTY2, ',', VAL2, ')') as scale:chararray;
STORE transform INTO '/$output' USING output.XML('/user/plasne/config.json');