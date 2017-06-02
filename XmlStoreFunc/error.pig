REGISTER output.jar;
REGISTER json-simple-1.1.jar;
REGISTER /usr/hdp/2.5.4.0-121/pig/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- ensure there is at least one product file
sh echo "-1,,,,,,,,,,," | hdfs dfs -put - /user/plasne/input-201705120930/product-csv/empty.csv

-- load all product CSVs
raw_products = LOAD '/user/plasne/input-201705120930/product-csv' USING CSVLoader(',')
  AS (CUSTOMER_SEGMENT_ID:int, CUSTOMER_SEGMENT_DESC:chararray, CUSTOMER_ID:int, CUSTOMER_DESC:chararray,
  CUSTOMER_DEST_LOC:chararray, CUSTOMER_DEST_LOC_DESC:chararray, SALES_REP:chararray, extime:chararray,
  QTY1:chararray, VAL1:chararray, QTY2:chararray, VAL2:chararray);

-- remove any fake rows
SPLIT raw_products INTO in_products IF CUSTOMER_SEGMENT_ID >= 0, ignore_products IF CUSTOMER_SEGMENT < 0;

-- transform products
x_products = FOREACH in_products GENERATE CUSTOMER_DESC, CUSTOMER_DEST_LOC, CUSTOMER_DEST_LOC_DESC,
  CUSTOMER_ID, CUSTOMER_SEGMENT_DESC, CUSTOMER_SEGMENT_ID,
  ToString(ToDate(extime, 'yyyyMMdd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as Extraction_Time:chararray,
  SALES_REP,
  CONCAT('(', QTY1, ',', VAL1, ';', QTY2, ',', VAL2, ')') as scale:chararray;

-- ensure there is at least one customer file
sh echo "-1,,,,,,,,,,," | hdfs dfs -put - /user/plasne/input-201705120930/customer-csv/empty.csv

-- load all customer CSVs
raw_customers = LOAD '/user/plasne/input-201705120930/customer-csv' USING CSVLoader(',')
  AS (CUSTOMER_SEGMENT_ID:int, CUSTOMER_SEGMENT_DESC:chararray, CUSTOMER_ID:int, CUSTOMER_DESC:chararray,
  CUSTOMER_DEST_LOC:chararray, CUSTOMER_DEST_LOC_DESC:chararray, SALES_REP:chararray, extime:chararray,
  QTY1:chararray, VAL1:chararray, QTY2:chararray, VAL2:chararray);

-- remove any fake rows
SPLIT raw_customers INTO in_customers IF CUSTOMER_SEGMENT_ID >= 0, ignore_customers IF CUSTOMER_SEGMENT < 0;

-- transform customers
x_customers = FOREACH in_products GENERATE CUSTOMER_DESC, CUSTOMER_DEST_LOC, CUSTOMER_DEST_LOC_DESC,
  CUSTOMER_ID, CUSTOMER_SEGMENT_DESC, CUSTOMER_SEGMENT_ID,
  ToString(ToDate(extime, 'yyyyMMdd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as Extraction_Time:chararray,
  SALES_REP,
  CONCAT('(', QTY1, ',', VAL1, ';', QTY2, ',', VAL2, ')') as scale:chararray;

-- store as XML
STORE x_products INTO '/user/plasne/output-201705120930/product-xml' USING output.XML('/user/plasne/config.json');
STORE x_customers INTO '/user/plasne/output-201705120930/customer-xml' USING output.XML('/user/plasne/config.json');
