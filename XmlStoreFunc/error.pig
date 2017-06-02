REGISTER output.jar;
REGISTER json-simple-1.1.jar;
REGISTER /usr/hdp/2.5.4.0-121/pig/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- ensure there is at least one product file
--sh echo "-1,,,,,,,,,,," | hdfs dfs -put - /user/plasne/input-201705120930/product-csv/empty.csv
sh echo "-1,,,,,,,,,,," > /input/product-csv/empty.csv

-- load all product CSVs   /user/plasne/input-201705120930/product-csv
raw_products = LOAD './input/product-csv' USING CSVLoader(',')
  AS (CUSTOMER_SEGMENT_ID:int, CUSTOMER_SEGMENT_DESC:chararray, CUSTOMER_ID:int, CUSTOMER_DESC:chararray,
  CUSTOMER_DEST_LOC:chararray, CUSTOMER_DEST_LOC_DESC:chararray, SALES_REP:chararray, extime:chararray,
  QTY1:chararray, VAL1:chararray, QTY2:chararray, VAL2:chararray);

-- remove any fake rows
--SPLIT raw_products INTO in_products IF CUSTOMER_SEGMENT_ID >= 0, ignore_products IF CUSTOMER_SEGMENT < 0;
in_products = FILTER raw_products BY CUSTOMER_SEGMENT_ID >= 0;

-- transform products
x_products = FOREACH in_products GENERATE CUSTOMER_DESC, CUSTOMER_DEST_LOC, CUSTOMER_DEST_LOC_DESC,
  CUSTOMER_ID, CUSTOMER_SEGMENT_DESC, CUSTOMER_SEGMENT_ID,
  ToString(ToDate(extime, 'yyyyMMdd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as Extraction_Time:chararray,
  SALES_REP,
  CONCAT('(', QTY1, ',', VAL1, ';', QTY2, ',', VAL2, ')') as scale:chararray;

-- ensure there is at least one customer file
--sh echo "-1,,,,,,,,,,," | hdfs dfs -put - /user/plasne/input-201705120930/customer-csv/empty.csv
sh echo "-1,,,,,,,,,,," > /input/customer-csv/empty.csv

-- load all customer CSVs     /user/plasne/input-201705120930/customer-csv
raw_customers = LOAD './input/customer-csv' USING CSVLoader(',')
  AS (CUSTOMER_SEGMENT_ID:int, CUSTOMER_SEGMENT_DESC:chararray, CUSTOMER_ID:int, CUSTOMER_DESC:chararray,
  CUSTOMER_DEST_LOC:chararray, CUSTOMER_DEST_LOC_DESC:chararray, SALES_REP:chararray, extime:chararray,
  QTY1:chararray, VAL1:chararray, QTY2:chararray, VAL2:chararray);

-- remove any fake rows
--SPLIT raw_customers INTO in_customers IF CUSTOMER_SEGMENT_ID >= 0, ignore_customers IF CUSTOMER_SEGMENT < 0;
in_customers = FILTER raw_customers BY CUSTOMER_SEGMENT_ID >= 0;

-- transform customers
x_customers = FOREACH in_customers GENERATE CUSTOMER_DESC, CUSTOMER_DEST_LOC, CUSTOMER_DEST_LOC_DESC,
  CUSTOMER_ID, CUSTOMER_SEGMENT_DESC, CUSTOMER_SEGMENT_ID,
  ToString(ToDate(extime, 'yyyyMMdd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as Extraction_Time:chararray,
  SALES_REP,
  CONCAT('(', QTY1, ',', VAL1, ';', QTY2, ',', VAL2, ')') as scale:chararray;

-- store as XML /user/plasne/output-201705120930    /user/plasne/config.json
STORE x_products INTO './output/product-xml' USING output.XML('./config.json');
STORE x_customers INTO './output/customer-xml' USING output.XML('./config.json');
