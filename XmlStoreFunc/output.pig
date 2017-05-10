REGISTER output.jar;
REGISTER json-simple-1.1.jar;
in = LOAD '/user/plasne/CUSTOMER.201705041622.csv' USING PigStorage(',')
  AS (CUSTOMER_SEGMENT_ID:int, CUSTOMER_SEGMENT_DESC:chararray, CUSTOMER_ID:int, CUSTOMER_DESC:chararray,
  CUSTOMER_DEST_LOC:chararray, CUSTOMER_DEST_LOC_DESC:chararray, SALES_REP:chararray, extime:chararray,
  QTY1:float, VAL1:float, QTY2:float, VAL2:float);
transform = FOREACH in GENERATE CUSTOMER_DESC, CUSTOMER_DEST_LOC, CUSTOMER_DEST_LOC_DESC,
  CUSTOMER_ID, CUSTOMER_SEGMENT_DESC, CUSTOMER_SEGMENT_ID,
  ToString(ToDate(extime, 'yyyyMMdd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as Extraction_Time:chararray,
  SALES_REP,
  '(0,2;1,3)' as scale:chararray;
STORE transform INTO '/user/plasne/output.xml' USING output.XML('/user/plasne/config.json');
DUMP transform;