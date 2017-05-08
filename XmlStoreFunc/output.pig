REGISTER output.jar;
REGISTER json-simple-1.1.jar;
in = LOAD '/user/plasne/lee.csv' USING PigStorage(',') AS (customer:chararray, product:chararray, discount:float, startdate:chararray, enddate:chararray);
transform = FOREACH in GENERATE 'PP01' as ppa:chararray, 'PP02' as ppb:chararray, CONCAT(CONCAT(customer, '/'), product) as cp:chararray,
  ToString(ToDate(startdate, 'MMddyyyy'), 'yyyy-MM-dd HH:mm:ss') as start:chararray,
  ToString(ToDate(enddate, 'MMddyyyy'), 'yyyy-MM-dd HH:mm:ss') as end:chararray,
  ToString(CurrentTime(), 'yyyy-MM-dd HH:mm:ss') as current:chararray;
STORE transform INTO '/user/plasne/output.xml' USING output.XML('/user/plasne/config.json');
DUMP transform;
