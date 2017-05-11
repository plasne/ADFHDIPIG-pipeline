REGISTER output.jar;
REGISTER json-simple-1.1.jar;
in = LOAD './lee.csv' USING PigStorage(',') AS (customer:chararray, product:chararray, discount:float, startdate:chararray, enddate:chararray);
transform = FOREACH in GENERATE 'PP01' as ppa:chararray, 'PP02' as ppb:chararray, CONCAT(CONCAT(customer, '/'), product) as cp:chararray,
  ToString(ToDate(startdate, 'MMddyyyy'), 'yyyy-MM-dd HH:mm:ss') as start:chararray,
  ToString(ToDate(enddate, 'MMddyyyy'), 'yyyy-MM-dd HH:mm:ss') as end:chararray,
  ToString(CurrentTime(), 'yyyy-MM-dd HH:mm:ss') as current:chararray,
  '(4,4.54;6,7894.00;3,479.79)' as scale:chararray;
STORE transform INTO './output.xml' USING output.XML('./config.json');