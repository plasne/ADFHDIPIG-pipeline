SET default_parallel 1;
REGISTER $storage/user/plasne/output.jar;
in = LOAD '$storage/$input' USING PigStorage(',') AS (customer:chararray, product:chararray, discount:float, startdate:chararray, enddate:chararray);
transform = FOREACH in GENERATE 'PP01' as ppa:chararray, 'PP02' as ppb:chararray, CONCAT(CONCAT(customer, '/'), product) as cp:chararray,
  ToString(ToDate(startdate, 'MMddyyyy'), 'yyyy-MM-dd HH:mm:ss') as start:chararray,
  ToString(ToDate(enddate, 'MMddyyyy'), 'yyyy-MM-dd HH:mm:ss') as end:chararray,
  ToString(CurrentTime(), 'yyyy-MM-dd HH:mm:ss') as current:chararray;
ordered = ORDER transform BY start PARALLEL 1;
STORE ordered INTO '$storage/$output' USING output.XML('output', 'line');