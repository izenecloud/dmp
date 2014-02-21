data = LOAD '$input';
STORE data INTO '$output' USING com.b5m.pig.udf.DateStorage('$date');
-- vim:ft=pig:nospell: