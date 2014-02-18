register dist/pig-udfs.jar

DEFINE ScdLoader com.b5m.pig.udf.ScdLoader();
DEFINE ValidMaxEntPairs com.b5m.pig.udf.MaxEntPairs();
DEFINE TopCategory com.b5m.pig.udf.CategoryText();
DEFINE MaxEntTrainer com.b5m.pig.udf.MaxEntClassifierBuilder();

data = LOAD '$input' USING ScdLoader();
data = FOREACH data GENERATE fields#'Title', fields#'Category';
data = FILTER data BY ValidMaxEntPairs(*);

top = FOREACH data GENERATE $0 AS title, TopCategory($1) AS category;

STORE top INTO '$output' USING MaxEntTrainer();

-- vim:ft=pig:nospell

