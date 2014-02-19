/*
Required parameters:
- input         path to SCD files
- model_file    path to model file
- udf_file      path to pig-udfs.jar
 */

REGISTER $udf_file

DEFINE ScdLoader com.b5m.pig.udf.ScdLoader();
DEFINE ValidMaxEntPairs com.b5m.pig.udf.MaxEntPairs();
DEFINE TopCategory com.b5m.pig.udf.CategoryText();
DEFINE MaxEntTrainer com.b5m.pig.udf.MaxEntClassifierBuilder();

data = LOAD '$input' USING ScdLoader();
data = FOREACH data GENERATE fields#'Title', fields#'Category';
data = FILTER data BY ValidMaxEntPairs(*);

top = FOREACH data GENERATE $0 AS title, TopCategory($1) AS category;

STORE top INTO '$model_file' USING MaxEntTrainer();

-- vim:ft=pig:nospell:
