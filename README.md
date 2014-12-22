DMP
===

Data Management Platform

# How to build

Checkout the source code and run unit tests:

    mvn test

Integration tests can be run using the `pig` profile:

    mvn verify -P pig

but require Pig and Couchbase installed on the local machine.

# How to deploy

## Package

Build the package using the `pig` profile:

    mvn package -P pig

The `dist` directory will contain the following files:
- `log_analysis.pig` Pig script implementing the daily analytics
- `log_analysis.properties.example` parameters required by the `log_analysis.pig` script
- `model_training.pig` Pig script for training a category classifier
- `model_training.properties.example` parameters required by the `model_training.pig` script
- `pig-udfs.jar` required Pig UDFs and their dependencies
- `user_profiles.pig` Pig script for building user profiles
- `user_profiles.properties.example` parameters required by the `user_categories.pig` script

## Deploy

The DMP is composed of several jobs that need to be properly scheduled and run.

### Category classifier

The classifier needs to be trained using some SCD files.
Adjust the properties file and then run:
   
    pig -f model_training.pig -m model_training.properties

This will produce a model file.
Please refer to the `model_training.properties.example` for details on the required properties.

### Daily log analysis

Daily logs are processed and user profiles extracted.
Adjust the properties file and then run:

    pig -f log_analysis.pig -m log_analysis.properties

This will produce a file with daily user profiles.
Please refer to the `log_analysis.properties.example` for details on the required properties.
Suggest specify invariant parameters in properties file and variant parameters with command line:
> pig -f log_analysis.pig -m log_analysis.properties -p input=.. -p output_dir=.. -p model_file=..
 

### User Profiles

User profiles can be computed over a time interval.
Typically one schedules two or more executions of this job, one for daily metrics and one or more for weekly/montly/etc. metrics.
Adjust the properties file and then run:

    pig -f user_profiles.pig -m user_profiles.properties

User profiles will be stored into Couchbase.
Please refer to the `user_profiles.properties.example` for details on the required properties.

> Invariant parameters in properties and Variant parameters with command line as well.

### Environment setup

Ensure that Pig directory contains
   - the file (or a symlink to) `piggybank.jar`
   - the directory `lib` containing Avro dependencies listed in `log_analysis.pig` script
      - `avro-*.jar`
      - `jackson-core-asl-*.jar`
      - `jackson-mapper-asl-*.jar`
      - `json-simple-*.jar`
      - `snappy-java-*.jar`

> Ensure 'lib' is on the same directory of `piggybank.jar`

# Data schema

User profiles are stored into Couchbase in Json format and accessed by composed keys containing the user `uuid` and the date.

Here is a sample document with key `0c6d8636f8be87e657f9b14ed07b54de::2014-03-28`:
<pre><code class="json">
{
  "uuid": "0c6d8636f8be87e657f9b14ed07b54de",
  "date": "2014-03-28",
  "period": 1,
  "page_categories": {
    "图书音像": 0.24210526315789474,
    "服装服饰": 0.6421052631578947,
    "玩乐爱好": 0.07368421052631578,
    "电脑办公": 0.03684210526315789,
    "鞋包配饰": 0.005263157894736842
  },
  "product_categories": {
    "服装服饰": 0.875,
    "运动户外": 0.125
  },
  "product_price": {
    "0 - 20": 0.020833333333333332,
    ......
    "400 - 420": 0.020833333333333332,
    "440 - 460": 0.020833333333333332,
  },
  "product_source": {
    "1号店官网": 0.041666666666666664,
    "V+": 0.041666666666666664,
    "京东商城": 0.0625,
    "卓越亚马逊": 0.0625,
    "天猫": 0.3125,
    "当当网": 0.14583333333333334,
    "淘宝网": 0.3333333333333333
  }
}
</code></pre>
