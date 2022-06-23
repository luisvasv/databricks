-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # DDL
-- MAGIC 
-- MAGIC 
-- MAGIC tener montado algun proceso y crear notebook
-- MAGIC 
-- MAGIC 
-- MAGIC for this section, we will using widely the next commands: `DESCRIBE <table> or DESCRIBE EXTENDED <table> ` and whole tables will be saved on database `demo`

-- COMMAND ----------

CREATE SCHEMA test 
LOCATION '/mnt/bronze' 
COMMENT 'test database for understanding the table concepts';

-- COMMAND ----------

USE test;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC base_location: str = '/mnt/bronze' 

-- COMMAND ----------

-- MAGIC %run ../../utilities/mount/with_storage_account $zone="bronze"

-- COMMAND ----------

-- MAGIC %run ../../utilities/mount/without_storage_account $zone="bronze"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CREATE TABLE
-- MAGIC ```bash
-- MAGIC { { [CREATE OR] REPLACE TABLE | CREATE TABLE [ IF NOT EXISTS ] }
-- MAGIC   [SCHEMA] table_name
-- MAGIC   [ column_specification ] [ USING data_source ]
-- MAGIC   [ table_clauses ]
-- MAGIC   [ AS query ] }
-- MAGIC 
-- MAGIC column_specification
-- MAGIC   ( { column_identifier column_type [ NOT NULL ]
-- MAGIC       [ GENERATED ALWAYS AS ( expr ) |
-- MAGIC         GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( [ START WITH start ] [ INCREMENT BY step ] ) ] ]
-- MAGIC       [ COMMENT column_comment ] } [, ...] )
-- MAGIC 
-- MAGIC table_clauses
-- MAGIC   { OPTIONS clause |
-- MAGIC     PARTITIONED BY clause |
-- MAGIC     clustered_by_clause |
-- MAGIC     LOCATION path [ WITH ( CREDENTIAL credential_name ) ] |
-- MAGIC     COMMENT table_comment |
-- MAGIC     TBLPROPERTIES clause } [...]
-- MAGIC 
-- MAGIC clustered_by_clause
-- MAGIC   { CLUSTERED BY ( cluster_column [, ...] )
-- MAGIC     [ SORTED BY ( { sort_column [ ASC | DESC ] } [, ...] ) ]
-- MAGIC     INTO num_buckets BUCKETS }
-- MAGIC     
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE DEFAULT (DELTA)
-- MAGIC 
-- MAGIC by default from databricks Runtime 8.0 and above the USING clause is optional. If you don’t specify the USING clause, DELTA is the default format.

-- COMMAND ----------

CREATE TABLE default_table(id INT, name STRING);

-- COMMAND ----------

DESCRIBE EXTENDED default_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE IF EXISTS

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS default_table (id INT, name STRING);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE FROM SELECT

-- COMMAND ----------

INSERT INTO default_table VALUES (1, 'USER # 1');
INSERT INTO default_table VALUES (2, 'USER # 2');
INSERT INTO default_table VALUES (3, 'USER # 3');
INSERT INTO default_table VALUES (4, 'USER # 4');
INSERT INTO default_table VALUES (5, 'USER # 5');

-- COMMAND ----------

CREATE TABLE table_from_query AS SELECT *,"sql - databricks" AS engine FROM default_table WHERE id > 2;

-- COMMAND ----------

DESC table_from_query;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE WITH LOCATION

-- COMMAND ----------

CREATE TABLE table_location(
email STRING)
LOCATION '/mnt/bronze/table_location' 

-- COMMAND ----------

DESCRIBE EXTENDED table_location;

-- COMMAND ----------

CREATE TABLE table_properties(name STRING) TBLPROPERTIES("version" ="1", "engine"="databricks");

-- COMMAND ----------

DESCRIBE EXTENDED table_properties ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE USING (WITH FORMAT)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE WITH PROPERTIES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CSV
-- MAGIC 
-- MAGIC from https://spark.apache.org/docs/latest/sql-data-sources-csv.html
-- MAGIC 
-- MAGIC 
-- MAGIC | Property Name             | Default                                                      | Meaning                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | Scope      |
-- MAGIC |---------------------------|--------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------|
-- MAGIC | sep                       | ,                                                            | Sets a separator for each field and value. This separator can be one or more characters.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | read/write |
-- MAGIC | encoding                  | UTF-8                                                        | For reading, decodes the CSV files by the given encoding type. For writing, specifies encoding (charset) of saved CSV files. CSV built-in functions ignore this option.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | read/write |
-- MAGIC | quote                     | "                                                            | Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string. For writing, if an empty string is set, it uses u0000 (null character).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | read/write |
-- MAGIC | quoteAll                  | false                                                        | A flag indicating whether all values should always be enclosed in quotes. Default is to only escape values containing a quote character.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | write      |
-- MAGIC | escape                    | \                                                            | Sets a single character used for escaping quotes inside an already quoted value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | read/write |
-- MAGIC | escapeQuotes              | true                                                         | A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | write      |
-- MAGIC | comment                   |                                                              | Sets a single character used for skipping lines beginning with this character. By default, it is disabled.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | read       |
-- MAGIC | header                    | false                                                        | For reading, uses the first line as names of columns. For writing, writes the names of columns as the first line. Note that if the given path is a RDD of Strings, this header option will remove all lines same with the header if exists. CSV built-in functions ignore this option.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | read/write |
-- MAGIC | inferSchema               | false                                                        | Infers the input schema automatically from data. It requires one extra pass over the data. CSV built-in functions ignore this option.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | read       |
-- MAGIC | enforceSchema             | true                                                         | If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Field names in the schema and column names in CSV headers are checked by their positions taking into account spark.sql.caseSensitive. Though the default value is true, it is recommended to disable the enforceSchema option to avoid incorrect results. CSV built-in functions ignore this option.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | read       |
-- MAGIC | ignoreLeadingWhiteSpace   | false (for reading), true (for writing)                      | A flag indicating whether or not leading whitespaces from values being read/written should be skipped.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | read/write |
-- MAGIC | ignoreTrailingWhiteSpace  | false (for reading), true (for writing)                      | A flag indicating whether or not trailing whitespaces from values being read/written should be skipped.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | read/write |
-- MAGIC | nullValue                 |                                                              | Sets the string representation of a null value. Since 2.0.1, this nullValue param applies to all supported types including the string type.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | read/write |
-- MAGIC | nanValue                  | NaN                                                          | Sets the string representation of a non-number value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | read       |
-- MAGIC | positiveInf               | Inf                                                          | Sets the string representation of a positive infinity value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | read       |
-- MAGIC | negativeInf               | -Inf                                                         | Sets the string representation of a negative infinity value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | read       |
-- MAGIC | dateFormat                | yyyy-MM-dd                                                   | Sets the string that indicates a date format. Custom date formats follow the formats at Datetime Patterns. This applies to date type.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | read/write |
-- MAGIC | timestampFormat           | yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]                             | Sets the string that indicates a timestamp format. Custom date formats follow the formats at Datetime Patterns. This applies to timestamp type.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | read/write |
-- MAGIC | timestampNTZFormat        | yyyy-MM-dd'T'HH:mm:ss[.SSS]                                  | Sets the string that indicates a timestamp without timezone format. Custom date formats follow the formats at Datetime Patterns. This applies to timestamp without timezone type, note that zone-offset and time-zone components are not supported when writing or reading this data type.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | read/write |
-- MAGIC | maxColumns                | 20480                                                        | Defines a hard limit of how many columns a record can have.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | read       |
-- MAGIC | maxCharsPerColumn         | -1                                                           | Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | read       |
-- MAGIC | mode                      | PERMISSIVE                                                   | Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes. Note that Spark tries to parse only required columns in CSV under column pruning. Therefore, corrupt records can be different based on required set of fields. This behavior can be controlled by spark.sql.csv.parser.columnPruning.enabled (enabled by default).  PERMISSIVE: when it meets a corrupted record, puts the malformed string into a field configured by columnNameOfCorruptRecord, and sets malformed fields to null. To keep corrupt records, an user can set a string type field named columnNameOfCorruptRecord in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. A record with less/more tokens than schema is not a corrupted record to CSV. When it meets a record having fewer tokens than the length of the schema, sets null to extra fields. When the record has more tokens than the length of the schema, it drops extra tokens. DROPMALFORMED: ignores the whole corrupted records. This mode is unsupported in the CSV built-in functions. FAILFAST: throws an exception when it meets corrupted records. | read       |
-- MAGIC | columnNameOfCorruptRecord | (value of spark.sql.columnNameOfCorruptRecord configuration) | Allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | read       |
-- MAGIC | multiLine                 | false                                                        | Parse one record, which may span multiple lines, per file. CSV built-in functions ignore this option.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | read       |
-- MAGIC | charToEscapeQuoteEscaping | escape or \0                                                 | Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | read/write |
-- MAGIC | samplingRatio             | 1.0                                                          | Defines fraction of rows used for schema inferring. CSV built-in functions ignore this option.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | read       |
-- MAGIC | emptyValue                | (for reading), "" (for writing)                              | Sets the string representation of an empty value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | read/write |
-- MAGIC | locale                    | en-US                                                        | Sets a locale as language tag in IETF BCP 47 format. For instance, this is used while parsing dates and timestamps.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | read       |
-- MAGIC | lineSep                   | \r, \r\n and \n (for reading), \n (for writing)              | Defines the line separator that should be used for parsing/writing. Maximum length is 1 character. CSV built-in functions ignore this option.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | read/write |
-- MAGIC | unescapedQuoteHandling    | STOP_AT_DELIMITER                                            | Defines how the CsvParser will handle values with unescaped quotes.  STOP_AT_CLOSING_QUOTE: If unescaped quotes are found in the input, accumulate the quote character and proceed parsing the value as a quoted value, until a closing quote is found. BACK_TO_DELIMITER: If unescaped quotes are found in the input, consider the value as an unquoted value. This will make the parser accumulate all characters of the current parsed value until the delimiter is found. If no delimiter is found in the value, the parser will continue accumulating characters from the input until a delimiter or line ending is found. STOP_AT_DELIMITER: If unescaped quotes are found in the input, consider the value as an unquoted value. This will make the parser accumulate all characters until the delimiter or a line ending is found in the input. SKIP_VALUE: If unescaped quotes are found in the input, the content parsed for the given value will be skipped and the value set in nullValue will be produced instead. RAISE_ERROR: If unescaped quotes are found in the input, a TextParsingException will be thrown.                                                                                | read       |
-- MAGIC | compression               | (none)                                                       | Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate). CSV built-in functions ignore this option.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | write      |

-- COMMAND ----------

CREATE TABLE test_csv(
name STRING,
email STRING,
country STRING)
USING CSV
OPTIONS (
  sep="|", 
  header="true")
LOCATION '/mnt/bronze/table_csv';

-- COMMAND ----------

DESCRIBE EXTENDED test_csv;

-- COMMAND ----------

SELECT * FROM test_csv;

-- COMMAND ----------

-- MAGIC %sh wget https://raw.githubusercontent.com/luisvasv/data/maser/datasets/csv/users/users.csv -O /tmp/users.csv%sh wget https://raw.githubusercontent.com/luisvasv/data/maser/datasets/csv/users/users.csv -O /tmp/users.csv

-- COMMAND ----------

-- MAGIC %fs cp  file:/tmp/users.csv dbfs:/mnt/bronze/table_csv/users.csv

-- COMMAND ----------

-- MAGIC %fs ls /mnt/bronze/table_csv/

-- COMMAND ----------

REFRESH TABLE test_csv;

-- COMMAND ----------

SELECT * FROM test_csv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####  PARQUET A
-- MAGIC 
-- MAGIC from https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
-- MAGIC 
-- MAGIC 
-- MAGIC | Property Name      | Default                                                             | Meaning                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Scope |
-- MAGIC |--------------------|---------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|
-- MAGIC | datetimeRebaseMode | (value of spark.sql.parquet.datetimeRebaseModeInRead configuration) | The datetimeRebaseMode option allows to specify the rebasing mode for the values of the DATE, TIMESTAMP_MILLIS, TIMESTAMP_MICROS logical types from the Julian to Proleptic Gregorian calendar. Currently supported modes are: EXCEPTION: fails in reads of ancient dates/timestamps that are ambiguous between the two calendars. CORRECTED: loads dates/timestamps without rebasing. LEGACY: performs rebasing of ancient dates/timestamps from the Julian to Proleptic Gregorian calendar. | read  |
-- MAGIC | int96RebaseMode    | (value of spark.sql.parquet.int96RebaseModeInRead configuration)    | The int96RebaseMode option allows to specify the rebasing mode for INT96 timestamps from the Julian to Proleptic Gregorian calendar. Currently supported modes are: EXCEPTION: fails in reads of ancient INT96 timestamps that are ambiguous between the two calendars. CORRECTED: loads INT96 timestamps without rebasing. LEGACY: performs rebasing of ancient timestamps from the Julian to Proleptic Gregorian calendar.                                                                  | read  |
-- MAGIC | mergeSchema        | (value of spark.sql.parquet.mergeSchema configuration)              | Sets whether we should merge schemas collected from all Parquet part-files. This will override spark.sql.parquet.mergeSchema.                                                                                                                                                                                                                                                                                                                                                                 | read  |
-- MAGIC | compression        | snappy                                                              | Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, uncompressed, snappy, gzip, lzo, brotli, lz4, and zstd). This will override spark.sql.parquet.compression.codec.                                                                                                                                                                                                                                                             | write |

-- COMMAND ----------

CREATE TABLE test_parquet(
name STRING,
email STRING,
country STRING)
USING PARQUET
OPTIONS (
  compression="snappy"
 )
LOCATION '/mnt/bronze/table_parquet';

-- COMMAND ----------

INSERT INTO test_parquet VALUES ("parquet # 1", "parquet@org.com", "colombia");
INSERT INTO test_parquet VALUES ("parquet # 2", "parquet@org.com", "colombia");

-- COMMAND ----------

-- MAGIC %fs ls /mnt/bronze/table_parquet

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### PARQUET B

-- COMMAND ----------

CREATE TABLE test_parquet_2(
name STRING,
email STRING,
country STRING)
USING PARQUET
OPTIONS (
  compression="snappy",
  path='/mnt/bronze/table_parquet_2'
 );

-- COMMAND ----------

INSERT INTO test_parquet_2 VALUES ("parquet # 1", "parquet@org.com", "colombia");
INSERT INTO test_parquet_2 VALUES ("parquet # 2", "parquet@org.com", "colombia");

-- COMMAND ----------

DESCRIBE EXTENDED test_parquet_2;

-- COMMAND ----------

-- MAGIC %fs ls /mnt/bronze/table_parquet_2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####  JSON
-- MAGIC from https://spark.apache.org/docs/latest/sql-data-sources-json.html
-- MAGIC 
-- MAGIC 
-- MAGIC | Property Name                      | Default                                                                                 | Meaning                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Scope      |
-- MAGIC |------------------------------------|-----------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------|
-- MAGIC | timeZone                           | (value of spark.sql.session.timeZone configuration)                                     | Sets the string that indicates a time zone ID to be used to format timestamps in the JSON datasources or partition values. The following formats of timeZone are supported:  Region-based zone ID: It should have the form 'area/city', such as 'America/Los_Angeles'. Zone offset: It should be in the format '(+\|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.Other short names like 'CST' are not recommended to use because they can be ambiguous.                                                                                                                                                                                                        | read/write |
-- MAGIC | primitivesAsString                 | false                                                                                   | Infers all primitive values as a string type.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | read       |
-- MAGIC | prefersDecimal                     | false                                                                                   | Infers all floating-point values as a decimal type. If the values do not fit in decimal, then it infers them as doubles.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | read       |
-- MAGIC | allowComments                      | false                                                                                   | Ignores Java/C++ style comment in JSON records.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | read       |
-- MAGIC | allowUnquotedFieldNames            | false                                                                                   | Allows unquoted JSON field names.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | read       |
-- MAGIC | allowSingleQuotes                  | true                                                                                    | Allows single quotes in addition to double quotes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | read       |
-- MAGIC | allowNumericLeadingZero            | false                                                                                   | Allows leading zeros in numbers (e.g. 00012).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | read       |
-- MAGIC | allowBackslashEscapingAnyCharacter | false                                                                                   | Allows accepting quoting of all character using backslash quoting mechanism.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | read       |
-- MAGIC | mode                               | PERMISSIVE                                                                              | Allows a mode for dealing with corrupt records during parsing.  PERMISSIVE: when it meets a corrupted record, puts the malformed string into a field configured by columnNameOfCorruptRecord, and sets malformed fields to null. To keep corrupt records, an user can set a string type field named columnNameOfCorruptRecord in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. When inferring a schema, it implicitly adds a columnNameOfCorruptRecord field in an output schema. DROPMALFORMED: ignores the whole corrupted records. This mode is unsupported in the JSON built-in functions. FAILFAST: throws an exception when it meets corrupted records. | read       |
-- MAGIC | columnNameOfCorruptRecord          | (value of spark.sql.columnNameOfCorruptRecord configuration)                            | Allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | read       |
-- MAGIC | dateFormat                         | yyyy-MM-dd                                                                              | Sets the string that indicates a date format. Custom date formats follow the formats at datetime pattern. This applies to date type.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | read/write |
-- MAGIC | timestampFormat                    | yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]                                                        | Sets the string that indicates a timestamp format. Custom date formats follow the formats at datetime pattern. This applies to timestamp type.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | read/write |
-- MAGIC | timestampNTZFormat                 | yyyy-MM-dd'T'HH:mm:ss[.SSS]                                                             | Sets the string that indicates a timestamp without timezone format. Custom date formats follow the formats at Datetime Patterns. This applies to timestamp without timezone type, note that zone-offset and time-zone components are not supported when writing or reading this data type.                                                                                                                                                                                                                                                                                                                                                                                                                            | read/write |
-- MAGIC | multiLine                          | false                                                                                   | Parse one record, which may span multiple lines, per file. JSON built-in functions ignore this option.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | read       |
-- MAGIC | allowUnquotedControlChars          | false                                                                                   | Allows JSON Strings to contain unquoted control characters (ASCII characters with value less than 32, including tab and line feed characters) or not.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | read       |
-- MAGIC | encoding                           | Detected automatically when multiLine is set to true (for reading), UTF-8 (for writing) | For reading, allows to forcibly set one of standard basic or extended encoding for the JSON files. For example UTF-16BE, UTF-32LE. For writing, Specifies encoding (charset) of saved json files. JSON built-in functions ignore this option.                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | read/write |
-- MAGIC | lineSep                            | \r, \r\n, \n (for reading), \n (for writing)                                            | Defines the line separator that should be used for parsing. JSON built-in functions ignore this option.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | read/write |
-- MAGIC | samplingRatio                      | 1.0                                                                                     | Defines fraction of input JSON objects used for schema inferring.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | read       |
-- MAGIC | dropFieldIfAllNull                 | false                                                                                   | Whether to ignore column of all null values or empty array/struct during schema inference.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | read       |
-- MAGIC | locale                             | en-US                                                                                   | Sets a locale as language tag in IETF BCP 47 format. For instance, locale is used while parsing dates and timestamps.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | read       |
-- MAGIC | allowNonNumericNumbers             | true                                                                                    | Allows JSON parser to recognize set of “Not-a-Number” (NaN) tokens as legal floating number values.  +INF: for positive infinity, as well as alias of +Infinity and Infinity. -INF: for negative infinity, alias -Infinity. NaN: for other not-a-numbers, like result of division by zero.                                                                                                                                                                                                                                                                                                                                                                                                                            | read       |
-- MAGIC | compression                        | (none)                                                                                  | Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate). JSON built-in functions ignore this option.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | write      |
-- MAGIC | ignoreNullFields                   | (value of spark.sql.jsonGenerator.ignoreNullFields configuration)                       | Whether to ignore null fields when generating JSON objects.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | write      |
-- MAGIC |                                    |                                                                                         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |            |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC to understand the example, es importand to understand the data
-- MAGIC 
-- MAGIC ```bash
-- MAGIC +----+---------------------------------+------------------+----------+------------------------+
-- MAGIC |    | email                           | city             |   active | citydc                 |
-- MAGIC |----+---------------------------------+------------------+----------+------------------------|
-- MAGIC |  0 | semper@yahoo.net                | Kraków           |        0 |                        |
-- MAGIC |  1 | purus.gravida@aol.couk          | Mmabatho         |        1 | Körfez                 |
-- MAGIC |  2 | lacinia.vitae@icloud.org        | Linköping        |        0 | Manaure                |
-- MAGIC |  3 | lacus@outlook.edu               | Ponte nelle Alpi |        1 | Périgueux              |
-- MAGIC |  4 | imperdiet.non@icloud.org        | New Glasgow      |        0 | Huelva                 |
-- MAGIC |  5 | erat.semper@outlook.com         | Surigao City     |        0 | Little Rock            |
-- MAGIC |  6 | tellus.nunc@yahoo.com           | Tocopilla        |        1 | Feldkirchen in Kärnten |
-- MAGIC |  7 | nascetur.ridiculus@hotmail.couk | Makurdi          |        0 |                        |
-- MAGIC |  8 | aliquet.sem@yahoo.couk          | River Valley     |        1 | Colomiers              |
-- MAGIC |  9 | nam.interdum.enim@yahoo.ca      | Legazpi          |          |                        |
-- MAGIC +----+---------------------------------+------------------+----------+------------------------+
-- MAGIC ```
-- MAGIC 
-- MAGIC 
-- MAGIC dropFieldIfAllNull
-- MAGIC 
-- MAGIC ignoreNullFields

-- COMMAND ----------

CREATE TABLE test_json(
email STRING,
city STRING,
active TINYINT,
citydc STRING
)
USING JSON
OPTIONS (
  primitivesAsString="true"
 )
LOCATION '/mnt/bronze/table_json';




-- COMMAND ----------

-- MAGIC %sh wget https://raw.githubusercontent.com/luisvasv/data/master/datasets/json/attendance/attendance.json -O /tmp/attendance.json

-- COMMAND ----------

-- MAGIC %fs cp  file:/tmp/attendance.json dbfs:/mnt/bronze/table_json/attendance.json

-- COMMAND ----------

-- MAGIC %fs ls /mnt/bronze/table_json/

-- COMMAND ----------

REFRESH test_json;

-- COMMAND ----------

drop table test_json;

-- COMMAND ----------

SELECT * FROM test_json;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE WITH PARTITION

-- COMMAND ----------

CREATE TABLE test_partition(
email STRING,
city STRING,
active TINYINT,
citydc STRING
) LOCATION '/mnt/bronze/table_partition'
PARTITIONED BY (active);

-- COMMAND ----------

-- MAGIC %fs ls /mnt/bronze/table_partition/

-- COMMAND ----------

INSERT INTO test_partition (email,city,active,citydc) VALUES ('purus.gravida@aol.couk','Mmabatho',1,'Körfez');
INSERT INTO test_partition (email,city,active,citydc) VALUES ('lacinia.vitae@icloud.org','Linköping',0,'Manaure');
INSERT INTO test_partition (email,city,active,citydc) VALUES ('lacus@outlook.edu','Ponte nelle Alpi',1,'Périgueux');

-- COMMAND ----------

-- MAGIC %fs ls /mnt/bronze/table_partition/

-- COMMAND ----------

-- MAGIC %fs ls /mnt/bronze/table_partition/active=1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE WITH GENERATED COLUMN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### AS EXPRESION

-- COMMAND ----------

CREATE TABLE test_generated_column(
test DATE,
year INT GENERATED ALWAYS AS (YEAR(test)),
month INT GENERATED ALWAYS AS (MONTH(test)),
day INT GENERATED ALWAYS AS (DAY(test))
) LOCATION '/mnt/bronze/table_generated_column';

-- COMMAND ----------

INSERT INTO test_generated_column(test) VALUES ('2017-12-02');

-- COMMAND ----------

SELECT * FROM test_generated_column;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### AS IDENTITY
-- MAGIC 
-- MAGIC `GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( [ START WITH start ] [ INCREMENT BY step ] ) ]`
-- MAGIC 
-- MAGIC The following operations are not supported:
-- MAGIC 
-- MAGIC * PARTITIONED BY an identity column
-- MAGIC * UPDATE an identity column

-- COMMAND ----------

CREATE TABLE test_generated_column_2(
test DATE,
counter BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 100)
) LOCATION '/mnt/bronze/table_generated_column_2';

-- COMMAND ----------

INSERT INTO test_generated_column_2(test) VALUES ('2017-12-02');
INSERT INTO test_generated_column_2(test) VALUES ('2017-12-03');

-- COMMAND ----------

SELECT * FROM test_generated_column_2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE WITH COMMENTS

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS events(
  id_event int
)LOCATION '/mnt/bronze/table_column'
COMMENT 'comment on table';

-- COMMAND ----------

DESCRIBE EXTENDED events;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE CONVERT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### WITHOUT PARTITION

-- COMMAND ----------

 CREATE TABLE test_convert_1 AS SELECT * FROM test_partition;


-- COMMAND ----------

SELECT * FROM test_convert_1;

-- COMMAND ----------

DESCRIBE EXTENDED test_convert_1;

-- COMMAND ----------

CONVERT TO DELTA test_convert_1;

-- COMMAND ----------

DESCRIBE EXTENDED test_convert_1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### *** WITH PARTITION

-- COMMAND ----------

drop table test_partition_to_convert;

-- COMMAND ----------

CREATE TABLE test_partition_to_convert(
email STRING,
city STRING,
active TINYINT,
citydc STRING)
USING PARQUET
PARTITIONED BY (active)
OPTIONS (path='/mnt/bronze/test_partition_to_convert')
CONVERT TO DELTA test_partition_to_convert;

-- COMMAND ----------

SELECT * FROM test_partition_to_convert;

-- COMMAND ----------

DESCRIBE EXTENDED test_convert_3;

-- COMMAND ----------

CONVERT TO DELTA test_partition_to_convert PARTITIONED BY (active)

-- COMMAND ----------

DESCRIBE EXTENDED test_convert_1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### NO STATISTICS

-- COMMAND ----------

 CREATE TABLE test_convert_2 AS SELECT * FROM test_partition;


-- COMMAND ----------

SELECT * FROM test_convert_2;

-- COMMAND ----------

DESCRIBE EXTENDED test_convert_2;

-- COMMAND ----------

CONVERT TO DELTA test_convert_2 NO STATISTICS;

-- COMMAND ----------

DESCRIBE EXTENDED test_convert_2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE CLONE
-- MAGIC 
-- MAGIC ```
-- MAGIC # way 1
-- MAGIC CREATE TABLE [IF NOT EXISTS] table_name
-- MAGIC    [SHALLOW | DEEP] CLONE source_table_name [VERSION AS OF version_number] [TBLPROPERTIES clause] [LOCATION path]
-- MAGIC    
-- MAGIC 
-- MAGIC # way 2
-- MAGIC [CREATE OR] REPLACE TABLE table_name
-- MAGIC    [SHALLOW | DEEP] CLONE source_table_name [VERSION AS OF version_number][TBLPROPERTIES clause] [LOCATION path]
-- MAGIC ```
-- MAGIC 
-- MAGIC 
-- MAGIC Clones have many use cases such as data archiving, reproducing ML datasets, data sharing and more. Additionally, clones can be either deep or shallow and there are a few notable differences between the two. A shallow clone does not copy the data files to clone the target, relies on the metadata as the source, and are cheaper to create. Deep clones will copy the source table data to the target location
-- MAGIC 
-- MAGIC for more information you can see https://www.mssqltips.com/sqlservertip/6853/deep-shallow-delta-clones-azure-databricks/#:~:text=Additionally%2C%20clones%20can%20be%20either,data%20to%20the%20target%20location.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SHALLOW CLONE
-- MAGIC 
-- MAGIC `SHALLOW CLONE` Databricks will make a copy of the source table’s definition, but refer to the source table’s files.
-- MAGIC 
-- MAGIC para este caso utilizaremos, la tabla `test_partition` para analizar el comportamiento antes y despues de la clonación

-- COMMAND ----------

SELECT *, input_file_name() 
FROM test_partition
ORDER BY active;

-- COMMAND ----------

CREATE TABLE clone_shadow_test_partition 
SHALLOW CLONE test_partition 
LOCATION '/mnt/bronze/clone_shadow_test_partition';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC al ejecutar la misma consulta sobre l tabla clonada, podemos ver que los datos no fueron agregados a la nueva locación, esto quiere decir que son linkeados y estamos accediendo remotamente

-- COMMAND ----------

SELECT *, input_file_name() 
FROM clone_shadow_test_partition
ORDER BY active;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC es importante entender que cuando se hace la clonación, el proceso toma un screen shot de cuando se realiza la clonación, al hacer la modificación a la tabla fuente no se vera reflejada en la tabla clonada, como podemos evidenciar:

-- COMMAND ----------

INSERT INTO test_partition (email,city,active,citydc) VALUES ('luis@outlook.edu','Ponte nelle Alpi',1,'Medellin');

-- COMMAND ----------

REFRESH clone_shadow_test_partition;

-- COMMAND ----------

SELECT *, input_file_name() 
FROM clone_shadow_test_partition
ORDER BY active;

-- COMMAND ----------

-- MAGIC %md se pueden realizar modificaciones sobre la tabla clonada, el cual, toda modificación no afectara a la tabla padre, si no, que todos los cambios se veran sobre la tabla clonada
-- MAGIC 
-- MAGIC nota: analizar los paths para evidenciar los resultados

-- COMMAND ----------

UPDATE clone_shadow_test_partition
SET citydc = 'bogota'
WHERE active = 1;

-- COMMAND ----------

SELECT *, input_file_name() 
FROM clone_shadow_test_partition
ORDER BY active;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DEEP CLONE GENERAL
-- MAGIC 
-- MAGIC `DEEP CLONE` (default) Databricks will make a complete, independent copy of the source table.
-- MAGIC 
-- MAGIC 
-- MAGIC Is similar to the script to create shallow clones, we can create a deep clone with the following script.

-- COMMAND ----------

CREATE TABLE deep_clone_test_partition 
DEEP CLONE test_partition 
LOCATION '/mnt/bronze/deep_clone_test_partition';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC After running the deep clone creation script, we can see the 4 active files from the original folder being copied to the deep clone folder.

-- COMMAND ----------

SELECT *, input_file_name() 
FROM deep_clone_test_partition
ORDER BY active;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DEEP CLONE VERSION
-- MAGIC 
-- MAGIC as we saw, deep clone copied 4 rows, that is the information that are storaged on the table, but if we want to create a table with specific version, clone table allows us it  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### SHALLOW

-- COMMAND ----------

CREATE TABLE shallow_clone_test_partition_v2 
SHALLOW CLONE test_partition 
VERSION AS OF 2
LOCATION '/mnt/bronze/shallow_clone_test_partition_v2';

-- COMMAND ----------

SELECT *, input_file_name() 
FROM shallow_clone_test_partition_v2
ORDER BY active;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### DEEP

-- COMMAND ----------

CREATE TABLE deep_clone_test_partition_v1 
DEEP CLONE test_partition 
VERSION AS OF 1
LOCATION '/mnt/bronze/shallow_clone_test_partition_v1';

-- COMMAND ----------

SELECT *, input_file_name() 
FROM deep_clone_test_partition_v1
ORDER BY active;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE TABLE WITH LIKE
-- MAGIC 
-- MAGIC Defines a table using the definition and metadata of an existing table or view.
-- MAGIC 
-- MAGIC ```
-- MAGIC CREATE TABLE [ IF NOT EXISTS ] table_name LIKE source_table_name [table_clauses]
-- MAGIC 
-- MAGIC table_clauses
-- MAGIC    { USING data_source |
-- MAGIC      LOCATION path |
-- MAGIC      TBLPROPERTIES clause } [...]
-- MAGIC 
-- MAGIC property_key
-- MAGIC   { identifier [. ...] | string_literal }
-- MAGIC ```
-- MAGIC 
-- MAGIC The file format to use for the table. data_source must be one of:
-- MAGIC 
-- MAGIC * TEXT
-- MAGIC * CSV
-- MAGIC * JSON
-- MAGIC * JDBC
-- MAGIC * PARQUET
-- MAGIC 
-- MAGIC If you do not specify `USING` the format of the source table will be inherited.
-- MAGIC 
-- MAGIC 
-- MAGIC we will use `test_json` that is a delta table
-- MAGIC 
-- MAGIC **NOTE**: Delta Lake does not support `CREATE TABLE LIKE`. Instead use `CREATE TABLE AS`.

-- COMMAND ----------

DESCRIBE EXTENDED test_json;

-- COMMAND ----------

CREATE TABLE test_json_like 
LIKE test_json
USING PARQUET
LOCATION '/mnt/bronze/test_json_like';

-- COMMAND ----------

DESCRIBE EXTENDED test_json_like;

-- COMMAND ----------

SELECT * FROM test_json_like;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ALTER TABLE
-- MAGIC 
-- MAGIC Alters the schema or properties of a table.
-- MAGIC 
-- MAGIC ```
-- MAGIC ALTER TABLE table_name
-- MAGIC    { RENAME TO clause |
-- MAGIC      ADD COLUMN clause |
-- MAGIC      ALTER COLUMN clause |
-- MAGIC      DROP COLUMN clause |
-- MAGIC      RENAME COLUMN clause |
-- MAGIC      ADD CONSTRAINT clause |
-- MAGIC      DROP CONSTRAINT clause |
-- MAGIC      ADD PARTITION clause |
-- MAGIC      DROP PARTITION clause |
-- MAGIC      RENAME PARTITION clause |
-- MAGIC      RECOVER PARTITIONS clause |
-- MAGIC      SET TBLPROPERTIES clause |
-- MAGIC      UNSET TBLPROPERTIES clause |
-- MAGIC      SET SERDE clause |
-- MAGIC      SET LOCATION clause |
-- MAGIC      OWNER TO clause }
-- MAGIC ```

-- COMMAND ----------

-- TO ANALIZE https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-alter-table.html

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DROP TABLE
-- MAGIC Deletes the table and removes the directory associated with the table from the file system if the table is not EXTERNAL table. An exception is thrown if the table does not exist.
-- MAGIC 
-- MAGIC ```
-- MAGIC DROP TABLE [ IF EXISTS ] table_name
-- MAGIC ```

-- COMMAND ----------

-- TO ANALIZE https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-drop-table.html

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## GENERAL COMMANDS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### REFRESH
-- MAGIC 
-- MAGIC Invalidates the cached entries for Apache Spark cache, which include data and metadata of the given table or view.
-- MAGIC 
-- MAGIC `REFRESH [TABLE] table_name`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### MSCK REPAIR
-- MAGIC 
-- MAGIC Recovers all the partitions in the directory of a table and updates the Hive metastore. When creating a table using PARTITIONED BY clause, partitions are generated and registered in the Hive metastore. However, if the partitioned table is created from existing data, partitions are not registered automatically in the Hive metastore
-- MAGIC 
-- MAGIC `MSCK REPAIR TABLE table_name [ {ADD | DROP | SYNC} PARTITIONS]`
-- MAGIC 
-- MAGIC Note:  only works with partitioned tables

-- COMMAND ----------


