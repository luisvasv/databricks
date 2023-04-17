-- Databricks notebook source
CREATE TABLE demo_types(
  bigint_field BIGINT COMMENT "range -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.",
  long_field LONG COMMENT "range -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.",
  binary_field BINARY COMMENT "type supports byte sequences of any length greater or equal to 0.",
  boolean_field BOOLEAN COMMENT "supports true and false values.",
  date_field DATE COMMENT "values of fields year, month, and day, without a time-zone, see format https://docs.databricks.com/sql/language-manual/data-types/date-type.html",
  float_field FLOAT COMMENT "range of numbers is : -3.402E+38 to -1.175E-37, +1.175E-37 to +3.402E+38",
  real_field REAL COMMENT "range of numbers is : -3.402E+38 to -1.175E-37, +1.175E-37 to +3.402E+38",
  int_field INT COMMENT "range of numbers is from -2,147,483,648 to 2,147,483,647",
  integer_field INTEGER COMMENT "range of numbers is from -2,147,483,648 to 2,147,483,647",
  interval_field INTERVAL DAY TO SECOND COMMENT " intervals of time either on a scale of seconds or months",
  void_field VOID COMMENT "void type can hold is NULL",
  smallint_field SMALLINT COMMENT "range of numbers is from -32,768 to 32,767",
  short_field SHORT COMMENT "range of numbers is from -32,768 to 32,767",
  string_field STRING COMMENT "character sequences of any length greater or equal to 0",
  timestamp_field TIMESTAMP COMMENT "comprising values of fields year, month, day, hour, minute, and second, with the session local time-zone, see formats: https://docs.databricks.com/sql/language-manual/data-types/timestamp-type.html",
  tinyint_field TINYINT COMMENT "range of numbers is from -128 to 127",
  byte_field BYTE COMMENT "range of numbers is from -128 to 127",
  array_field ARRAY<TINYINT> COMMENT "sequence of elements with the type of elementType",
  map_field  MAP<STRING, INT> COMMENT "set of key-value pairs",
  struct_field  STRUCT<Field1:INT NOT NULL COMMENT 'The first field.',Field2:ARRAY<INT>> COMMENT "values with the structure described by a sequence of fields"
)

-- COMMAND ----------

DESCRIBE EXTENDED demo_types;

-- COMMAND ----------

INSERT INTO demo_types
VALUES     ( 92233720368547758,                                     -- bigint_field  bigint
             9223372036854775807,                                   -- long_field  bigint
             '0b1000',                                              -- binary_field  binary
             true,                                                  -- boolean_field  boolean
             CAST('17/11/1987' AS DATE),                            -- date_field  date
             -5555555555555555.1f,                                  -- float_field  float
             -.1f,                                                  -- real_field  float
             10,                                                    -- int_field  int
             2344,                                                  -- integer_field  int
             '11 23:4:0',                                           -- interval_field  interval day to second
             NULL,                                                  -- void_field  void
             2000,                                                  -- smallint_field  smallint
             2000,                                                  -- short_field  smallint
             'databricks',                                          -- string_field  string
             CAST('2021-7-1T8:43:28.123456' as TIMESTAMP),          -- timestamp_field  timestamp
             -128,                                                  -- tinyint_field  tinyint
             100,                                                   -- byte_field  tinyint
             ARRAY(1, 2, 3, 4, 5, 6, 7),                            -- array_field  array
             MAP('red',1, 'black', 2 ),                             -- map_field  map
             named_struct('Field1', 1, 'Field2', Array(5, 3, 2, 1)) -- struct_field  struct>
) 

-- COMMAND ----------

SELECT string_field,
       binary_field,
       boolean_field,
       array_field,
       map_field,
       struct_field
FROM   demo_types; 

-- COMMAND ----------

SELECT array_field[1]
FROM   demo_types; 

-- COMMAND ----------

SELECT struct_field.Field2 AS general_struct,
       struct_field.Field2[2] AS struct_item
FROM   demo_types; 

-- COMMAND ----------

SELECT map_field AS general_map,
       map_field['red'] AS map_item
FROM demo_types; 

