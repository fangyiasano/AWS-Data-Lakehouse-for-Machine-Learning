CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing (
  user STRING,
  timeStamp BIGINT,
  x DOUBLE,
  y DOUBLE,
  z DOUBLE
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://fangyi/accelerometer/'
TBLPROPERTIES ('has_encrypted_data'='false');
