CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
 customerName STRING,
  email STRING,
  phone STRING,
  birthDay STRING,
  serialNumber STRING,
  registrationDate BIGINT,
  lastUpdateDate BIGINT,
  shareWithResearchAsOfDate BIGINT,
  shareWithPublicAsOfDate BIGINT
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://fangyi/customer/'
TBLPROPERTIES ('has_encrypted_data'='false');
