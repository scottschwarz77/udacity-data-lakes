CREATE EXTERNAL TABLE `accelerometer_landing`(
  `timestamp` bigint COMMENT 'from deserializer', 
  `user` string COMMENT 'from deserializer', 
  `x` float COMMENT 'from deserializer', 
  `y` float COMMENT 'from deserializer', 
  `z` float COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://scottschwarz77-bucket8782/accelerometer/landing/'
TBLPROPERTIES (
  'classification'='json')