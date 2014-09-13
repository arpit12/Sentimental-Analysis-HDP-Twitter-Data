shark/bin/shark --service sharkserver2

shark/bin/beeline

!connect jdbc:hive2://localhost:10000/default
!connect jdbc:hive2://54.226.78.66:10000/default

wget http://s3.amazonaws.com/hw-sandbox/tutorial13/SentimentFiles.zip

locally also download: for testing..

cd SentimentFiles/tweets_raw
~/ephemeral-hdfs/bin/hadoop fs -mkdir /upload
~/ephemeral-hdfs/bin/hadoop fs -copyFromLocal . /upload
~/ephemeral-hdfs/bin/hadoop fs -ls /upload

~/ephemeral-hdfs/bin/hadoop fs -mkdir /upload/dictionary
~/ephemeral-hdfs/bin/hadoop fs -mkdir /upload/time_zone_map

~/ephemeral-hdfs/bin/hadoop fs -put ../upload/data/dictionary/dictionary.tsv /upload/dictionary
~/ephemeral-hdfs/bin/hadoop fs -put ../upload/data/time_zone_map/time_zone_map.tsv /upload/time_zone_map



ADD JAR /root/SentimentFiles/upload/hive/json-serde-1.1.6-SNAPSHOT-jar-with-dependencies.jar;

CREATE EXTERNAL TABLE tweets_raw (
   id BIGINT,
   created_at STRING,
   source STRING,
   favorited BOOLEAN,
   retweet_count INT,
   retweeted_status STRUCT<
      text:STRING,
      user:STRUCT<screen_name:STRING,name:STRING>>,
   entities STRUCT<
      urls:ARRAY<STRUCT<expanded_url:STRING>>,
      user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
      hashtags:ARRAY<STRUCT<text:STRING>>>,
   text STRING,
   user STRUCT<
      screen_name:STRING,
      name:STRING,
      friends_count:INT,
      followers_count:INT,
      statuses_count:INT,
      verified:BOOLEAN,
      utc_offset:STRING,
      time_zone:STRING>,
   in_reply_to_screen_name STRING
)
PARTITIONED BY (day INT, hour INT)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'  
LOCATION '/upload/tweets_raw';


ALTER TABLE tweets_raw ADD  PARTITION (day=02,hour=15)  PARTITION (day=02,hour=16) PARTITION (day=02,hour=17) PARTITION (day=02,hour=18) PARTITION (day=02,hour=19) PARTITION (day=02,hour=20) PARTITION (day=02,hour=21) PARTITION (day=02,hour=22) PARTITION (day=02,hour=23)
PARTITION (day=03,hour=01) 
PARTITION (day=03,hour=02) 
PARTITION (day=03,hour=03) 
PARTITION (day=03,hour=04) 
PARTITION (day=03,hour=05) 
PARTITION (day=03,hour=06) 
PARTITION (day=03,hour=07) 
PARTITION (day=03,hour=08) 
PARTITION (day=03,hour=09) 
PARTITION (day=03,hour=10) 
PARTITION (day=03,hour=11) 
PARTITION (day=03,hour=12) 
PARTITION (day=03,hour=13) 
PARTITION (day=03,hour=14) 
PARTITION (day=03,hour=15) 
PARTITION (day=03,hour=16) 
PARTITION (day=03,hour=17) 
PARTITION (day=03,hour=18) 
PARTITION (day=03,hour=19) 
PARTITION (day=03,hour=20) 
PARTITION (day=03,hour=21) 
PARTITION (day=03,hour=22) 
PARTITION (day=03,hour=23) 
PARTITION (day=04,hour=01) 
PARTITION (day=04,hour=02) 
PARTITION (day=04,hour=03) 
PARTITION (day=04,hour=04) 
PARTITION (day=04,hour=05) 
PARTITION (day=04,hour=06) 
PARTITION (day=04,hour=07) 
PARTITION (day=04,hour=08) 
PARTITION (day=04,hour=09) 
PARTITION (day=04,hour=10) 
PARTITION (day=04,hour=11) 
PARTITION (day=04,hour=12) 
PARTITION (day=04,hour=13) 
PARTITION (day=04,hour=14) 
PARTITION (day=04,hour=15) 
PARTITION (day=04,hour=16) 
PARTITION (day=04,hour=17) 
PARTITION (day=04,hour=18) 
PARTITION (day=04,hour=19) 
PARTITION (day=04,hour=20) 
PARTITION (day=04,hour=21) 
PARTITION (day=04,hour=22) 
PARTITION (day=04,hour=23) 
PARTITION (day=05,hour=01) 
PARTITION (day=05,hour=02) 
PARTITION (day=05,hour=03) 
PARTITION (day=05,hour=04) 
PARTITION (day=05,hour=05) 
PARTITION (day=05,hour=06) 
PARTITION (day=05,hour=07) 
PARTITION (day=05,hour=08) 
PARTITION (day=05,hour=09) 
PARTITION (day=05,hour=10) 
PARTITION (day=05,hour=11) 
PARTITION (day=05,hour=12) 
PARTITION (day=05,hour=13) 
PARTITION (day=05,hour=14) 
PARTITION (day=05,hour=15) 
PARTITION (day=05,hour=16) 
PARTITION (day=05,hour=17) 
PARTITION (day=05,hour=18) 
PARTITION (day=05,hour=19) 
PARTITION (day=05,hour=20) 
PARTITION (day=05,hour=21) 
PARTITION (day=05,hour=22) 
PARTITION (day=05,hour=23) 
PARTITION (day=06,hour=01) 
PARTITION (day=06,hour=02) 
PARTITION (day=06,hour=03) 
PARTITION (day=06,hour=04) 
PARTITION (day=06,hour=05) 
PARTITION (day=06,hour=06) 
PARTITION (day=06,hour=07) 
PARTITION (day=06,hour=08) 
PARTITION (day=06,hour=09) 
PARTITION (day=06,hour=10) 
PARTITION (day=06,hour=11) 
PARTITION (day=06,hour=12) 
PARTITION (day=06,hour=13) 
PARTITION (day=06,hour=14) 
PARTITION (day=06,hour=15) 
PARTITION (day=06,hour=16) 
PARTITION (day=06,hour=17) 
PARTITION (day=06,hour=18) 
PARTITION (day=06,hour=19) 
PARTITION (day=06,hour=20) 
PARTITION (day=06,hour=21) 
PARTITION (day=06,hour=22) 
PARTITION (day=06,hour=23) 
PARTITION (day=07,hour=01) 
PARTITION (day=07,hour=02) 
PARTITION (day=07,hour=03) 
PARTITION (day=07,hour=04) 
PARTITION (day=07,hour=05) 
PARTITION (day=07,hour=06) 
PARTITION (day=07,hour=07) 
PARTITION (day=07,hour=08) 
PARTITION (day=07,hour=09) 
PARTITION (day=07,hour=10) 
PARTITION (day=07,hour=11) 
PARTITION (day=07,hour=12) 
PARTITION (day=07,hour=13) 
PARTITION (day=07,hour=14) 
PARTITION (day=07,hour=15) 
PARTITION (day=07,hour=16) 
PARTITION (day=07,hour=17) 
PARTITION (day=07,hour=18) 
PARTITION (day=07,hour=19) 
PARTITION (day=07,hour=20) 
PARTITION (day=07,hour=21) 
PARTITION (day=07,hour=22) 
PARTITION (day=07,hour=23)
PARTITION (day=08,hour=00)PARTITION (day=08,hour=01)PARTITION (day=08,hour=02)PARTITION (day=08,hour=03)PARTITION (day=08,hour=04)PARTITION (day=08,hour=05)PARTITION (day=08,hour=06)PARTITION (day=08,hour=07)PARTITION (day=08,hour=08)PARTITION (day=08,hour=09)PARTITION (day=08,hour=10)PARTITION (day=08,hour=11)
;
 



-- create sentiment dictionary
CREATE EXTERNAL TABLE dictionary (
    type string,
    length int,
    word string,
    pos string,
    stemmed string,
    polarity string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE
LOCATION '/upload/dictionary';

CREATE EXTERNAL TABLE time_zone_map (
    time_zone string,
    country string,
    notes string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE
LOCATION '/upload/time_zone_map';

-- Clean up tweets
CREATE VIEW tweets_simple AS
SELECT
  id,
  cast ( from_unixtime( unix_timestamp(concat( '2013 ', substring(created_at,5,15)), 'yyyy MMM dd hh:mm:ss')) as timestamp) ts,
  text,
  user.time_zone 
FROM tweets_raw
;

CREATE VIEW tweets_clean AS
SELECT
  id,
  ts,
  text,
  m.country 
 FROM tweets_simple t LEFT OUTER JOIN time_zone_map m ON t.time_zone = m.time_zone;

-- Compute sentiment
create view l1 as select id, words from tweets_raw lateral view explode(sentences(lower(text))) dummy as words;
create view l2 as select id, word from l1 lateral view explode( words ) dummy as word ;

-- was: select * from l2 left outer join dict d on l2.word = d.word where polarity = 'negative' limit 10;

create view l3 as select 
    id, 
    l2.word, 
    case d.polarity 
      when  'negative' then -1
      when 'positive' then 1 
      else 0 end as polarity 
 from l2 left outer join dictionary d on l2.word = d.word;
 


 create table tweets_sentiment as select 
  id, 
  case 
    when sum( polarity ) > 0 then 'positive' 
    when sum( polarity ) < 0 then 'negative'  
    else 'neutral' end as sentiment 
 from l3 group by id;

-- put everything back together and re-number sentiment
CREATE TABLE tweetsbi_cached 
AS
SELECT 
  t.*,
  case s.sentiment 
    when 'positive' then 2 
    when 'neutral' then 1 
    when 'negative' then 0 
  end as sentiment  
FROM tweets_clean t LEFT OUTER JOIN tweets_sentiment s on t.id = s.id;

-- for Tableau or Excel
-- UDAF sentiscore = sum(sentiment)*50  / count(sentiment)

-- context n-gram made readable
CREATE TABLE twitter_3grams
AS
SELECT day, hour, snippet 
FROM
( SELECT
     day,
     hour,
     context_ngrams(sentences(lower(text)), array("iron","man","3",null,null,null), 10) ngs
  FROM tweets_raw group by day, hour 
) base
 LATERAL VIEW
     explode(  ngs  ) ngsTab AS snippet
; 

CREATE TABLE twitter_3grams AS SELECT day, hour, snippet FROM ( SELECT day, hour, context_ngrams(sentences(lower(text)), array("iron","man","3",null,null,null), 10) ngs FROM tweets_raw group by day, hour ) base LATERAL VIEW explode(  ngs  ) ngsTab AS snippet ;

