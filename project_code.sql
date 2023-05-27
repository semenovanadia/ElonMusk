--tweet text clean-up, creating columns to identify tweets related to Tesla, Bitcoin and Dogecoin
CREATE OR REPLACE TABLE "EM_TWEETS_15_23" AS 
select *
			,REPLACE(REPLACE(REPLACE("full_text", '&amp;', '&'), '&lt;', '<'), '&gt;', '>') as "tweet_text"
      ,CASE 
       WHEN "full_text" ilike any ('%tesla%', '%spacex%', '%starship%' ) THEN true
       ELSE false
       END AS "Tesla"
       ,CASE 
       WHEN "full_text" ilike any ('%btc%', '%bitcoin%','%crypto%', '%cryptocurrency%', '%cryptocurrencies%') THEN true
       ELSE false
       END AS "Bitcoin"
       ,CASE 
       WHEN "full_text" ilike any ('%doge%', '%dogecoin%','%crypto%', '%cryptocurrency%', '%cryptocurrencies%') THEN true
       ELSE false
       END AS "Dogecoin"
from "EM_TWEETS_15_23";

--fixing time format on tweets dataset
CREATE OR REPLACE TABLE EM_TWEETS_15_23 AS
SELECT *
      ,SUBSTR("timezone_et", 1, 17) || '00' AS "time_EM"
from EM_TWEETS_15_23;

--change datatype for columns 'likes' and 'replies'
CREATE OR REPLACE TABLE EM_TWEETS AS
select *
      ,to_number("favorite_count") AS "likes"
      ,to_number("reply_count") AS "replies"
FROM EM_TWEETS_15_23;


--changing time format in Bitcoin dataset
CREATE OR REPLACE TABLE BTC_19_23 AS
SELECT *,
  CASE
    WHEN TRY_TO_TIMESTAMP("date", 'MM/DD/YYYY HH24:MI') IS NOT NULL
    THEN TO_VARCHAR(TRY_TO_TIMESTAMP("date", 'MM/DD/YYYY HH24:MI'), 'YYYY-MM-DD HH24:MI:SS')
    ELSE "date"
  END AS "time"
FROM BTC_19_23
order by "time";

ALTER TABLE BTC_19_23 DROP "date";

--creating 5-hour volume/price average before the tweet, calculating price/volume change after 1/10mins
CREATE OR REPLACE TABLE "BTC_19_23INTERVALS" AS
  SELECT 
    *,
    AVG("close") OVER (ORDER BY "time" ROWS BETWEEN 360 PRECEDING AND 1 PRECEDING) AS "P_AVG_5hrs",
    CASE WHEN "P_AVG_5hrs" = 0 THEN NULL ELSE (LEAD("close", 1) OVER (ORDER BY "time") / "P_AVG_5hrs") - 1 END AS "price_diff_1min",
    CASE WHEN "P_AVG_5hrs" = 0 THEN NULL ELSE (LEAD("close", 10) OVER (ORDER BY "time") / "P_AVG_5hrs") - 1 END AS "price_diff_10mins",
    AVG("volume") OVER (ORDER BY "time" ROWS BETWEEN 360 PRECEDING AND 1 PRECEDING) AS "V_AVG_5hrs",
    CASE WHEN "V_AVG_5hrs" = 0 THEN NULL ELSE (LEAD("volume", 10) OVER (ORDER BY "time") / "V_AVG_5hrs") - 1 END AS "vol_diff_10mins",
  FROM 
    "BTC_19_23";

--join tweets on stock data
CREATE OR REPLACE TABLE BTC_TWEETS AS
SELECT BTC.*
       ,EM."time_EM" 
      ,EM."conversation_id"
      ,EM."full_text"
      ,EM."tweet_type"
      ,EM."url"
      ,EM."likes"
      ,EM."replies"
       ,EM."BTC"
FROM "BTC_19_23INTERVALS" BTC
LEFT JOIN EM_TWEETS EM ON BTC."time" = EM."time_EM";

--filling null data for market offline hours
CREATE OR REPLACE TABLE TESLA_upd AS
SELECT *, 
        LAG ("open") IGNORE NULLS OVER (ORDER BY "time") AS "open_filled",
        LAG ("high") IGNORE NULLS OVER (ORDER BY "time") AS "high_filled",
        LAG ("low") IGNORE NULLS OVER (ORDER BY "time") AS "low_filled",
        LAG ("close") IGNORE NULLS OVER (ORDER BY "time") AS "close_filled",
        LAG ("volume") IGNORE NULLS OVER (ORDER BY "time") AS "volume_filled",
        LAG ("P_AVG_5hrs") IGNORE NULLS OVER (ORDER BY "time") AS "P_AVG_5hrs_filled",
        LAG ("V_AVG_5hrs") IGNORE NULLS OVER (ORDER BY "time") AS "V_AVG_5hrs_filled",
        LAG ("price_diff_10mins") IGNORE NULLS OVER (ORDER BY "time") AS "price_diff_1min_filled",
        LAG ("price_diff_10mins") IGNORE NULLS OVER (ORDER BY "time") AS "price_diff_10mins_filled",
        LAG ("vol_diff_10mins") IGNORE NULLS OVER (ORDER BY "time") AS "vol_diff_10mins_filled",
FROM TESLA_TWEETS
order by "time";
