-- Azure Stream Analytics SQL query
WITH emojis AS (  
    SELECT
        emoji.ArrayValue as emj
    FROM
        PublicTweetStream as tweets
    CROSS APPLY GetArrayElements(tweets.emojis) AS emoji
)
-- store windowed calculations to Azure SQL DB every X secs 
SELECT emj AS ecode,COUNT(*) AS counter INTO winCountDB FROM emojis
    GROUP BY emj,TumblingWindow(second,5)
   
-- store a projection of raw data to Azure Blob Storage continuously as well
SELECT tweets.id,tweets.text AS msg INTO rawEventsBS FROM PublicTweetStream as tweets

-- store windowed calculations to Azure Table Storage every X secs
SELECT CONCAT('emojis_',DATENAME(yyyy,System.Timestamp),'_',DATENAME(mm,System.Timestamp),
'_',DATENAME(dd,System.Timestamp),'_',DATENAME(hh,System.Timestamp),'_',DATENAME(mi,System.Timestamp),
'_',DATENAME(ss,System.Timestamp)) AS emojiwindow, emj AS ecode,COUNT(*) AS counter INTO winCountTS FROM emojis
    GROUP BY emj,TumblingWindow(second,30)