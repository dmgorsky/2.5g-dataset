> 5 Write a service, which accepts the path to the dataset and produces a list of top 10 songs played in the top 50 longest user sessions by tracks count. Each user session may be comprised of one or more songs played by that user, where each song is started within 20 minutes of the previous song's start time.
> ...http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz...

This task solution took me a couple of evenings (thanks in advance for being patient while waiting :), it's sometimes hard to work from home, especially these days.
Back to our topic, the task looks like an olympiad problem which I love so much.
I've just used Apache Spark to cope with a big-sized dataset collections, and the straightforward algorithm is the following:
* we divide source dataset by users and order by timestamps inside each one (our main window for analytical functions);
* to see if we should start a new listening session we add a column with a pause between user's tracks;
* we assign an increasing numbers to listening sessions:
	- session starts when the user changed, or the track starts later than allowed pause, or the window starts (the first row of a session)
	- we fill empty session rows with the last number observed (the other rows of a session)
* we rank the results by session numbers and collect the 50 longest (by tracks count in each one) listening sessions
* in the sessions collected we rank tracks by counting their number and select top 10 users likes' winners (`spoiler: they are all chinese`)

The program can be run via
* ```./run.sh path_to/file.tsv``` or ```sbt "run path_to/file.tsv"```
* ```./run.sh``` or ```sbt run``` (will ask for a file input)

On a '2014 notebook it takes ~4 minutes to process 2.5Gb dataset. For the algorithm readability sake I didn't invest in any optimization :)
Due to the same I didn't add any tests ;) If you need any, please don't hesitate to contact me. 

The following tables are built on a reduced demo dataset. And only showing top 5 rows from each

|       User|          Timestamp|           TrackName|
|-----------|-------------------|--------------------|
|user_000001|2009-05-05 02:08:57|Fuck Me Im Famous...|
|user_000001|2009-05-04 16:54:10|Composition 0919 ...|
|user_000001|2009-05-04 16:52:04|Mc2 (Live_2009_4_15)|
|user_000001|2009-05-04 16:42:52|Hibari (Live_2009...|
|user_000001|2009-05-04 16:42:11|Mc1 (Live_2009_4_15)|


|       User|          Timestamp|           TrackName|    next song start|            pause|
|-----------|-------------------|--------------------|-------------------|-----------------|
|user_000001|2009-04-20 16:57:45|Bibo No Aozora (L...|2009-04-20 17:18:00|            20.25|
|user_000001|2009-04-20 17:18:00|Behind The Mask (...|2009-04-20 17:22:08|4.133333333333334|
|user_000001|2009-04-20 17:22:08|Tibetan Dance (Ve...|2009-04-20 17:27:22|5.233333333333333|
|user_000001|2009-04-20 17:27:22|Happyend (Live_20...|2009-04-21 17:12:07|          1424.75|
|user_000001|2009-04-21 17:12:07|Behind The Mask (...|2009-04-21 17:16:27|4.333333333333333|


|       User|          Timestamp|           TrackName|    next song start|            pause|previous duration|New Listening Session?|
|-----------|-------------------|--------------------|-------------------|-----------------|-----------------|----------------------|
|user_000001|2009-04-20 16:57:45|Bibo No Aozora (L...|2009-04-20 17:18:00|            20.25|             21.0|          944892805120|
|user_000001|2009-04-20 17:18:00|Behind The Mask (...|2009-04-20 17:22:08|4.133333333333334|            20.25|          944892805121|
|user_000001|2009-04-20 17:22:08|Tibetan Dance (Ve...|2009-04-20 17:27:22|5.233333333333333|4.133333333333334|                  null|
|user_000001|2009-04-20 17:27:22|Happyend (Live_20...|2009-04-21 17:12:07|          1424.75|5.233333333333333|                  null|
|user_000001|2009-04-21 17:12:07|Behind The Mask (...|2009-04-21 17:16:27|4.333333333333333|          1424.75|          944892805122|


|       User|          Timestamp|           TrackName|    next song start|            pause|previous duration|New Listening Session?|  Session ID|
|-----------|-------------------|--------------------|-------------------|-----------------|-----------------|----------------------|------------|
|user_000001|2009-04-20 16:57:45|Bibo No Aozora (L...|2009-04-20 17:18:00|            20.25|             21.0|          944892805120|944892805120|
|user_000001|2009-04-20 17:18:00|Behind The Mask (...|2009-04-20 17:22:08|4.133333333333334|            20.25|          944892805121|944892805121|
|user_000001|2009-04-20 17:22:08|Tibetan Dance (Ve...|2009-04-20 17:27:22|5.233333333333333|4.133333333333334|                  null|944892805121|
|user_000001|2009-04-20 17:27:22|Happyend (Live_20...|2009-04-21 17:12:07|          1424.75|5.233333333333333|                  null|944892805121|
|user_000001|2009-04-21 17:12:07|Behind The Mask (...|2009-04-21 17:16:27|4.333333333333333|          1424.75|          944892805122|944892805122|


|       User|          Timestamp|      TrackName|   Session ID|tracks in session|rank|
|-----------|-------------------|---------------|-------------|-----------------|----|
|user_000002|2008-11-17 12:26:08|     All I Need|1700807049224|              130|   1|
|user_000002|2008-11-17 12:30:37|Playground Love|1700807049224|              130|   1|
|user_000002|2008-11-17 12:34:09|      Clouds Up|1700807049224|              130|   1|
|user_000002|2008-11-17 12:35:39|  Bathroom Girl|1700807049224|              130|   1|
|user_000002|2008-11-17 12:38:05| Cemetary Party|1700807049224|              130|   1|


|     TrackName|track rank|
|--------------|----------|
|     Á Rebours|         1|
|Youth Of Today|         2|
|Youth Of Today|         2|
|Youth Of Today|         2|
|Your Protector|         3|


