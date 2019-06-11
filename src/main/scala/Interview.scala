import java.nio.file.{Files, Paths}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{SparkSession, functions => F}

object Interview extends App {

  override def main(args: Array[String]): Unit = {

    //
    // look in args / input the fileName
    //
    val fileName = if (args.nonEmpty && args(0).nonEmpty)
      args(0)
    else
      scala.io.StdIn.readLine("Input filename (i.e. /path_to/dataset_file.tsv): ")

    //
    // guard if the file exists
    //
    if (Files.notExists(Paths.get(fileName))) {
      println(s"File doesn't exist: $fileName")
    } else measureTime { // haha, you still want me to work, okay then...

      val sparkSession = SparkSession.builder()
        .master("local")
        .appName("interview")
        .getOrCreate


      //
      // set initial column names/types
      // according to readme.txt
      //
      val schema = StructType(Array(
        StructField(C.user, StringType),
        StructField(C.timestamp, TimestampType),
        StructField(C.artistID, StringType),
        StructField(C.artistName, StringType),
        StructField(C.trackID, StringType),
        StructField(C.trackName, StringType)
      ))

      //
      // read the file
      //
      val source = sparkSession.read
        .format("csv")
        .option("sep", "\t")
        .option("header", "false") // default
        .schema(schema)
        .csv(fileName)
        .drop(C.artistID, C.artistName, C.trackID) // we only need User, Timestamp, TrackName

      //
      // we'll usually sort by User/Timestamp
      //
      val userTimePartitionWindow = Window.partitionBy(C.user).orderBy(C.timestamp)

      //
      // previous (in window) track start time
      //
      val leadTimeColumn = F.lead(C.timestamp, 1, null).over(userTimePartitionWindow)

      //
      // a difference between current and next (in window) track
      //
      val pauseColumn = (F.when(F.col(C.nextSongStart).isNull, F.unix_timestamp(F.col(C.timestamp)))
        .otherwise(F.unix_timestamp(F.col(C.nextSongStart)))
        - F.unix_timestamp(F.col(C.timestamp))) / 60.0

      //
      // add pauses between tracks column
      //
      val srcSortedDataColumns = source
        .withColumn(C.nextSongStart, leadTimeColumn)
        .withColumn(C.pause, pauseColumn)


      //
      // set increasing IDs for each new listening session (first row)
      //
      val dataWithUserSessionDurations = srcSortedDataColumns
        .withColumn(C.durationFromPreviousTrack,
          F.lag(C.pause, 1, defaultValue = C.moreThanAllowedPause).over(userTimePartitionWindow))
        .withColumn(C.isNewListeningSession,
          F.when(
            F.lag(C.pause, 1, defaultValue = C.moreThanAllowedPause).over(userTimePartitionWindow) <= 0.0
            , F.monotonically_increasing_id()
          ).otherwise(
            F.when(F.lag(C.pause, 1, defaultValue = C.moreThanAllowedPause)
              .over(userTimePartitionWindow) < C.maxAllowedPause, null)
              .otherwise(F.monotonically_increasing_id())))

      //
      // fill session numbers
      //
      val dataBySessions = dataWithUserSessionDurations
        .withColumn(C.sessionId,
          F.last(C.isNewListeningSession, ignoreNulls = true).over(userTimePartitionWindow)
        )

      //
      // count and rank sessions to determine 50 longest
      //
      val dataForEstimates = dataBySessions.drop(
        C.nextSongStart, C.pause, C.durationFromPreviousTrack, C.isNewListeningSession
      )
        .withColumn(C.tracksInSession, F.count(C.sessionId).over(Window.partitionBy(C.sessionId)))
        .withColumn(C.sessionRank, F.dense_rank()
          .over(Window.orderBy(F.col(C.tracksInSession).desc, F.col(C.sessionId))))
        .where(F.col(C.sessionRank) <= C.winningSessions)


      //
      // count and rank tracks in 50 longest sessions to order and select 10 most popular
      //
      val dataForReport = dataForEstimates
        .drop(
          C.user, C.timestamp, C.sessionId, C.tracksInSession, C.sessionRank
        )
        .withColumn(C.trackRank, F.dense_rank().over(Window.orderBy(F.col(C.trackName).desc)))
        .where(F.col(C.trackRank) <= C.winningTracks)


      //
      //
      //
      dataForReport.drop(C.trackRank).distinct.show(false)
      sparkSession.stop()

    }


  }

  /**
    * Register a block (call-by-name) execution time
    */
  def measureTime[T](block: => T): T = {
    println("Measuring execution time...")
    val timeFrom = System.nanoTime()
    val result = block
    val timeTo = System.nanoTime()
    println(s"Elapsed time: ${(timeTo - timeFrom) / 1000000000} s")
    result
  }

  /**
    * constants warehouse
    * pretending to fight magic numbers and so on
    */
  object C {
    val maxAllowedPause = 20.0
    val moreThanAllowedPause: Double = maxAllowedPause + 1.0

    val winningTracks = 10
    val winningSessions = 50

    val user = "User"
    val timestamp = "Timestamp"
    val trackName = "TrackName"
    val artistID = "ArtistID"
    val artistName = "ArtistName"
    val trackID = "TrackID"
    val sessionId = "Session ID"
    val pause = "pause"
    val nextSongStart = "next song start"
    val durationFromPreviousTrack = "previous duration"
    val isNewListeningSession = "New Listening Session?"
    val tracksInSession = "tracks in session"
    val sessionRank = "session rank"
    val trackRank = "track rank"

  }

}

