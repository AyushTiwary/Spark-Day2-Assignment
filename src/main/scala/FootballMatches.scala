import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object FootballMatches extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("Spark-Day2_Assignment")
    .setMaster("local[4]")

  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val filePath: String = "src/main/resources/D1.csv"

  //Q-1

  val footballDF: DataFrame = sparkSession.read.option("header", "true").option("inferSchema","true").csv(filePath)

  footballDF.createOrReplaceTempView("football")

  //Q-2

  val HomeTeamCount = sparkSession.sql("SELECT HomeTeam, count(1) As Counts FROM football GROUP BY HomeTeam")

    HomeTeamCount.createOrReplaceTempView("MatchesAsHomeTeam")

    HomeTeamCount.show()

  //Q-3

  sparkSession.sql("SELECT AwayTeam, count(1) As Counts FROM football GROUP BY AwayTeam")
    .createOrReplaceTempView("MatchesAsAwayTeam")

  sparkSession.sql("SELECT HomeTeam, count(1) As Wins FROM football WHERE FTR = 'H' GROUP BY HomeTeam")
    .createOrReplaceTempView("HomeTeamWins")

  sparkSession.sql("SELECT AwayTeam, count(1) As Wins FROM football WHERE FTR = 'A' GROUP BY AwayTeam")
    .createOrReplaceTempView("AwayTeamWins")

  sparkSession.sql("SELECT HomeTeam As Team, (HomeTeamWins.Wins + AwayTeamWins.Wins) AS Wins FROM HomeTeamWins FULL OUTER JOIN AwayTeamWins " +
    "ON HomeTeamWins.HomeTeam=AwayTeamWins.AwayTeam ").createOrReplaceTempView("TotalTeamWins")

  sparkSession.sql("SELECT HomeTeam As Team, (MatchesAsHomeTeam.Counts + MatchesAsAwayTeam.Counts) AS Total FROM MatchesAsHomeTeam FULL OUTER JOIN MatchesAsAwayTeam " +
    "ON MatchesAsHomeTeam.HomeTeam=MatchesAsAwayTeam.AwayTeam ").createOrReplaceTempView("TotalTeamMatches")

  sparkSession.sql("SELECT TotalTeamMatches.Team, (Wins/Total)*100 as Percentage FROM TotalTeamMatches FULL OUTER JOIN TotalTeamWins " +
    "ON TotalTeamWins.Team=TotalTeamMatches.Team ORDER BY Percentage DESC LIMIT 10" ).show()

  import sparkSession.implicits._

  //Q-4

  val footballDataSet = footballDF.map(row => Football(row.getString(2),row.getString(3),row.getInt(4),row.getInt(5),row.getString(6)))

  //Q-5

  val footballMatchesCount: DataFrame = footballDataSet.select("HomeTeam").withColumnRenamed("HomeTeam", "Team")
    .union(footballDataSet.select("AwayTeam").withColumnRenamed("AwayTeam", "Team")).groupBy("Team").count()
  footballMatchesCount.show(false)

  //Q-6

  val homeTeamDF = footballDataSet.select("HomeTeam","FTR").where("FTR = 'H'" ).groupBy("HomeTeam").count().withColumnRenamed("count", "HomeWins")
  val awayTeamDF = footballDataSet.select("AwayTeam","FTR").where("FTR = 'A'").groupBy("AwayTeam").count().withColumnRenamed("count", "AwayWins")

  val teamsDF = homeTeamDF.join(awayTeamDF, homeTeamDF.col("HomeTeam") === awayTeamDF.col("AwayTeam"))

  val add: (Int, Int) => Int = (HomeMatches: Int, TeamMatches: Int) => HomeMatches + TeamMatches

  val total = udf(add)

  teamsDF.withColumn("TotalWins", total(col("HomeWins"), col("AwayWins"))).select("HomeTeam","TotalWins")
    .withColumnRenamed("HomeTeam","Team").sort(desc("TotalWins")).limit(10).show(false)

}
