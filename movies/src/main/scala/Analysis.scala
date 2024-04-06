import Analysis.movies
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object Analysis extends App{

  val spark = SparkSession.builder()
    .appName("Movies Analysis")
    .config("spark.master", "local")
    .getOrCreate()

  val movies = spark.read
    .format("csv")
    .option("header", true)
    .option("sep", ",")
    .option("inferSchema", "True")
    .load("src/resources/movie_metadata.csv")

  def getMeanRating(movies: DataFrame): Double =
  {
    movies.select(mean("imdb_score")).first().getDouble(0)
  }

  def getSTDRating(movies: DataFrame): Double = {
    movies.select(stddev("imdb_score")).first().getDouble(0)
  }

  println("Mean rating for the movies is "+getMeanRating(movies))
  println("STD of the ratings is "+getSTDRating(movies))


}
