import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class AnalysisTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Movies Analysis Test")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") // We want to ignore all of the INFO and WARN messages.
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  "The Movies Analysis program" should "calculate mean and standard deviation of imdb_score correctly" in {
    val movies = spark.createDataFrame(Seq(
      (1, 7.5),
      (2, 8.0),
      (3, 6.5),
      (4, 9.0),
      (5, 7.0)
    )).toDF("id", "imdb_score")

    val meanRating = Analysis.getMeanRating(movies)

    meanRating shouldBe 7.6 +- 0.1
  }

  "The Movie Analysis Program" should "calculate STD of imdb_rating correctly" in{
    val movies = spark.createDataFrame(Seq(
      (1, 7.5),
      (2, 8.0),
      (3, 6.5),
      (4, 9.0),
      (5, 7.0)
    )).toDF("id", "imdb_score")

    val stdRating = Analysis.getSTDRating(movies)

    stdRating shouldBe 0.9 +- 0.1

  }


}
