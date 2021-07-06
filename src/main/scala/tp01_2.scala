import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object tp01_2 {
  def readData(path: String, fileType: String)(implicit sparkSession: SparkSession): Either[String, DataFrame] = {
    fileType match {
      case "CSV" => Right(sparkSession.read.option("inferSchema", true).option("header", true).csv(path))
      case "PARQUET" => Right(sparkSession.read.parquet(path))
      case _ => Left("File format is not allowed")
    }
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    implicit val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import org.apache.spark.sql.functions._
    // 1
    val maybeDataFrame = readData("films01.csv", "CSV")
    val df = maybeDataFrame.right.get

    //2
    val df2 = df.withColumnRenamed("directedBY","acteur_principal")
      .withColumnRenamed("nb_vues","nombre_vues")
      .withColumnRenamed("note","note_film")
      .withColumnRenamed("title","nom_film")
    df2.printSchema()


    println(df2.show(10))

    // question 1
    //val dftvshow = df.filter("type == 'TV Show'")
    //println(dftvshow.show(3))
    // question 2

  }
}
