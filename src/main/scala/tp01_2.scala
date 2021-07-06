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

    // 2
    val df2 = df.withColumnRenamed("directedBY","acteur_principal")
      .withColumnRenamed("nb_vues","nombre_vues")
      .withColumnRenamed("note","note_film")
      .withColumnRenamed("title","nom_film")
    df2.printSchema()


    println(df2.show(10))



    // 3
    val df_Leonardo_Di_Caprio = df2.filter("acteur_principal == 'Leonardo Di Caprio'")
    println(df_Leonardo_Di_Caprio.show(3))
    val cnt_ldc  = df_Leonardo_Di_Caprio.count()
    val cnt_total  = df2.count()
    println("le nombre de films de Leonardo Di Caprio est :"+cnt_ldc)
    println("cnt_total:"+cnt_total)
    println("moy est : >> "+ cnt_ldc.toDouble/cnt_total.toDouble)


    df2.groupBy("acteur_principal").avg("note_film").show(10)

    df2.groupBy("acteur_principal").avg("nombre_vues").show(10)

    // 4
    val total_views = df2.select(col("nombre_vues")).rdd.map(_(0).asInstanceOf[Int]).reduce(_+_)
    println(total_views)
    //df.withColumn("perc", ( total_views)).show


  }
}
