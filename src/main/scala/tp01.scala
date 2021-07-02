import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
//import org.joda.time._
import java.util.Date
import java.text.SimpleDateFormat


import org.apache.spark.SparkConf

case class Film(imdb_title_id: String,
                weighted_average: Double,
                total_votes: Int,
                mean_vote: Double,
                median_vote: Double,
                votes_10: Int,
                votes_9: Int,
                votes_8: Int,
                votes_7: Int,
                votes_6: Int
               )

case class Movies2(
                    imdb_title_id: String,
                    genre: String,
                    duration: Double,
                    country: String,
                    language: String,
                    director: String

                  )


object tp01 {



  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")

    Logger.getLogger("org").setLevel(Level.OFF)
    val session = SparkSession.builder().master("local").getOrCreate()
    val sc = session.sparkContext
    val movieRDD = sc.textFile("films.csv")
    val header = movieRDD.first()
    val movieRDDWithoutHeader = movieRDD.filter(e => e!=header)



    val movieObjectRDD = movieRDDWithoutHeader.map(elem => (Movies2(elem.split(",")(0),elem.split(",")(5), elem.split(",")erci(6), elem.split(",")(7),elem.split(",")(8),elem.split(",")(9))))
    // 3 count of movies directed by Leonardo Di Caprio
    val rddLeonardo = movieObjectRDD.filter(elem=>elem.director.equals("Leonardo Di Caprio"))
    println(rddLeonardo.count())
  }
  def getDataByDirector(id1: String, id2: String): Boolean = {
    id1 == id2
  }

}