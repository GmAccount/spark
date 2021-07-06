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
                    duration: Int,
                    country: String,
                    language: String,
                    director: String

                  )

case class Movies3(
                    id: String,
                    directedBY: String,
                    note: Int,
                    nb_vues: Int,
                    genre: String,
                    title: String
                  )
object tp01 {



  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")

    Logger.getLogger("org").setLevel(Level.OFF)
    val session = SparkSession.builder().master("local").getOrCreate()
    val sc = session.sparkContext
    val movieRDD = sc.textFile("films01.csv")
    val header = movieRDD.first()
    //print(header)
    val movieRDDWithoutHeader = movieRDD.filter(e => e!=header)

     //movieRDDWithoutHeader.map(elem => elem.split(",")(6)).foreach(println)

    val movieObjectRDD = movieRDDWithoutHeader.map(elem => (Movies3(elem.split(",")(0),
        elem.split(",")(1),
        elem.split(",")(2).toInt,
        elem.split(",")(3).toInt,
        elem.split(",")(4),
        elem.split(",")(5))))

    // 3 count of movies directed by Leonardo Di Caprio
   val rddLeonardo = movieObjectRDD.filter(elem=>elem.directedBY.equals("Leonardo Di Caprio"))
   val cntLeonardoDiCaprio=rddLeonardo.count()


    val list02 = rddLeonardo.map(elem => elem.note).collect().toList
    val sumNotesLeonardoDiCaprio = list02.reduceLeft((x,y) => x+y)
    println(sumNotesLeonardoDiCaprio)
    println(cntLeonardoDiCaprio)

    println("moy est : "+(sumNotesLeonardoDiCaprio.toDouble/cntLeonardoDiCaprio.toDouble))

    // 4
    val list03 = rddLeonardo.map(elem => elem.nb_vues).collect().toList
    val sumViewsLeonardoDiCaprio = list03.reduceLeft((x,y) => x+y)
    println(sumViewsLeonardoDiCaprio)
    val sumViewsTotal = movieObjectRDD.map(elem => elem.nb_vues).collect().toList.reduceLeft((x,y) => x+y)
    println(sumViewsTotal)

    println("moy de vu est  : "+((sumViewsLeonardoDiCaprio.toDouble/sumViewsTotal.toDouble))*100)

    // 5

    val pairs = movieObjectRDD.map(x => (x.directedBY, (x.note,1)))

    //pairs.take(10)foreach(println)
    // 5.1
    val res03 = pairs.reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2))
    res03.map(x => (x._1, x._2._1 / x._2._2)).foreach(println)
    // 5.1.1

    val pairs02 = movieObjectRDD.map(x => (x.directedBY, (x.nb_vues,1)))

    val res04 = pairs02.reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2))
    res04.map(x => (x._1, x._2._1 / x._2._2)).foreach(println)

  }


  def getDataByDirector(id1: String, id2: String): Boolean = {
    id1 == id2
  }

}