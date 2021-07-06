import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.concurrent.ExecutionContext.Implicits.global

//import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._

object gdpr_services {

  def readData(path: String, fileType: String)(implicit sparkSession: SparkSession): Either[String, DataFrame] = {
    fileType match {
      case "CSV" => Right(sparkSession.read.option("inferSchema", true).option("header", true).csv(path))
      case "PARQUET" => Right(sparkSession.read.parquet(path))
      case _ => Left("File format is not allowed")
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    implicit val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val maybeDataFrame = readData("data01.csv", "CSV")
    val df = maybeDataFrame.right.get
    println(df.show(4))
    val result = Service1.filterClientIdNotEq(df, "1")
    println(result.show(12))

    result.write.option("header", "false")
      .mode("overwrite")
      .save("data_step01.csv")

    println(result.show(12))

    val resultOneClient = Service1.filterClientIdEq(df, "1")
    println(resultOneClient.show(12))
    val resultOneClient01 = resultOneClient.withColumn("Nom",base64(col("Nom")))
    val resultOneClient02 = resultOneClient01.withColumn("Prenom",base64(col("Prenom")))
    //println(resultOneClient02.show(12))



    val mergeDf = resultOneClient02.union(result)
    println(mergeDf.show(15))



    // 3

    // a more convenient way to create an email
    /*
    import net.kaliber.mailer._

    val eml = Email(
      subject = "Test mail ADL",
      from = EmailAddress("badre L", "laghmich@gmail.com"),
      text = "hello from ADL",
      htmlText = "htmlText",
      replyTo = None,
      recipients = List(Recipient(
        RecipientType.TO, EmailAddress("badre L", "laghmich@gmail.com"))),
      attachments = Seq.empty)



    val mailerSettings = MailerSettings(
      protocol = Some("smtps"),
      host = "smtp.gmail.com",
      port = "465",
      failTo = "failto+customer@company.org",
      auth = Some(true),
      username = Some("badre.gm@gmail.com"),
      password = Some("****"))
     */

    // val result_eml = new Mailer(Session.fromSetting(mailerSettings)).sendEmail(eml)
    /*
    import courier._, Defaults._
    println("step001")

    val mailer = Mailer("smtp.gmail.com", 587)
      .auth(true)
      .as("badre.gm@gmail.com", "****")
      .startTls(true)()

    mailer(Envelope.from("badre.gm" `@` "gmail.com")
      .to("laghmich" `@` "gmail.com")
      .subject("adl test")
      .content(Text("hi adl test"))).onSuccess {
      case _ => println("delivered report")
    }

    println("step002")

    */
    import com.github.jurajburian.mailer._
    println("------------")
    val session = (SmtpAddress("smtp.gmail.com", 465) :: SessionFactory()).session(Some("badre.test@gmail.com"-> "****"))

    println("===========")
    import javax.mail.internet.InternetAddress
    val content = new Content().text("Hello there!")
    val msg = Message(
      from = new InternetAddress("badre.test@gmail.com"),
      subject = "my subject",
      content = content,
      to = Seq(new InternetAddress("test@gmail.com")))
    val mailer = Mailer(session)
    // recomendations: use try

    mailer.send(msg)
    mailer.close()

    /*
    mailer(Envelope.from("you" `@` "work.com")
      .to("boss" `@` "work.com")
      .subject("tps report")
      .content(Multipart()
        .attach(new java.io.File("tps.xls"))
        .html("<html><body><h1>IT'S IMPORTANT</h1></body></html>")))
      .onSuccess {
        case _ => println("delivered report")
      }
    */

    //val resultService2 = Service2.hashIdColumn(df, "name03")
    //println(resultService2.show(12))
    //resultService2.write.csv("")

  }
}


//Service1
import org.apache.spark.sql.DataFrame

object Service1 {
  def filterClientIdNotEq(df: DataFrame, clientIdToDelete: String): DataFrame = {
    df.filter(s"ID != $clientIdToDelete")
  }
  def filterClientIdEq(df: DataFrame, clientIdToDelete: String): DataFrame = {
    df.filter(s"ID == $clientIdToDelete")
  }
}

//Service2
import org.apache.spark.sql.DataFrame

object Service2 {
  def hashIdColumn(df: DataFrame, columnNameToHash: String): DataFrame = {
    df.withColumn(columnNameToHash, ???)
    ???
  }
}
