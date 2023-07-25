package in.iitpkd.scala

import org.apache.log4j._
import org.apache.spark._
import scala.math.min

object Assignment {


  def parsing(line: String) = {
    val fields = line.split(",")
    val last_name = fields(0)
    val first_name = fields(1)
    val middle_name = fields(2)
    val appointment_made_date = fields(10)
    val cancellation = fields(13)
    val caller_last_name = fields(19)
    val caller_first_name = fields(20)
    val meeting_room = fields(22)
    (last_name, first_name, middle_name, caller_last_name, caller_first_name, appointment_made_date, meeting_room, cancellation)
  }

  def main(args: Array[String]) {
    //17 Set the log Level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Create a SparkContext using every core of the Local machine, WhiteHousevisitors
    val sc = new SparkContext("local[*]", "WhiteHousevisitors")
    //Load each 1ine of the registry csv file data into an RDD
    var raw_input = sc.textFile("data/2022.07_WAVES-ACCESS-RECORDS.csv")
    val unfiltered_input = raw_input.map(parsing)
    //removing the cancelled appointments
    val input_row = unfiltered_input.filter(x => (x._8 == ""))
    // Question 1
    val visitors_full_name = input_row.map(x => x._1 + ' ' + x._2 + ' ' + x._3)
    val q1_all_visitors = visitors_full_name.countByValue()
    val q1_result = q1_all_visitors.toSeq.sortBy(_._2).reverse.take(10)
    println("First Question:")
    q1_result.foreach(println)

    // Question 2
    val visitee_full_name = input_row.map(x => x._4 + ' ' + x._5)
    val q2_all_visitee = visitee_full_name.countByValue()
    val q2_result = q2_all_visitee.toSeq.sortBy(_._2).reverse.take(10)
    println("Second question:")
    q2_result.foreach(println)

    // Question 3
    val vistor_vistee = input_row.map(x => '(' + x._1 + ' ' + x._2 + ' ' + x._3 + ' ' + ',' + ' ' + x._4 + ' ' + x._5 + ')')
    val q3_all_pairs = vistor_vistee.countByValue()
    val q3_result = q3_all_pairs.toSeq.sortBy(_._2).reverse.take(10)
    println("Third question:")
    q3_result.foreach(println)


    // Question 4
    val appointment_made_date = unfiltered_input.map(x => x._6.split( " ")(0))
    val q4_all_dates = appointment_made_date.countByValue()
    val q4_result = q4_all_dates.toSeq.sortBy(_._2).reverse.take(10)
    println("Fourth question:")
    q4_result.foreach(println)

    // Question 5
    val total_vistors = input_row.count().toDouble
    val number_of_tour_visitors = input_row.filter(x => x._7.contains("EW TOUR")).count().toDouble
    val q5_result = (number_of_tour_visitors / total_vistors) * 100
    println("Fifth Question:")
    println(q5_result)

  }
}