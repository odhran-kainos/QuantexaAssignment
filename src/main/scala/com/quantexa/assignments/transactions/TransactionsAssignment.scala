/*
  Quantexa Copyright Statement
 */

package com.quantexa.assignments.transactions

import com.quantexa.assignments.transactions.TransactionAssignment.transactions

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Try

/***
  * This Object holds the functions required for the Quantexa coding exercise and an entry point to execute them.
  * Once executed each question is executed in turn printing results to console
  */
  
object TransactionAssignment extends App{

  /***
    * A case class to represent a transaction
    * @param transactionId The transaction Identification String
    * @param accountId The account Identification String
    * @param transactionDay The day of the transaction
    * @param category The category of the transaction
    * @param transactionAmount The transaction value
    */
  case class Transaction(
                          transactionId: String,
                          accountId: String,
                          transactionDay: Int,
                          category: String,
                          transactionAmount: Double
                        )

  case class Question1Result(
                             transactionDay: Int,
                             transactionTotal: Double
                            )

  case class Question2Result(
                              accountId: String,
                              categoryAvgValueMap: Map[String, Double]
                            )

  case class Question3Result(
                              transactionDay: Int,
                              accountId: String,
                              max: Double,
                              avg: Double,
                              aaTotal: Double,
                              ccTotal: Double,
                              ffTotal: Double
                            )

  //The full path to the file to import
  val fileName = getClass.getResource("/transactions.csv").getPath

  //The lines of the CSV file (dropping the first to remove the header)
  //  Source.fromInputStream(getClass.getResourceAsStream("/transactions.csv")).getLines().drop(1)
  val transactionLines: Iterator[String] = Source.fromFile(fileName).getLines().drop(1)

  //Here we split each line up by commas and construct Transactions
  val transactions: List[Transaction] = transactionLines.map { line =>
    val split = line.split(',')
    Transaction(split(0), split(1), split(2).toInt, split(3), split(4).toDouble)
  }.toList

  /*
   * 
   * END PROVIDED CODE
   * 
   */

  /*
  Question 1 - Total transaction value for all transactions for each day
   */
  def getTotalTransactionsPerDay(transactions: List[Transaction]): Seq[Question1Result] = {
    transactions.groupBy(_.transactionDay)
      .map(x => Question1Result(x._1, x._2.map(_.transactionAmount).sum))
      .toSeq
      .sortBy(_.transactionDay)
  }

  val question1ResultValue = getTotalTransactionsPerDay(transactions)
  question1ResultValue.foreach(println)

  /*
  Question 2 - Average value of transactions per account for each type of transaction
   */
  def getAverageTransactionsPerAccountByType(transactions: List[Transaction]): Seq[Question2Result] = {
    transactions.groupBy(trans => (trans.accountId, trans.category))
      .mapValues(_.map(_.transactionAmount))
      .mapValues(amounts => amounts.sum/amounts.size)
      .map(x => Question2Result(x._1._1, Map(x._1._2 -> x._2)))
      .toSeq
      .sortBy(_.accountId)
  }

  val question2ResultValue = getAverageTransactionsPerAccountByType(transactions)
  question2ResultValue.foreach(println)
  /*
  Question 3 - Statistics for each account number for the previous five days
   */
  def getStatsPerAccountForLastFiveDays(transactions: List[Transaction]) = {
    val windowSize = 5

    val transWindow = transactions.groupBy(trans =>(trans.transactionDay, trans.accountId))
      .toList.sortBy(x => (x._1._1, x._1._2))
      .sliding(windowSize)

    val statistics: Iterator[Iterable[Question3Result]] =
      for ((transactionsPeriod, index) <- transWindow.zipWithIndex) yield {
        val day = index + windowSize + 1
        val transactionsFromPeriod = transactionsPeriod.flatMap(_._2)
        val transactionsFromPeriodByAccount = transactionsFromPeriod.groupBy(_.accountId)

        val stats: Iterable[Question3Result] = transactionsFromPeriodByAccount.map {
          case (accountId, t) =>
            Question3Result(
              day,
              accountId,
              t.map(_.transactionAmount).max,
              t.map(_.transactionAmount).sum / t.size,
              t.filter(_.category == "AA").map(_.transactionAmount).sum,
              t.filter(_.category == "CC").map(_.transactionAmount).sum,
              t.filter(_.category == "FF").map(_.transactionAmount).sum)
        }
        stats
      }
    statistics.flatten.toSeq
  }

  val question3ResultValue = getStatsPerAccountForLastFiveDays(transactions)
  question3ResultValue.foreach(println)
}