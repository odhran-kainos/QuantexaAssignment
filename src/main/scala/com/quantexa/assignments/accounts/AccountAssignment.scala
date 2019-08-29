package com.quantexa.assignments.accounts

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.scalalang.typed

/***
 * A common problem we face at Quantexa is having lots of disjointed raw sources of data and having to aggregate and collect
 * relevant pieces of information into hierarchical case classes which we refer to as Documents. This exercise simplifies
 * the realities of this with just two sources of high quality data however reflects the types of transformations we must
 * perform.
 *
 * You have been given customerData and accountData. This has been read into a DataFrame for you and then converted into a
 * Dataset of the given case classes.
 *
 * If you run this App you will see the top 20 lines of the Datasets provided printed to console
 *
 * This allows you to use the Dataset API which includes .map/.groupByKey/.joinWith ect.
 * But a Dataset also includes all of the DataFrame functionality allowing you to use .join ("left-outer","inner" ect)
 *
 * https://spark.apache.org/docs/latest/sql-programming-guide.html
 *
 * The challenge here is to group, aggregate, join and map the customer and account data given into the hierarchical case class
 * customerAccountoutput. We would prefer you to write using the Scala functions instead of spark.sql() if you choose to perform
 * Dataframe transformations. We also prefer the use of the Datasets API.
 *
 * Example Answer Format:
 *
 * val customerAccountOutputDS: Dataset[customerAccountOutput] = ???
 * customerAccountOutputDS.show(1000,truncate = false)
 *
 * +----------+-----------+----------+---------------------------------------------------------------------+--------------+------------+-----------------+
 * |customerId|forename   |surname   |accounts                                                             |numberAccounts|totalBalance|averageBalance   |
 * +----------+-----------+----------+---------------------------------------------------------------------+--------------+------------+-----------------+
 * |IND0001   |Christopher|Black     |[]                                                                   |0             |0           |0.0              |
 * |IND0002   |Madeleine  |Kerr      |[[IND0002,ACC0155,323], [IND0002,ACC0262,60]]                        |2             |383         |191.5            |
 * |IND0003   |Sarah      |Skinner   |[[IND0003,ACC0235,631], [IND0003,ACC0486,400], [IND0003,ACC0540,53]] |3             |1084        |361.3333333333333|
 * ...
 */
object AccountAssignment extends App {

  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("AccountAssignment").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]
  import spark.implicits._

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)
  
  //Get file paths from files stored in project resources
  val customerCSV = getClass.getResource("/customer_data.csv").getPath
  val accountCSV = getClass.getResource("/account_data.csv").getPath

  //Create DataFrames of sources
  val customerDF = spark.read.option("header","true")
    .csv(customerCSV)
  val accountDF = spark.read.option("header","true")
    .csv(accountCSV)

  case class CustomerData(
                          customerId: String,
                          forename: String,
                          surname: String
                        )

  case class AccountData(
                          customerId: String,
                          accountId: String,
                          balance: Long
                        )

  //Expected Output Format
  case class CustomerAccountOutput(
                                    customerId: String,
                                    forename: String,
                                    surname: String,
                                    //Accounts for this customer
                                    accounts: Seq[AccountData],
                                    //Statistics of the accounts
                                    numberAccounts: Int,
                                    totalBalance: Long,
                                    averageBalance: Double
                                  )

  //Create Datasets of sources
  val customerDS = customerDF.as[CustomerData]
  val accountDS = accountDF.withColumn("balance",'balance.cast("long")).as[AccountData]

  customerDS.show
  accountDS.show

  //END GIVEN CODE

  // Get all aggregate data for each customer
  val customerAggregatesDF = accountDS.groupByKey(_.customerId)
    .agg(typed.count(_.accountId), typed.sum(_.balance), typed.avg(_.balance))
    .toDF("customerId", "numberAccounts", "totalBalance", "averageBalance")

  // Get list of accounts for each customer
  val customerAccountsDF = accountDS
    .groupByKey(_.customerId)
    .mapGroups { case (key, accountData) => (key, accountData.toArray) }
    .toDF("customerId", "accounts")


  //customerAggregatesDF.show
  //customerAccountsDF.show

  // Combine all data together and cast to dataset.
  val customerAccountOutputDS = customerDS
    .join(customerAccountsDF, Seq("customerId"), "left_outer")
    .join(customerAggregatesDF, Seq("customerId"), "left_outer")
    .withColumn("numberAccounts", 'numberAccounts.cast("Int"))
    .withColumn("totalBalance", 'totalBalance.cast("BigInt"))
    .na.fill(0, Seq("numberAccounts","totalBalance","averageBalance"))
    //.na.fill("", Seq("accounts"))  ::TODO Not able to get this working!!!!
    .as[CustomerAccountOutput]

  customerAccountOutputDS.show
}
