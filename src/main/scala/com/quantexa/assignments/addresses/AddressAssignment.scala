package com.quantexa.assignments.addresses

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Random

/***
  *  You have been given a dataset containing a list of addresses, along with customers who lived at the addresses
  *  and the start and end date that they lived there. Here is an example:
  *
  *     Customer ID 	Address ID 	From date 	To_date
  *     IND0003	      ADR001	    727	        803
  *     IND0004	      ADR003	    651	        820
  *     IND0007	      ADR003	    1710	      1825
  *     IND0008	      ADR005	    29	        191
  *     IND0001	      ADR003	    1777	      1825
  *     IND0002	      ADR003	    1144        1158
  *
  *  Write an algorithm for the following:
  *
  *  "For each address, calculate all of the groups of customers who have lived at the address at overlapping times."
  *
  *  Note that each customer in the group only needs to overlap with at least one other customer in the group, so there
  *  may be pairs of customers in the group who never lived at the address at the same time.
  *
  *  The algorithm should output the following columns:
  *  •	 The address
  *  •	 The list of customers in the group
  *  •	 The first date that any customer in the group lived at the address
  *  •	 The last date that any customer in the group lived at the address
  *
  *  Example single row of output:
  *
  *  Address_ID 	Group_Customers	    Group_Start	  Group_End
  *  ADR003	      [IND0001,IND0007]	  1710	        1825
  *
  */
  
  
object AddressAssignment extends App{

  //Define a case class AddressData which stores the occupancy data
  case class AddressData(
                          customerId: String,
                          addressId: String,
                          fromDate: Int,
                          toDate: Int
                        )

  // Define a case class AddressGroupedData which stores the grouped data
  case class AddressGroupedData(
                                  group: Long,
                                  addressId: String,
                                  customerIds: Seq[String],
                                  startDate: Int,
                                  endDate: Int
                               )

  //The full path to the file to import
  val fileName = getClass.getResource("/address_data.csv").getPath

  //The lines of the CSV file (dropping the first to remove the header)
  //  Source.fromInputStream(getClass.getResourceAsStream("/address_data.csv")).getLines().drop(1)
  val addressLines = Source.fromFile(fileName).getLines().drop(1)

  val occupancyData: List[AddressData] = addressLines.map {
    line =>
      val split = line.split(',')
      AddressData(split(0), split(1), split(2).toInt, split(3).toInt)
  }.toList

  //END GIVEN CODE

  // Sort by AddressID then fromDate
  val sortedList = occupancyData.sortBy( stay => (stay.addressId, stay.fromDate))

  // Initialise values for initial group.
  val initialGroup = AddressGroupedData(0,"",List[String](), 0,0)

  // Call recursive function to group data.
  val addressGroups = groupDataByStay(sortedList, initialGroup, List[AddressGroupedData]())

  // Get number of groups found.
  val numberOfGroups = addressGroups.length

  addressGroups.sortBy(stay => (stay.addressId, stay.startDate)).foreach{println}
  println(s"Number of groups: $numberOfGroups")

  // Recursive function to iterate through addresses and group by overlapping stays
  @tailrec
  def groupDataByStay(list: List[AddressData], currentGroup: AddressGroupedData, groupedData: List[AddressGroupedData]): List[AddressGroupedData] = {
    list match {
      // Reached the end, add last group and return groupedData
      case Nil =>
        currentGroup :: groupedData
      case x :: xs =>
        // If new group or address
        if(currentGroup.addressId != x.addressId || x.fromDate > currentGroup.endDate) {
          // Complete previous group and start new one
          val newGroupedData = if(currentGroup.addressId != "") currentGroup :: groupedData else groupedData
          // Start new group
          val newCurrentGroup = AddressGroupedData(
            currentGroup.group + 1,
            x.addressId,
            List(x.customerId),
            x.fromDate,
            x.toDate
          )
          groupDataByStay(xs, newCurrentGroup, newGroupedData)
        }
        // Else add data to existing list
        else {
          val newCurrentGroup = currentGroup.copy(endDate = math.max(x.toDate, currentGroup.endDate), customerIds = currentGroup.customerIds :+ x.customerId )
          groupDataByStay(xs, newCurrentGroup, groupedData)
        }

    }
  }

}
