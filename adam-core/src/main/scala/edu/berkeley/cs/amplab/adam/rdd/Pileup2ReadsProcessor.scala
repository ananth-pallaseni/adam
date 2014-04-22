package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.avro.{Base, ADAMPileup, ADAMRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.util._
import net.sf.samtools.{CigarOperator, TextCigarCodec}
import scala.collection.JavaConverters._
import scala.collection.immutable.StringOps
import scala.collection.mutable._

private[rdd] class Pileup2ReadsProcessor extends Serializable with Logging {

	/** Converts a list of Pileups from many records back into the original records
	  *
	  * @param list A list of ADAMPileups from various reads
	  * @return A list of ADAMRecord containing the records that the pileups originally came from
	  */ 
	def convertPileupsToReads(list: List[ADAMPileup]): List[ADAMRecord] = {
		var sortedPileups: scala.collection.immutable.Map[String, List[ADAMPileup]] = sortPileupsByReadName(list)
		var records = List[ADAMRecord]()
		for((name, sortedList) <- sortedPileups) {
			records ::= pileupsToRead(sortedList)
		}
		return records
	}

	/** Converts a list of pileups from the same read into an ADAMRecord
	  * 
	  * @param list A list of ADAMPileups, all from the same read
	  * @return An ADAMRecord containing the information from the pileups 
	  * (ie - one could recreate the pileup list by passing this record into the pileup processor)
	  */
	def pileupsToRead(list: List[ADAMPileup]): ADAMRecord = {
		//Sort the list by referencePos 
		//for each pileup in the sorted list, add the readBase to the String
		var sortedList = sortListByPos(list)
		var sequence = "" // Base sequence for read
		var rawMdTag = "" 
		var mdTag = ""
		var count = 0
		for(element <- sortedList) {
			var base = element.readBase
			var refBase = element.referenceBase
			if(base != null) {
				sequence += base
			}
			if(base != refBase) {
				rawMdTag += count
				count = 0
				rawMdTag += "X"
			}	
			else if(base == refBase && element.rangeOffset == null) { //means match
				count += 1
			}
			else if(base == refBase && element.rangeOffset != null) { // means a delete
				rawMdTag += count
				count = 0
				rawMdTag += "^" // need to reprocess afterwards to remove extra carats
				rawMdTag += base
			}

			// Process mdTag to remove carats:
			var del = false
			for(i <- rawMdTag) {
				if(i != "^") {
					mdTag += i
				}
				else if(!del) { // first carat in series
					del = true
					mdTag += i
				}
			}
		}

		var examplePileup = list(0)
		var CIGAR = recreateCIGAR(list)
		var negativeStrand = false
		if(examplePileup.getNumReverseStrand > 0) {
			negativeStrand = true
		}
		ADAMRecord.newBuilder()
        	.setReferenceName(examplePileup.getReferenceName)
        	.setReferenceId(examplePileup.getReferenceId)
        	.setMapq(examplePileup.getMapQuality)
        	.setRecordGroupSequencingCenter(examplePileup.getRecordGroupSequencingCenter)
        	.setRecordGroupDescription(examplePileup.getRecordGroupDescription)
        	.setRecordGroupRunDateEpoch(examplePileup.getRecordGroupRunDateEpoch)
        	.setRecordGroupFlowOrder(examplePileup.getRecordGroupFlowOrder)
        	.setRecordGroupKeySequence(examplePileup.getRecordGroupKeySequence)
        	.setRecordGroupLibrary(examplePileup.getRecordGroupLibrary)
        	.setRecordGroupPredictedMedianInsertSize(examplePileup.getRecordGroupPredictedMedianInsertSize)
        	.setRecordGroupPlatform(examplePileup.getRecordGroupPlatform)
       		.setRecordGroupPlatformUnit(examplePileup.getRecordGroupPlatformUnit)
        	.setRecordGroupSample(examplePileup.getRecordGroupSample)
        	.setReadNegativeStrand(negativeStrand)
        	.setReadName(examplePileup.getReadName)
        	.setStart(examplePileup.getReadStart)
        	.setSequence(sequence)
        	.setMismatchingPositions(mdTag)
        	.setCigar(CIGAR)
        	.build()

	}

	
	/** Sorts a list of pileups by readname
	  * 
	  * @param list A list of ADAMPileup objects 
	  * @return A Map with readNames as keys and lists of ADAMPileups with that readname as values. 
	  */
	def sortPileupsByReadName(list: List[ADAMPileup]) : scala.collection.immutable.Map[String, List[ADAMPileup]] = {
		/*var nameMap = scala.collection.mutable.Map[String, Integer]() // Keys are read names and values are indices
		var sortedPileups:MutableList[MutableList[ADAMPileup]] = MutableList()
		for(element <- list) {
			if(nameMap.contains(element.getReadName.toString())) {
				var index = nameMap(element.getReadName.toString())
				sortedPileups(index) ::= element
			}
			else {
				var temp1 = List[ADAMPileup](element) // Has to be a better way of doing this...
				var temp2 = List[List[ADAMPileup]](temp1)
				sortedPileups = sortedPileups ::: temp2 // Append to end
				var index = sortedPileups.size - 1
				nameMap += element.getReadName.toString -> index
			}
		}
		return sortedPileups*/
		return list.groupBy(_.getReadName.toString)
	}

	/** 
	  * Reconstructs a CIGAR string from a list of Pileups 
	  *
	  * @param list - A list of pileups, all from the same read 	
	  * @return A String containing the CIGAR (ie - the various insertions, deletions and matches)
	  */
	def recreateCIGAR(list: List[ADAMPileup]): String = {
		var sortedList = sortListByPos(list)
		var CIGAR = ""
		var run = 0
		var CIGARtype = ""
		for(element <- sortedList) {
			// Check if current element is of a different type to the previous
			var newType = ""
			if(element.numSoftClipped == 1) { // Soft Clip 
				if(CIGARtype == "") {
					CIGARtype = "S"
				}
				else if(CIGARtype != "S") {
					newType = "S"
				}
			}
			else if(element.referenceBase == null) { // Insertion
				if(CIGARtype == "") {
					CIGARtype = "I"
				}
				else if(CIGARtype != "I") {
					newType = "I"
				}
			}
			else if(element.readBase == null){ // Deletion
				if(CIGARtype == "") {
					CIGARtype = "D"
				}
				else if(CIGARtype != "D") {
					newType = "D"
				}
			}
			else if(element.rangeOffset == null) { // Match
				if(CIGARtype == "") {
					CIGARtype = "M"
				}
				else if(CIGARtype != "M") {
					newType = "M"
				}
			}
			if(newType != "") { // Means the current element is not same type as previous
				CIGAR += run.toString() // Add num of run to CIGAR
				CIGAR += CIGARtype  // Add type of run to CIGAR (eg. 10M or 2D)
				CIGARtype = newType
				run = 0
			}
			run += 1
		}
		CIGAR += run.toString() // Add last run
		CIGAR += CIGARtype 
		return CIGAR
	}

	/** Sorts a list of ADAMPileups into the order of the original read
	  *
	  * @param list A list of unsorted ADAMPileups
	  * @return A list of ADAMPileups that are sorted according to where each base was in the original read
	  * 		eg: if pileup A contains base #2, pileup B contains base #3 and pileup C contains base #1, 
	  * 			the list (A,B,C) would get sorted to (C,A,B)
	  */
	def sortListByPos(list:List[ADAMPileup]): List[ADAMPileup] = {
		return list.sortBy(_.getPosition)
	}
}

// for testing purposes
object Pileup2ReadsProcessor {
	def main(args: Array[String]): Unit = {

		

		/*def make_pileup(reads : RDD[ADAMRecord]) : Array[Seq[ADAMPileup]] = {
		    reads.adamRecords2Pileup().groupBy(_.getPosition).sortByKey().map(_._2).collect()
		}*/

	}
	
}

 