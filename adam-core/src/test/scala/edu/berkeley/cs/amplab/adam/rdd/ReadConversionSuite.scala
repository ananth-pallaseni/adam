/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, ADAMRecord}
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite

class ReadConversionSuite extends SparkFunSuite {
	
	def make_read(start: Long = 0L,sequence : String = "AAAAA", cigar : String = "5M", mismatchPositions : String = "5", length : Int = 5, id : Int = 0) : ADAMRecord = {
		ADAMRecord.newBuilder()
			.setReadName("read" + id.toString) 
		  	.setStart(start)
		  	.setReadMapped(true)
		  	.setSequence(sequence)
		  	.setCigar(cigar)
		  	.setReadNegativeStrand(false)
		  	.setMismatchingPositions(mismatchPositions)
		  	.setMapq(60)
		  	.setQual(sequence)
		  	.build()  		 	
	}

	def make_pileup(read: ADAMRecord) : List[ADAMPileup] = {
		var converter = new Reads2PileupProcessor
		return converter.readToPileups(read)
	}	

	test("Make read works") {
		var p = make_read()
		assert(p.getReadName == "read0")
		assert(p.getSequence == "AAAAA")
	}

	test("make pileup works") {
		var read = make_read()
		var c = new Reads2PileupProcessor
		var p = c.readToPileups(read)
		assert(p.length > 0)
	}

	test("Sort works correctly") {
		var converter = new Pileup2ReadsProcessor
		var pileupList:List[ADAMPileup] = make_pileup(make_read())
		var sortedList:List[ADAMPileup] = converter.sortListByPos(pileupList)
		for(i <- 0 to sortedList.length-1) {
			assert(sortedList(i).getPosition == i)
		}
	}

	test("Basic CIGAR properly reconstructed") {
		var converter = new Pileup2ReadsProcessor
		var read = make_read()
		var CIGAR = read.getCigar
		var pileupList:List[ADAMPileup] = make_pileup(read)
		var sortedList:List[ADAMPileup] = converter.sortListByPos(pileupList)
		var recon = converter.recreateCIGAR(sortedList)
		assert(CIGAR == recon)
	}

	test("More Complex CIGAR properly reconstructed") {
		var converter = new Pileup2ReadsProcessor
		var read = make_read(0, "AAAAAAAAAA", "2M1D6M", "2^C6", 10, 0)
		var CIGAR = read.getCigar
		var pileupList:List[ADAMPileup] = make_pileup(read)
		var sortedList:List[ADAMPileup] = converter.sortListByPos(pileupList)
		var recon = converter.recreateCIGAR(sortedList)
		assert(CIGAR == recon)
	}

	test("CIGAR Test 2") {
		var converter = new Pileup2ReadsProcessor
		var read = make_read(0, "AAAAAAAAAA", "2I2M1D6M", "2^C6", 10, 0)
		var CIGAR = read.getCigar
		var pileupList:List[ADAMPileup] = make_pileup(read)
		var sortedList:List[ADAMPileup] = converter.sortListByPos(pileupList)
		var recon = converter.recreateCIGAR(sortedList)
		println("CIGAR output:")
		println(CIGAR)
		println(recon)
		assert(CIGAR == recon)
	}

	
}