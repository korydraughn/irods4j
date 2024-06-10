package org.irods.irods4j.low_level.protocol.packing_instructions;

import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;

public class GenQueryOut_PI {
	
	public int rowCnt;
	public int attriCnt;
	public int continueInx;
	public int totalRowCount;

	// See KeyValPair_PI.java to learn why this is necessary.
	@JacksonXmlElementWrapper(useWrapping = false)
	public List<SqlResult_PI> SqlResult_PI;

}
