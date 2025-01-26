package org.irods.irods4j.low_level.protocol.packing_instructions;

import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;

public class InxValPair_PI {

	public int isLen;

	@JacksonXmlElementWrapper(useWrapping = false)
	public List<Integer> inx;

	@JacksonXmlElementWrapper(useWrapping = false)
	public List<String> svalue;

}
