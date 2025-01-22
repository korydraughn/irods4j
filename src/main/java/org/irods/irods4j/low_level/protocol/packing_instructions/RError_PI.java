package org.irods.irods4j.low_level.protocol.packing_instructions;

import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;

public class RError_PI {

	public int count = 0;

	// To understand this annotation, see KeyValPair_PI.
	@JacksonXmlElementWrapper(useWrapping = false)
	public List<RErrMsg_PI> RErrMsg_PI;

}
