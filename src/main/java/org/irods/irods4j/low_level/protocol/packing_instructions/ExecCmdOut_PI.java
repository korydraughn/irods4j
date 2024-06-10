package org.irods.irods4j.low_level.protocol.packing_instructions;

import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;

public class ExecCmdOut_PI {
	
	// To understand this annotation, see KeyValPair_PI.
	@JacksonXmlElementWrapper(useWrapping = false)
	public List<BinBytesBuf_PI> BinBytesBuf_PI;
	public int status;

}
