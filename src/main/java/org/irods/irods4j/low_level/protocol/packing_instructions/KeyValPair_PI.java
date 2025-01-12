package org.irods.irods4j.low_level.protocol.packing_instructions;

import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;

public class KeyValPair_PI {
	
	// The annotation, @JacksonXmlElementWrapper, instructs the Jackson
	// XmlMapper class to not wrap the list members. This is required by
	// the iRODS protocol and results in XML like so:
	//
	// <KeyValPair_PI>
	//   <ssLen>2</ssLen>
	//   <keyWord>forceFlag</keyWord>
	//   <keyWord>recursiveOpr</keyWord>
	//   <svalue></svalue>
	//   <svalue></svalue>
	// </KeyValPair_PI>
	
	public int ssLen;

	@JacksonXmlElementWrapper(useWrapping = false)
	public List<String> keyWord;

	@JacksonXmlElementWrapper(useWrapping = false)
	public List<String> svalue;

}
