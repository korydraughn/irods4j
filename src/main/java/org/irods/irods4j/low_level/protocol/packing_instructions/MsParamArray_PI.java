package org.irods.irods4j.low_level.protocol.packing_instructions;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;

public class MsParamArray_PI {

	public int paramLen;
	public int oprType;

	// To understand this annotation, see KeyValPair_PI.
	@JacksonXmlElementWrapper(useWrapping = false)
	public List<MsParam_PI> MsParam_PI;

	public static void main(String[] args) throws JsonProcessingException {
		XmlMapper m = new XmlMapper();
		System.out.println(m.writeValueAsString(new MsParamArray_PI()));
	}

}
