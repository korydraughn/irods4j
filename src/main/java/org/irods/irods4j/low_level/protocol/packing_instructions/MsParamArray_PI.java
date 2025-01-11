package org.irods.irods4j.low_level.protocol.packing_instructions;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class MsParamArray_PI {

	public int paramLen;
	public int oprType;
	public List<MsParam_PI> MsParam_PI;

	public static void main(String[] args) throws JsonProcessingException {
		var m = new XmlMapper();
		System.out.println(m.writeValueAsString(new MsParamArray_PI()));
	}

}
