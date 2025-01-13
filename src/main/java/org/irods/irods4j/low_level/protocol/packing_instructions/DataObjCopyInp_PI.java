package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class DataObjCopyInp_PI {
	
	public DataObjInp_PI[] DataObjInp_PI = new DataObjInp_PI[2];
	
	public static void main(String[] args) throws JsonProcessingException {
		var xm = new XmlMapper();
		var v = new DataObjCopyInp_PI();
		System.out.println(xm.writeValueAsString(v));
	}

}
