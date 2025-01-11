package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class CS_NEG_PI {
	
	public static final String RESULT_CS_NEG_REFUSE = "CS_NEG_REFUSE";
	public static final String RESULT_CS_NEG_DONT_CARE = "CS_NEG_DONT_CARE";
	public static final String RESULT_CS_NEG_REQUIRE = "CS_NEG_REQUIRE";
	
	public int status = 0;
	public String result = RESULT_CS_NEG_REFUSE;
	
	public static void main(String[] args) throws JsonProcessingException {
		var m = new XmlMapper();
		System.out.println(m.writeValueAsString(new CS_NEG_PI()));
	}

}
