package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

@JsonRootName("generalAdminInp_PI")
public class GeneralAdminInp_PI {

	public String arg0;
	public String arg1;
	public String arg2;
	public String arg3;
	public String arg4;
	public String arg5;
	public String arg6;
	public String arg7;
	public String arg8;
	public String arg9;

	public static void main(String[] args) throws JsonProcessingException {
		var m = new XmlMapper();
		System.out.println(m.writeValueAsString(new GeneralAdminInp_PI()));
	}

}
