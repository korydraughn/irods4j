package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class StartupPack_PI {
	
	public int irodsProt = 1;
	public int reconnFlag = 0;
	public int connectCnt = 0;

	public String proxyUser = "rods";
	public String proxyRcatZone = "tempZone";

	public String clientUser = "rods";
	public String clientRcatZone = "tempZone";

	public String relVersion = "rods4.3.2";
	public String apiVersion = "d";

	public String option = "";
	
	public static void main(String[] args) throws JsonProcessingException {
		var m = new XmlMapper();
		System.out.println(m.writeValueAsString(new StartupPack_PI()));
	}

}
