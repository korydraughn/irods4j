package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class MsgHeader_PI {

	public static final class MsgType {
		public static final String RODS_CONNECT = "RODS_CONNECT";
		public static final String RODS_CS_NEG_T = "RODS_CS_NEG_T";
		public static final String RODS_VERSION = "RODS_VERSION";
		public static final String RODS_API_REQ = "RODS_API_REQ";
		public static final String RODS_API_REPLY = "RODS_API_REPLY";
		public static final String RODS_DISCONNECT = "RODS_DISCONNECT";
	}

	public String type = MsgType.RODS_CONNECT;
	public int msgLen = 0;
	public int errorLen = 0;
	public int bsLen = 0;
	public int intInfo = 0;

	public static void main(String[] args) throws JsonProcessingException {
		XmlMapper m = new XmlMapper();
		System.out.println(m.writeValueAsString(new MsgHeader_PI()));
	}

}
