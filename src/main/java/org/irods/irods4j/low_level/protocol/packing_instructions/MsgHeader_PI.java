package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class MsgHeader_PI {

	public static enum MsgType {
		RODS_CONNECT, RODS_CS_NEG_T, RODS_VERSION, RODS_API_REQ, RODS_API_REPLY, RODS_DISCONNECT
	}

	public MsgType type = MsgType.RODS_CONNECT;
	public int msgLen = 0;
	public int errorLen = 0;
	public int bsLen = 0;
	public int intInfo = 0;

	public static void main(String[] args) throws JsonProcessingException {
		XmlMapper m = new XmlMapper();
		System.out.println(m.writeValueAsString(new MsgHeader_PI()));
	}

}
