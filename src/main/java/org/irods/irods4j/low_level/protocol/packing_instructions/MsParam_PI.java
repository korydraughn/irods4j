package org.irods.irods4j.low_level.protocol.packing_instructions;

public class MsParam_PI {
	
	public String label;
	public String type; // TODO PiStr? This tells the lib how to deserialize inOutStruct.
	public Object inOutStruct; // TODO "?type*" => Object?
	public BinBytesBuf_PI BinBytesBuf_PI;

}
