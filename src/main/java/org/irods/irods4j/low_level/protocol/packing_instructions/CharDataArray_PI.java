package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName("charDataArray_PI")
public class CharDataArray_PI {
	
	public int type;
	public int len;
	public byte[] buf;

}
