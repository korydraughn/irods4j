package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName("intDataArray_PI")
public class IntDataArray_PI {
	
	public int type;
	public int len;
	public int[] buf;

}
