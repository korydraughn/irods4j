package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName("int64DataArray_PI")
public class Int64DataArray_PI {
	
	public int type;
	public int len;
	public double[] buf;

}
