package org.irods.irods4j.low_level.protocol.packing_instructions;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName("strDataArray_PI")
public class StrDataArray_PI {
	
	public int type;
	public int len;
	public List<String> buf;

}
