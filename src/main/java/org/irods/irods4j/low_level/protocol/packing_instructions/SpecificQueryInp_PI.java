package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName("specificQueryInp_PI")
public class SpecificQueryInp_PI {
	
	public String sql;
	public String arg1;
	public String arg2;
	public String arg3;
	public String arg4;
	public String arg5;
	public String arg6;
	public String arg7;
	public String arg8;
	public String arg9;
	public String arg10;
	public int maxRows;
	public int continueInx;
	public int rowOffset;
	public int options;
	public KeyValPair_PI KeyValPair_PI;

}
