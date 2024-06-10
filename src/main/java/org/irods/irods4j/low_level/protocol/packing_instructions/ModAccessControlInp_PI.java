package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName("modAccessControlInp_PI")
public class ModAccessControlInp_PI {
	
	public int recursiveFlag;
	public String accessLevel;
	public String userName;
	public String zone;
	public String path;

}
