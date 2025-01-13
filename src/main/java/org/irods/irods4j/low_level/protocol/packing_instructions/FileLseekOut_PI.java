package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName("fileLseekOut_PI")
public class FileLseekOut_PI {
	
	public long offset;

}
