package org.irods.irods4j.low_level.protocol.packing_instructions;

public class CollOprStat_PI {
	
	public int filesCnt;
	public int totalFileCnt;
	public long bytesWritten; // This is a double in the C impl.
	public String lastObjPath;

}
