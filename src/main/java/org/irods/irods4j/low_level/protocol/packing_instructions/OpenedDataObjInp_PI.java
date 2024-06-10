package org.irods.irods4j.low_level.protocol.packing_instructions;

public class OpenedDataObjInp_PI {
	
	public int l1descInx;
	public int len;
	public int whence;
	public int oprType;
	public long offset; // This is a double in the C impl.
	public long bytesWritten; // This is a double in the C impl.
	public KeyValPair_PI KeyValPair_PI;

}
