package org.irods.irods4j.low_level.protocol.packing_instructions;

public class OpenedDataObjInp_PI {
	
	public int l1descInx;
	public int len;
	public int whence;
	public int oprType;
//	public double offset;
//	public double bytesWritten;
	public int offset; // TODO This is a double in the iRODS C API, which is crap.
	public int bytesWritten; // TODO This is a double in the iRODS C API, which is crap.
	public KeyValPair_PI KeyValPair_PI;

}
