package org.irods.irods4j.low_level.protocol.packing_instructions;

public class DataObjInp_PI {

	public static class PosixOpenFlags {
		// Access Mode
		public static final int O_RDONLY = 0;
		public static final int O_WRONLY = 1;
		public static final int O_RDWR = 2;
		//
		public static final int O_CREAT = 0100;
		public static final int O_TRUNC = 01000;
		public static final int O_APPEND = 02000;
	}

	public String objPath;
	public int createMode;
	public int openFlags;
//	public double offset;
//	public double dataSize;
	public int offset; // TODO This is a double in the iRODS C API, which is crap.
	public int dataSize; // TODO This is a double in the iRODS C API, which is crap.
	public int numThreads;
	public int oprType;
	// TODO Causes operations to fail unless handled using a custom serialization
	// method.
	// Will deal with this later.
//	public SpecColl_PI SpecColl_PI;
	public KeyValPair_PI KeyValPair_PI;

}
