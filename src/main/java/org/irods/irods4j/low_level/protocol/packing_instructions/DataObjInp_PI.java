package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

public class DataObjInp_PI {

	public static class OpenFlags {
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
	public long offset; // This is a double in the C impl.
	public long dataSize; // This is a double in the C impl.
	public int numThreads;
	public int oprType;

	@JsonInclude(Include.NON_NULL)
	public SpecColl_PI SpecColl_PI;

	public KeyValPair_PI KeyValPair_PI;

}
