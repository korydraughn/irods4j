package org.irods.irods4j.low_level.protocol.packing_instructions;

public class GenQueryInp_PI {
	
	// TODO Perhaps this library only supports GenQuery2, because
	// why would anyone want to use GenQuery1 when GenQuery2 is significantly
	// more powerful (even if it takes more work and care to use).

	public int maxRows;
	public int continueInx;
	public int partialStartIndex;
	public int options;
	public KeyValPair_PI KeyValPair_PI;
	public InxIvalPair_PI InxIvalPair_PI;
	public InxValPair_PI InxValPair_PI;

}
