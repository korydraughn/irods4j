package org.irods.irods4j.low_level.protocol.packing_instructions;

public class CollEnt_PI {

	public int objType;
	public int replNum;
	public int replStatus;
	public int dataMode;
	public long dataSize; // This is a double in the C impl.
	public String collName;
	public String dataName;
	public String dataId;
	public String createTime;
	public String modifyTime;
	public String chksum;
	public String resource;
	public String resc_hier;
	public String phyPath;
	public String ownerName;
	public String dataType;
	public SpecColl_PI SpecColl_PI;

}
