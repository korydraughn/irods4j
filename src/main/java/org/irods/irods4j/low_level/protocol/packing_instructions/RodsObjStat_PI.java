package org.irods.irods4j.low_level.protocol.packing_instructions;

public class RodsObjStat_PI {

	public long objSize; // This is a double in the C impl.
	public int objType;
	public int dataMode;
	public String dataId;
	public String chksum;
	public String ownerName;
	public String ownerZone;
	public String createTime;
	public String modifyTime;
	public SpecColl_PI SpecColl_PI;

}
