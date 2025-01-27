package org.irods.irods4j.low_level.protocol.packing_instructions;

public class CollInfo_PI {

	public long collId; // This is a double in the C impl.
	public String collName;
	public String collParentName;
	public String collOwnerName;
	public String collOwnerZone;
	public int collMapId;
	public int collAccessInx;
	public String collComments;
	public String collInheritance;
	public String collExpiry;
	public String collCreate;
	public String collModify;
	public String collAccess;
	public String collType;
	public String collInfo1;
	public String collInfo2;
	public KeyValPair_PI KeyValPair_PI;
	public int next;

}
