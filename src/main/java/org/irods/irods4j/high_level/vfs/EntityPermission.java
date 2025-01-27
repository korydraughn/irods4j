package org.irods.irods4j.high_level.vfs;

import org.irods.irods4j.high_level.administration.IRODSUsers.UserType;

public class EntityPermission {
	
	String name;
	String zone;
	Permission prms;
	UserType type;
	
	public String getName() {
		return name;
	}
	
	public String getZone() {
		return zone;
	}
	
	public Permission getPermission() {
		return prms;
	}
	
	public UserType getUserType() {
		return type;
	}

}
