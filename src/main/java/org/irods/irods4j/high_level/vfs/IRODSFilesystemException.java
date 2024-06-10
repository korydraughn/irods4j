package org.irods.irods4j.high_level.vfs;

import org.irods.irods4j.low_level.api.IRODSException;

public class IRODSFilesystemException extends IRODSException {
	
	private static final long serialVersionUID = -5289964691795217742L;

	private String path1;
	private String path2;
	
	public IRODSFilesystemException(int errorCode) {
		super(errorCode, "");
	}
	
	public IRODSFilesystemException(int errorCode, String message) {
		super(errorCode, message);
	}
	
	public IRODSFilesystemException(int errorCode, String message, String path) {
		super(errorCode, message);
		this.path1 = path;
	}
	
	public IRODSFilesystemException(int errorCode, String message, String path1, String path2) {
		super(errorCode, message);
		this.path1 = path1;
		this.path2 = path2;
	}
	
	public String getPath1() {
		return path1;
	}

	public String getPath2() {
		return path2;
	}

}
