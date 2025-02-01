package org.irods.irods4j.low_level.api;

public class IRODSException extends Exception {
	
	private static final long serialVersionUID = 7853171723685024420L;

	private int errorCode;

	public IRODSException(int errorCode, String message) {
		super(String.format("[%d] %s", errorCode, message));
		this.errorCode = errorCode;
	}
	
	public int getErrorCode() {
		return errorCode;
	}
}
