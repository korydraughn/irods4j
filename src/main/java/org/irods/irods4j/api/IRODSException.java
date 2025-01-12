package org.irods.irods4j.api;

public class IRODSException extends Exception {
	
	private static final long serialVersionUID = 7853171723685024420L;

	private int errorCode;

	public IRODSException(int errorCode, String message) {
		super(message);
		this.errorCode = errorCode;
	}
	
	public int getErrorCode() {
		return errorCode;
	}
}
