package org.irods.irods4j.common;

public class Reference <T> {
	
	public T value;
	
	public Reference() {}
	
	public Reference(T object) {
		this.value = object;
	}

}
