package org.irods.irods4j.common;

/**
 * A generic class which holds a reference to a specific object.
 * 
 * The class is designed for use with the low-level iRODS API functions.
 * 
 * @param <T> The type of the wrapped object.
 * 
 * @since 0.1.0
 */
public class Reference <T> {
	
	public T value;
	
	/**
	 * Initializes a newly created reference holding no value.
	 * 
	 * @since 0.1.0
	 */
	public Reference() {}
	
	/**
	 * Initializes a newly created reference with a value.
	 * 
	 * @param object The object to reference.
	 * 
	 * @since 0.1.0
	 */
	public Reference(T object) {
		this.value = object;
	}

}
