package org.irods.irods4j.high_level.connection;

/**
 * A class which enforces use of fully-qualified iRODS usernames.
 * 
 * Instances of this class are readonly.
 * 
 * @since 0.1.0
 */
public class QualifiedUsername {
	
	private String name;
	private String zone;
	
	/**
	 * Constructs a fully-qualified username.
	 * 
	 * @param name The part of the username preceding the pound sign.
	 * @param zone The part of the username following the pound sign.
	 * 
	 * @throws IllegalArgumentException If the name or zone is null or empty.
	 * 
	 * @since 0.1.0
	 */
	public QualifiedUsername(String name, String zone) {
		if (null == name || name.isEmpty()) {
			throw new IllegalArgumentException("Name is null or empty");
		}

		if (null == zone || zone.isEmpty()) {
			throw new IllegalArgumentException("Zone is null or empty");
		}
		
		this.name = name;
		this.zone = zone;
	}
	
	/**
	 * Returns the part of the username preceding the pound sign.
	 * 
	 * @since 0.1.0
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the part of the username following the pound sign.
	 * 
	 * @since 0.1.0
	 */
	public String getZone() {
		return zone;
	}
	
	/**
	 * Returns the fully-qualified username.
	 * 
	 * @since 0.1.0
	 */
	public String getQualifiedName() {
		return String.format("%s#%s", name, zone);
	}

}
