package org.irods.irods4j.high_level.vfs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Holds information describing the status of a filesystem object.
 * 
 * @since 0.1.0
 */
public class ObjectStatus {

	private ObjectType type;
	private List<EntityPermission> perms;
	private boolean inheritance;

	/**
	 * Values which define the type of a filesystem object.
	 * 
	 * @since 0.1.0
	 */
	public static enum ObjectType {
		NONE, NOT_FOUND, DATA_OBJECT, COLLECTION, SPECIAL_COLLECTION, UNKNOWN
	}

	/**
	 * Initializes an {@code ObjectStatus} instance with an object type of
	 * {@code ObjectType.NONE}, empty permissions list, and inheritance set to
	 * false.
	 * 
	 * @since 0.1.0
	 */
	public ObjectStatus() {
		this(ObjectType.NONE, new ArrayList<>());
	}

	/**
	 * Initializes an {@code ObjectStatus} instance with a specific type and
	 * permission list. Inheritance is set to false.
	 * 
	 * @param type  The type of the filesystem object.
	 * @param perms The permissions list of the filesystem object.
	 * 
	 * @since 0.1.0
	 */
	public ObjectStatus(ObjectType type, List<EntityPermission> perms) {
		this.type = type;
		this.perms = perms;
		inheritance = false;
	}

	/**
	 * Returns the type of the filesystem object.
	 * 
	 * @since 0.1.0
	 */
	public ObjectType getType() {
		return type;
	}

	/**
	 * Returns the list of permissions for the filesystem object.
	 * 
	 * @since 0.1.0
	 */
	public List<EntityPermission> getPermissions() {
		return Collections.unmodifiableList(perms);
	}

	/**
	 * Checks whether the inheritance flag is enabled.
	 * 
	 * @since 0.1.0
	 */
	public boolean isInheritanceEnabled() {
		return inheritance;
	}

	/**
	 * Sets the object type of this {@code ObjectStatus} instance. This operation
	 * does not update the catalog.
	 * 
	 * @param type The new type of the filesystem object.
	 * 
	 * @since 0.1.0
	 */
	public void setType(ObjectType type) {
		this.type = type;
	}

	/**
	 * Sets the permissions of this {@code ObjectStatus} instance. This operation
	 * does not update the catalog.
	 * 
	 * @param perms The new permissions of the filesystem object.
	 * 
	 * @since 0.1.0
	 */
	public void setPermissions(List<EntityPermission> perms) {
		this.perms = perms;
	}

	/**
	 * Sets the inheritance flag of this {@code ObjectStatus} instance. This
	 * operation does not update the catalog.
	 * 
	 * @param value The new inheritance value of the filesystem object.
	 * 
	 * @since 0.1.0
	 */
	public void setInheritance(boolean value) {
		inheritance = value;
	}

}
