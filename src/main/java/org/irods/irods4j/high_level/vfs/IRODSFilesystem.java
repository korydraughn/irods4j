package org.irods.irods4j.high_level.vfs;

import org.irods.irods4j.api.IRODSApi.RcComm;

/**
 * TODO
 * 
 * @since 0.1.0
 */
public class IRODSFilesystem {
	
	/**
	 * TODO
	 */
	public static enum RemoveOptions {
		NONE, NO_TRASH
	}
	
	/**
	 * TODO
	 */
	public static final class ExtendedRemoveOptions {
		public boolean noTrash = false;
		public boolean verbose = false;
		public boolean progress = false;
		public boolean recursive = false;
		public boolean unregister = false;
	}
	
	/**
	 * TODO
	 */
	public static final class CopyOptions {
		public static final int NONE = 0;
	}
	
	/**
	 * TODO
	 */
	public static final class EntityPermission {
		public String name;
		public String zone;
		public Permission prms;
		public String type;
	}
	
	// TODO Consider porting metadata types and functions.
	
	// TODO Consider moving this into a shared/common location/package.
	private static final class AdminTag {}
	
	/**
	 * TODO
	 */
	public static final AdminTag asAdmin = new AdminTag();
	
	public static void copy(RcComm comm, String from, String to, int copyOptions) {
		// TODO
	}

	public static void copy(RcComm comm, String from, String to) {
		copy(comm, from, to, CopyOptions.NONE);
	}
	
	public static boolean copyDataObject(RcComm comm, String from, String to, int copyOptions) {
		return false; // TODO
	}

	public static boolean copyDataObject(RcComm comm, String from, String to) {
		return copyDataObject(comm, from, to, CopyOptions.NONE);
	}
	
	public static boolean createCollection(RcComm comm, String path) {
		return false; // TODO
	}

	public static boolean createCollection(RcComm comm, String path, String existingPath) {
		return false; // TODO
	}

	public static boolean createCollections(RcComm comm, String path) {
		return false; // TODO
	}

	public static boolean exists(ObjectStatus status) {
		return false; // TODO
	}

	public static boolean exists(RcComm comm, String path) {
		return false; // TODO
	}
	
	public static boolean isCollectionRegistered(RcComm comm, String path) {
		return false; // TODO
	}

	public static boolean isDataObjectRegistered(RcComm comm, String path) {
		return false; // TODO
	}
	
	public static boolean equivalent(RcComm comm, String path1, String path2) {
		return false; // TODO This function may not be implemented in this port.
	}
	
	public static long dataObjectSize(RcComm comm, String path) {
		return -1;
	}
	
	public static boolean isCollection(ObjectStatus status) {
		return false; // TODO
	}

	public static boolean isCollection(RcComm comm, String path) {
		return false; // TODO
	}

	public static boolean isDataObject(ObjectStatus status) {
		return false; // TODO
	}

	public static boolean isDataObject(RcComm comm, String path) {
		return false; // TODO
	}

	public static boolean isOther(ObjectStatus status) {
		return false; // TODO
	}

	public static boolean isOther(RcComm comm, String path) {
		return false; // TODO
	}

	public static boolean isSpecialCollection(RcComm comm, String path) {
		return false; // TODO
	}

	public static boolean isEmpty(RcComm comm, String path) {
		return false; // TODO
	}

	public static long lastWriteTime(RcComm comm, String path) {
		return -1; // TODO
	}

	public static void lastWriteTime(RcComm comm, String path, long newTime) {
		// TODO
	}
	
	public static boolean remove(RcComm comm, String path, RemoveOptions removeOptions) {
		return false; // TODO
	}

	public static boolean remove(RcComm comm, String path) {
		return remove(comm, path, RemoveOptions.NONE);
	}

	public static boolean remove(RcComm comm, String path, ExtendedRemoveOptions removeOptions) {
		return false; // TODO
	}

	public static boolean removeAll(RcComm comm, String path, RemoveOptions removeOptions) {
		return false; // TODO
	}

	public static boolean removeAll(RcComm comm, String path) {
		return removeAll(comm, path, RemoveOptions.NONE);
	}

	public static boolean removeAll(RcComm comm, String path, ExtendedRemoveOptions removeOptions) {
		return false; // TODO
	}
	
	public static void permissions(RcComm comm, String path, String userOrGroup, Permission prms) {
		// TODO
	}

	public static void permissions(AdminTag adminTag, RcComm comm, String path, String userOrGroup, Permission prms) {
		// TODO
	}
	
	public static void enableInheritance(RcComm comm, String path, boolean enable) {
		// TODO
	}

	public static void enableInheritance(AdminTag adminTag, RcComm comm, String path, boolean enable) {
		// TODO
	}
	
	public static void rename(RcComm comm, String from, String to) {
		// TODO
	}

	public static ObjectStatus status(RcComm comm, String path) {
		return null; // TODO
	}

	public static boolean statusKnown(ObjectStatus status) {
		return false; // TODO
	}

	public static String dataObjectChecksum(RcComm comm, String path) {
		return null; // TODO
	}

}
