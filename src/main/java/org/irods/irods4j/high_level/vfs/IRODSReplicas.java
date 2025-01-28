package org.irods.irods4j.high_level.vfs;

import org.irods.irods4j.api.IRODSApi.RcComm;

/**
 * A class providing high-level data types and functions for working with
 * replicas.
 * 
 * @since 0.1.0
 */
public class IRODSReplicas {

	/**
	 * Defines values which influence the behavior of checksum calculations.
	 * 
	 * @since 0.1.0
	 */
	public static enum VerificationCalculation {
		IF_EMPTY, ALWAYS
	}

	/**
	 * Returns the size of a replica in the catalog.
	 * 
	 * @param comm          The connection to an iRODS server.
	 * @param logicalPath   The absolute path to a data object.
	 * @param replicaNumber The replica number of the target replica.
	 * 
	 * @return An integer representing the size.
	 * 
	 * @since 0.1.0
	 */
	public static long replicaSize(RcComm comm, String logicalPath, int replicaNumber) {
		return 0; // TODO
	}

	/**
	 * Returns the size of a replica in the catalog.
	 * 
	 * @param comm             The connection to an iRODS server.
	 * @param logicalPath      The absolute path to a data object.
	 * @param leafResourceName The resource holding the target replica.
	 * 
	 * @return An integer representing the size.
	 * 
	 * @since 0.1.0
	 */
	public static long replicaSize(RcComm comm, String logicalPath, String leafResourceName) {
		return 0; // TODO
	}

	/**
	 * Returns the physical size of a replica in storage.
	 * 
	 * @param comm          The connection to an iRODS server.
	 * @param logicalPath   The absolute path to a data object.
	 * @param replicaNumber The replica number of the target replica.
	 * 
	 * @return An integer representing the size.
	 * 
	 * @since 0.1.0
	 */
	public static long replicaSizeInStorage(RcComm comm, String logicalPath, int replicaNumber) {
		return 0; // TODO
	}

	/**
	 * Returns the physical size of a replica in storage.
	 * 
	 * @param comm             The connection to an iRODS server.
	 * @param logicalPath      The absolute path to a data object.
	 * @param leafResourceName The resource holding the target replica.
	 * 
	 * @return An integer representing the size.
	 * 
	 * @since 0.1.0
	 */
	public static long replicaSizeInStorage(RcComm comm, String logicalPath, String leafResourceName) {
		return 0; // TODO
	}

	/**
	 * Replicates a replica to a resource.
	 * 
	 * @param comm             The connection to an iRODS server.
	 * @param logicalPath      The absolute path to the data object which owns the
	 *                         source replica.
	 * @param srcReplicaNumber The replica number of the source replica to read
	 *                         from.
	 * @param dstResourceName  The root resource to replicate the data to.
	 * 
	 * @since 0.1.0
	 */
	public static void replicateReplica(RcComm comm, String logicalPath, int srcReplicaNumber, String dstResourceName) {
		// TODO
	}

	/**
	 * Replicates a replica to a resource.
	 * 
	 * @param comm            The connection to an iRODS server.
	 * @param logicalPath     The absolute path to the data object which owns the
	 *                        source replica.
	 * @param srcResourceName The resource of the source replica to read from.
	 * @param dstResourceName The root resource to replicate the data to.
	 * 
	 * @since 0.1.0
	 */
	public static void replicateReplica(RcComm comm, String logicalPath, String srcResourceName,
			String dstResourceName) {
		// TODO
	}

	/**
	 * Trims a replica.
	 * 
	 * @param comm          The connection to an iRODS server.
	 * @param logicalPath   The absolute path to the data object which owns the
	 *                      target replica.
	 * @param replicaNumber The replica number of the replica to trim.
	 * 
	 * @since 0.1.0
	 */
	public static void trimReplica(RcComm comm, String logicalPath, int replicaNumber) {
		// TODO
	}

	/**
	 * Trims a replica.
	 * 
	 * @param comm             The connection to an iRODS server.
	 * @param logicalPath      The absolute path to the data object which owns the
	 *                         target replica.
	 * @param leafResourceName The resource holding the replica to trim.
	 * 
	 * @since 0.1.0
	 */
	public static void trimReplica(RcComm comm, String logicalPath, String leafResourceName) {
		// TODO
	}

	/**
	 * Optionally calculates and returns the checksum of a replica.
	 * 
	 * @param comm          The connection to an iRODS server.
	 * @param logicalPath   The absolute path to the data object which owns the
	 *                      source replica.
	 * @param replicaNumber The replica number of the replica to checksum.
	 * @param calculation   An option which influences the behavior of the checksum
	 *                      operation.
	 * 
	 * @return A string representing the checksum.
	 * 
	 * @since 0.1.0
	 */
	public static String replicaChecksum(RcComm comm, String logicalPath, int replicaNumber,
			VerificationCalculation calculation) {
		return null; // TODO
	}

	/**
	 * Optionally calculates and returns the checksum of a replica.
	 * 
	 * @param comm             The connection to an iRODS server.
	 * @param logicalPath      The absolute path to the data object which owns the
	 *                         source replica.
	 * @param leafResourceName The resource holding the target replica.
	 * @param calculation      An option which influences the behavior of the
	 *                         checksum operation.
	 * 
	 * @return A string representing the checksum.
	 * 
	 * @since 0.1.0
	 */
	public static String replicaChecksum(RcComm comm, String logicalPath, String leafResourceName,
			VerificationCalculation calculation) {
		return null; // TODO
	}

	/**
	 * Returns the modification time of a replica.
	 * 
	 * @param comm          The connection to an iRODS server.
	 * @param logicalPath   The absolute path to the data object which owns the
	 *                      replica.
	 * @param replicaNumber The replica number of the target replica.
	 * 
	 * @return An integer representing the time.
	 * 
	 * @since 0.1.0
	 */
	public static long lastWriteTime(RcComm comm, String logicalPath, int replicaNumber) {
		return 0; // TODO
	}

	/**
	 * Returns the modification time of a replica.
	 * 
	 * @param comm             The connection to an iRODS server.
	 * @param logicalPath      The absolute path to the data object which owns the
	 *                         replica.
	 * @param leafResourceName The resource holding the target replica.
	 * 
	 * @return An integer representing the time.
	 * 
	 * @since 0.1.0
	 */
	public static long lastWriteTime(RcComm comm, String logicalPath, String leafResourceName) {
		return 0; // TODO
	}

	/**
	 * Sets the modification time of a replica.
	 * 
	 * @param comm          The connection to an iRODS server.
	 * @param logicalPath   The absolute path to the data object which owns the
	 *                      replica.
	 * @param replicaNumber The replica number of the target replica.
	 * @param newTime       The new modification time in seconds since epoch.
	 * 
	 * @since 0.1.0
	 */
	public static void lastWriteTime(RcComm comm, String logicalPath, int replicaNumber, long newTime) {
		// TODO
	}

	/**
	 * Sets the modification time of a replica.
	 * 
	 * @param comm             The connection to an iRODS server.
	 * @param logicalPath      The absolute path to the data object which owns the
	 *                         replica.
	 * @param leafResourceName The resource holding the target replica.
	 * @param newTime          The new modification time in seconds since epoch.
	 * 
	 * @since 0.1.0
	 */
	public static void lastWriteTime(RcComm comm, String logicalPath, String leafResourceName, long newTime) {
		// TODO
	}

	/**
	 * Returns the replica number of a replica.
	 * 
	 * @param comm             The connection to an iRODS server.
	 * @param logicalPath      The absolute path to the data object which owns the
	 *                         replica.
	 * @param leafResourceName The resource holding the target replica.
	 * 
	 * @return An integer representing the replica number.
	 * 
	 * @since 0.1.0
	 */
	public static int toReplicaNumber(RcComm comm, String logicalPath, String leafResourceName) {
		return 0; // TODO
	}

	/**
	 * Returns the name of the leaf resource which holds a replica.
	 * 
	 * @param comm          The connection to an iRODS server.
	 * @param logicalPath   The absolute path to the data object which owns the
	 *                      replica.
	 * @param replicaNumber The replica number of the target replica.
	 * 
	 * @return A string representing the resource name.
	 * 
	 * @since 0.1.0
	 */
	public static String toLeafResource(RcComm comm, String logicalPath, int replicaNumber) {
		return null; // TODO
	}

	/**
	 * Checks if a replica exists in the catalog.
	 * 
	 * @param comm          The connection to an iRODS server.
	 * @param logicalPath   The absolute path to the data object which owns the
	 *                      replica.
	 * @param replicaNumber The replica number of the target replica.
	 * 
	 * @return A boolean indicating whether the target replica exists.
	 * 
	 * @since 0.1.0
	 */
	public static boolean replicaExists(RcComm comm, String logicalPath, int replicaNumber) {
		return false; // TODO
	}

	/**
	 * Checks if a replica exists in the catalog.
	 * 
	 * @param comm             The connection to an iRODS server.
	 * @param logicalPath      The absolute path to the data object which owns the
	 *                         replica.
	 * @param leafResourceName The resource holding the target replica.
	 * 
	 * @return A boolean indicating whether the target replica exists.
	 * 
	 * @since 0.1.0
	 */
	public static boolean replicaExists(RcComm comm, String logicalPath, String leafResourceName) {
		return false; // TODO
	}

}
