package org.irods.irods4j.high_level.connection;

import java.util.Optional;

/**
 * Defines various options for changing the behavior of a connection pool.
 * 
 * Refresh options are not mutually exclusive.
 * 
 * It is important to understand that any of the options can trigger a
 * connection refresh. When a refresh occurs, all state related to connection
 * refresh is reset. Therefore, care must be taken when setting multiple options
 * in order to obtain deterministic refresh behavior.
 * 
 * @since 0.1.0
 */
public class ConnectionPoolOptions {

	/**
	 * Defines the number of times a connection is retrieved from the pool before it
	 * is refreshed.
	 * 
	 * @since 0.1.0
	 */
	public Optional<Integer> numberOfRetrievalsBeforeConnectionRefresh = Optional.empty();

	/**
	 * Defines the number of seconds that must pass before a connection is
	 * refreshed.
	 * 
	 * @since 0.1.0
	 */
	public Optional<Integer> numberOfSecondsBeforeConnectionRefresh = Optional.empty();

	/**
	 * Instructs the connection pool to refresh a connection when a change is
	 * detected in any of the zone's resources.
	 * 
	 * @since 0.1.0
	 */
	public boolean refreshConnectionsWhenResourceChangesDetected = false;

}
