package org.irods.irods4j.high_level.connection;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.irods.irods4j.api.IRODSApi.ConnectionOptions;
import org.irods.irods4j.api.IRODSApi.RcComm;

/**
 * TODO
 * 
 * @since 0.1.0
 */
public class IRODSConnectionPool implements AutoCloseable {

	private String host;
	private int port;

	private QualifiedUsername clientUser;
	private QualifiedUsername proxyUser;

	private ConnectionOptions connOptions;
	private List<ConnectionContext> pool;

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
	public static final class ConnectionPoolOptions {
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
	
	private static final class ConnectionContext {
		Lock lock = new ReentrantLock();
		AtomicBoolean inUse = new AtomicBoolean();
		boolean refresh;
		IRODSConnection conn;
		long ctime;
		String latestRescMTime;
		int rescCount;
		int retrievalCount;
	}

	/**
	 * TODO
	 * 
	 * @since 0.1.0
	 */
	public static final class Connection implements AutoCloseable {
		private RcComm comm;
		
		/**
		 * TODO
		 * 
		 * @since 0.1.0
		 */
		private Connection() {
		}

		/**
		 * TODO
		 * 
		 * @return
		 * 
		 * @since 0.1.0
		 */
		public boolean isValid() {
			return false; // TODO
		}

		/**
		 * TODO
		 * 
		 * @return
		 * 
		 * @since 0.1.0
		 */
		public RcComm getRcComm() {
			return null; // TODO
		}
		
		/**
		 * TODO
		 * 
		 * @since 0.1.0
		 */
		public RcComm release() {
			return null; // TODO
		}

		/**
		 * TODO
		 * 
		 * @since 0.1.0
		 */
		@Override
		public void close() throws Exception {
			// TODO Auto-generated method stub

		}
	}

	/**
	 * TODO
	 * 
	 * @param poolSize
	 * 
	 * @throws IllegalArgumentException
	 * 
	 * @since 0.1.0
	 */
	public IRODSConnectionPool(int poolSize) {
		if (poolSize <= 0) {
			throw new IllegalArgumentException("Connection pool size is less than or equal to 0");
		}

		connOptions = new ConnectionOptions();
	}

	/**
	 * TODO
	 * 
	 * @param connOptions
	 * @param poolSize
	 * 
	 * @throws IllegalArgumentException
	 * 
	 * @since 0.1.0
	 */
	public IRODSConnectionPool(ConnectionOptions connOptions, int poolSize) {
		if (null == connOptions) {
			throw new IllegalArgumentException("Connection options is null");
		}

		if (poolSize <= 0) {
			throw new IllegalArgumentException("Connection pool size is less than or equal to 0");
		}

		this.connOptions = connOptions;
	}
	
	/**
	 * TODO
	 * 
	 * @param host
	 * @param port
	 * @param clientUser
	 * 
	 * @since 0.1.0
	 */
	public void connect(String host, int port, QualifiedUsername clientUser) {
		throwIfInvalidHost(host);
		throwIfInvalidPortNumber(port);
		throwIfInvalidClientUser(clientUser);

		this.host = host;
		this.port = port;
		this.clientUser = clientUser;
		
		// TODO make connections
	}

	/**
	 * TODO
	 * 
	 * @param host
	 * @param port
	 * @param proxyUser
	 * @param clientUser
	 * 
	 * @since 0.1.0
	 */
	public void connect(String host, int port, QualifiedUsername proxyUser, QualifiedUsername clientUser) {
		throwIfInvalidHost(host);
		throwIfInvalidPortNumber(port);
		throwIfInvalidProxyUser(proxyUser);
		throwIfInvalidClientUser(clientUser);

		this.host = host;
		this.port = port;
		this.clientUser = clientUser;
		this.proxyUser = proxyUser;

		// TODO make connections
	}
	
	/**
	 * TODO
	 * 
	 * @param authScheme
	 * @param password
	 * 
	 * @since 0.1.0
	 */
	public void authenticate(String authScheme, String password) {
		
	}
	
	/**
	 * TODO
	 * 
	 * @since 0.1.0
	 */
	public void shutdown() {
		
	}

	@Override
	public void close() throws Exception {
		shutdown();
	}
	
	/**
	 * TODO
	 * 
	 * @return
	 * 
	 * @since 0.1.0
	 */
	public Connection getConnection() {
		return null; // TODO
	}

	private static void throwIfInvalidHost(String host) {
		if (null == host || host.isEmpty()) {
			throw new IllegalArgumentException("Host is null or empty");
		}
	}

	private static void throwIfInvalidPortNumber(int port) {
		if (port <= 0) {
			throw new IllegalArgumentException("Port is less than or equal to 0");
		}
	}

	private static void throwIfInvalidProxyUser(QualifiedUsername user) {
		if (null == user) {
			throw new IllegalArgumentException("Proxy user is null");
		}
	}

	private static void throwIfInvalidClientUser(QualifiedUsername user) {
		if (null == user) {
			throw new IllegalArgumentException("Client user is null");
		}
	}

}
