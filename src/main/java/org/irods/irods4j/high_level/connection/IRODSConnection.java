package org.irods.irods4j.high_level.connection;

import java.io.IOException;
import java.util.Optional;

import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.api.IRODSApi.ConnectionOptions;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.api.IRODSException;
import org.irods.irods4j.low_level.protocol.packing_instructions.RErrMsg_PI;

public class IRODSConnection implements AutoCloseable {

	private String host;
	private int port;

	private QualifiedUsername clientUser;
	private QualifiedUsername proxyUser;

	private ConnectionOptions connOptions;
	private RcComm comm;

	public IRODSConnection() {
		connOptions = new ConnectionOptions();
	}

	public IRODSConnection(ConnectionOptions options) {
		if (null == options) {
			throw new IllegalArgumentException("Connection options is null");
		}
		connOptions = options.copy();
	}

	public void connect(String host, int port, QualifiedUsername clientUser) throws Exception {
		throwIfInvalidHost(host);
		throwIfInvalidPortNumber(port);
		throwIfInvalidClientUser(clientUser);

		this.host = host;
		this.port = port;
		this.clientUser = clientUser;

		doConnect();
	}

	public void connect(String host, int port, QualifiedUsername proxyUser, QualifiedUsername clientUser)
			throws Exception {
		throwIfInvalidHost(host);
		throwIfInvalidPortNumber(port);
		throwIfInvalidProxyUser(proxyUser);
		throwIfInvalidClientUser(clientUser);

		this.host = host;
		this.port = port;
		this.clientUser = clientUser;
		this.proxyUser = proxyUser;

		doConnect();
	}

	public void connect() throws Exception {
		doConnect();
	}

	public void authenticate(String authScheme, String password) throws Exception {
		IRODSApi.rcAuthenticateClient(comm, authScheme, password);
	}

	public boolean isConnected() {
		return (null != comm) && comm.socket.isConnected();
	}

	public RcComm getRcComm() {
		if (null == comm) {
			throw new IllegalStateException("No active connection to server");
		}
		return comm;
	}

	public void disconnect() throws IOException {
		if (null == comm) {
			return;
		}

		try {
			IRODSApi.rcDisconnect(comm);
		} finally {
			comm = null;
		}
	}

	@Override
	public void close() throws Exception {
		disconnect();
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

	private void doConnect() throws Exception {
		Optional<String> proxyUserName = Optional.empty();
		Optional<String> proxyUserZone = Optional.empty();
		if (null != proxyUser) {
			proxyUserName = Optional.of(proxyUser.getName());
			proxyUserZone = Optional.of(proxyUser.getZone());
		}

		var options = Optional.of(connOptions);
		var errInfo = Optional.of(new RErrMsg_PI());

		comm = IRODSApi.rcConnect(host, port, clientUser.getName(), clientUser.getZone(), proxyUserName, proxyUserZone,
				options, errInfo);

		if (null == comm || errInfo.get().status < 0) {
			throw new IRODSException(errInfo.get().status, "rcConnect error");
		}
	}

}
