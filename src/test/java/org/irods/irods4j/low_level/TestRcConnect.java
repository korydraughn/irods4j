package org.irods.irods4j.low_level;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.irods.irods4j.api.IRODSApi;
import org.junit.jupiter.api.Test;

class TestRcConnect {

	@Test
	void testConnectAndDisconnect() {
		final var host = "localhost";
		final var port = 1247;
		final var zone = "tempZone";
		final var username = "rods";
		
		var comm = assertDoesNotThrow(() -> IRODSApi.rcConnect(host, port, username, zone, null, null));
		assertNotNull(comm);
		assertDoesNotThrow(() -> IRODSApi.rcDisconnect(comm));
	}

}
