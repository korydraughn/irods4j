package org.irods.irods4j.low_level;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Optional;

import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.common.XmlUtil;
import org.irods.irods4j.low_level.api.IRODSApi;
import org.irods.irods4j.low_level.api.IRODSApi.RcComm;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestNativeAuthentication {

	static String host = "localhost";
	static int port = 1247;
	static String zone = "tempZone";
	static String username = "rods";
	static String password = "rods";
	static RcComm comm;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		XmlUtil.enablePrettyPrinting();
		JsonUtil.enablePrettyPrinting();
		comm = IRODSApi.rcConnect(host, port, username, zone, Optional.empty(), Optional.empty(), Optional.empty(),
				Optional.empty());
		assertNotNull(comm);
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		IRODSApi.rcDisconnect(comm);
	}

	@Test
	void testNativeAuthentication() {
		assertDoesNotThrow(() -> IRODSApi.rcAuthenticateClient(comm, "native", password));
	}

	@Test
	void testAuthenticateAsAnonymousUser() {
		var anonymousComm = assertDoesNotThrow(() -> IRODSApi.rcConnect(host, port, "anonymous", zone, Optional.empty(),
				Optional.empty(), Optional.empty(), Optional.empty()));
		assertNotNull(anonymousComm);
		assertDoesNotThrow(() -> IRODSApi.rcAuthenticateClient(anonymousComm, "native", ""));
		assertDoesNotThrow(() -> IRODSApi.rcDisconnect(anonymousComm));
	}

}
