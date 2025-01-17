package org.irods.irods4j.low_level;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.common.XmlUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestPamPasswordAuthentication {

	static String host = "localhost";
	static int port = 1247;
	static String zone = "tempZone";
	static String username = "john";
	static String password = "irods";
	static RcComm comm;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		XmlUtil.enablePrettyPrinting();
		JsonUtil.enablePrettyPrinting();
		comm = IRODSApi.rcConnect(host, port, username, zone, null, null, null);
		assertNotNull(comm);
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		IRODSApi.rcDisconnect(comm);
	}

	@Test
	void testPamPasswordAuthentication() {
		assumeTrue(false, "Disabled until there are config options for working with a PAM-enabled server");
		assertDoesNotThrow(() -> IRODSApi.rcAuthenticateClient(comm, "pam_password", password));
	}

}
