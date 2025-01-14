package org.irods.irods4j.low_level;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestStreamOperations {

	static String host = "localhost";
	static int port = 1247;
	static String zone = "tempZone";
	static String username = "rods";
	static String password = "rods";
	static RcComm comm;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		comm = IRODSApi.rcConnect(host, port, zone, username);
		assertNotNull(comm);
		IRODSApi.authenticate(comm, "native", password);
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		IRODSApi.rcDisconnect(comm);
	}

	@Test
	void testReadAndWriteToADataObject() {
	}

}
