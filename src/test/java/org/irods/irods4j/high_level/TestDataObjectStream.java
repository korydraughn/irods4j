package org.irods.irods4j.high_level;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.high_level.io.IRODSDataObjectStream;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI.OpenFlags;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestDataObjectStream {

	static final Logger log = LogManager.getLogger();

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
	void testDataObjectStreamCapturesReplicaNumberAndReplicaToken() throws Exception {
		var logicalPath = Paths
				.get("/", zone, "home", username, "testDataObjectStreamCapturesReplicaNumberAndReplicaToken.txt")
				.toString();

		var in = new IRODSDataObjectStream(comm);
		in.open(logicalPath, OpenFlags.O_CREAT | OpenFlags.O_WRONLY);
		assertTrue(in.isOpen());

		assertTrue(in.getNativeHandle() >= 3);
		log.info("Native handle = {}", in.getNativeHandle());

		assertTrue(in.getReplicaNumber() >= 0);
		log.info("Replica number = {}", in.getReplicaNumber());

		assertNotNull(in.getReplicaToken());
		log.info("Replica token = {}", in.getReplicaToken());

		in.close();
		assertFalse(in.isOpen());
	}

}
