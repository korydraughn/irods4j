package org.irods.irods4j.low_level;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.common.Reference;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.KeyValPair_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RodsObjStat_PI;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestObjStat {

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testRcObjStat() {
		var host = "localhost";
		var port = 1247;
		var zone = "tempZone";
		var username = "rods";
		var password = "rods";
		
		var comm = assertDoesNotThrow(() -> IRODSApi.rcConnect(host, port, zone, username));
		assertNotNull(comm);
		assertDoesNotThrow(() -> IRODSApi.authenticate(comm, "native", password));

		var input = new DataObjInp_PI();
		input.objPath = "/tempZone/home/" + username;
		input.KeyValPair_PI = new KeyValPair_PI();
		input.KeyValPair_PI.ssLen = 0;
		var output = new Reference<RodsObjStat_PI>();
		assertDoesNotThrow(() -> IRODSApi.rcObjStat(comm, input, output));
		assertNotNull(output);
		assertNotNull(output.value);

		assertDoesNotThrow(() -> IRODSApi.rcDisconnect(comm));
	}

}
