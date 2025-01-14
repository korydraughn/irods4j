package org.irods.irods4j.low_level;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.api.IRODSException;
import org.irods.irods4j.common.Reference;
import org.irods.irods4j.low_level.protocol.packing_instructions.Genquery2Input_PI;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestGenQuery2 {
	
	private static final Logger log = LogManager.getLogger();

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
	void testRcGenQuery2() throws IOException, IRODSException {
		var host = "localhost";
		var port = 1247;
		var zone = "tempZone";
		var username = "rods";
		var password = "rods";
		
		RcComm comm = assertDoesNotThrow(() -> IRODSApi.rcConnect(host, port, zone, username));
		assertNotNull(comm);
		assertDoesNotThrow(() -> IRODSApi.authenticate(comm, "native", password));

		var input = new Genquery2Input_PI();
		input.query_string = "select COLL_NAME";
		input.zone = zone;
		input.sql_only = 0;
		input.column_mappings = 0;
		var output = new Reference<String>();
		assertEquals(IRODSApi.rcGenQuery2(comm, input, output), 0);
		assertNotNull(output);
		assertNotNull(output.value);
		log.info("results = {}", output.value);

		assertDoesNotThrow(() -> IRODSApi.rcDisconnect(comm));
	}

}
