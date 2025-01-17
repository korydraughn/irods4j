package org.irods.irods4j.low_level;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.common.Reference;
import org.irods.irods4j.common.XmlUtil;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.KeyValPair_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RodsObjStat_PI;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestRcObjStat {

	static String host = "localhost";
	static int port = 1247;
	static String zone = "tempZone";
	static String username = "rods";
	static String password = "rods";
	static RcComm comm;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		JsonUtil.enablePrettyPrinting();
		XmlUtil.enablePrettyPrinting();

		comm = IRODSApi.rcConnect(host, port, zone, username);
		assertNotNull(comm);
		IRODSApi.authenticate(comm, "native", password);
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		IRODSApi.rcDisconnect(comm);
	}

	@Test
	void testRcObjStat() throws IOException {
		var input = new DataObjInp_PI();
		input.objPath = "/tempZone/home/" + username;
		input.KeyValPair_PI = new KeyValPair_PI();
		input.KeyValPair_PI.ssLen = 0;

		var output = new Reference<RodsObjStat_PI>();

		var ec = IRODSApi.rcObjStat(comm, input, output);
		assertTrue(ec > 0);
		assertNotNull(output);
		assertNotNull(output.value);
	}

}
