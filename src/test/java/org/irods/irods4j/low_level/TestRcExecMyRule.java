package org.irods.irods4j.low_level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;

import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.common.Reference;
import org.irods.irods4j.common.XmlUtil;
import org.irods.irods4j.low_level.protocol.packing_instructions.ExecMyRuleInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.KeyValPair_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.MsParamArray_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RHostAddr_PI;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestRcExecMyRule {

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
	void testRcExecMyRule() throws IOException {
		XmlUtil.enablePrettyPrinting();
		
		var input = new ExecMyRuleInp_PI();
		// The rule text to execute.
		input.myRule = "@external rule { writeLine('stdout', 'Hello, irods4j!'); }";
		input.RHostAddr_PI = new RHostAddr_PI();
		// The output variable to store the output in. This is specific to stdout.
		input.KeyValPair_PI = new KeyValPair_PI();
		input.KeyValPair_PI.ssLen = 1;
		input.KeyValPair_PI.keyWord = new ArrayList<>();
		input.KeyValPair_PI.svalue = new ArrayList<>();
		// Target a specific rule engine plugin (REP).
		input.KeyValPair_PI.keyWord.add("instance_name");
		input.KeyValPair_PI.svalue.add("irods_rule_engine_plugin-irods_rule_language-instance");
		input.outParamDesc = "ruleExecOut";
		input.MsParamArray_PI = new MsParamArray_PI();
		input.MsParamArray_PI.paramLen = 0;
		
		var output = new Reference<MsParamArray_PI>();
		
		var ec = IRODSApi.rcExecMyRule(comm, input, output);
		assertEquals(ec, 0);
		assertNotNull(output);
		assertNotNull(output.value);
	}

}
