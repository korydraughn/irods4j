package irods4j;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.irods.irods4j.api.API;
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
		
		var comm = assertDoesNotThrow(() -> API.rcConnect(host, port, zone, username));
		assertNotNull(comm);
		assertDoesNotThrow(() -> API.authenticate(comm, "native", password));
		assertDoesNotThrow(() -> API.rcObjStat(comm, "/tempZone/home/" + username));
		assertDoesNotThrow(() -> API.rcDisconnect(comm));
	}

}
