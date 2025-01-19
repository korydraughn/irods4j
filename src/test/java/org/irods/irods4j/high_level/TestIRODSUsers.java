package org.irods.irods4j.high_level;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;

import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.api.IRODSException;
import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.common.XmlUtil;
import org.irods.irods4j.high_level.administration.IRODSUsers;
import org.irods.irods4j.high_level.administration.IRODSUsers.User;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestIRODSUsers {

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
		comm = IRODSApi.rcConnect(host, port, username, zone, null, null, null);
		assertNotNull(comm);
		IRODSApi.rcAuthenticateClient(comm, "native", password);
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		IRODSApi.rcDisconnect(comm);
	}

	@Test
	void testModifyPasswordOfDifferentUser() throws IOException, NoSuchAlgorithmException, IRODSException {
		var prop = new IRODSUsers.UserPasswordProperty();
		prop.value = "kpass";
		prop.requesterPassword = password;
		IRODSUsers.modifyUser(comm, new User("kory", Optional.of(zone)), prop);
	}

}
