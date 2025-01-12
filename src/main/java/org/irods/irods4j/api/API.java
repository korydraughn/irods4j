package org.irods.irods4j.api;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.common.XmlUtil;
import org.irods.irods4j.low_level.network.Network;
import org.irods.irods4j.low_level.protocol.packing_instructions.BinBytesBuf_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.CS_NEG_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.Genquery2Input_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.KeyValPair_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.MsgHeader_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RodsObjStat_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.STR_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.StartupPack_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.Version_PI;

public class API {

	public static final Logger log = LogManager.getLogger();

	private static String appName = "irods4j";

	public static class RcComm {
		Socket socket;

		boolean usingTLS = false;
		boolean loggedIn = false;

		String clientUsername;
		String clientUserZone;

		String proxyUsername;
		String proxyUserZone;

		String sessionSignature;

		String relVersion;
		String apiVersion;
		int status;
		int cookie;
	}

	public static void setApplicationName(String name) {
		if (null == name || name.isEmpty()) {
			throw new IllegalArgumentException("Application name is null or empty");
		}

		if (!"irods4j".equals(appName)) {
			throw new RuntimeException("setApplicationName() invoked more than once");
		}

		appName = name;
	}

	public static RcComm rcConnect(String host, int port, String zone, String username)
			throws UnknownHostException, IOException, IRODSException {
		if (null == host || host.isEmpty()) {
			throw new IllegalArgumentException("Host is null or empty");
		}

		if (port <= 0) {
			throw new IllegalArgumentException("Port is less than or equal to 0");
		}

		if (null == zone || zone.isEmpty()) {
			throw new IllegalArgumentException("Zone is null or empty");
		}

		if (null == username || username.isEmpty()) {
			throw new IllegalArgumentException("Username is null or empty");
		}

		RcComm comm = new RcComm();
		comm.socket = new Socket(host, port);

		// Create the StartupPack message.
		// This is how a connection to iRODS is always initiated.
		var sp = new StartupPack_PI();
		sp.clientUser = comm.clientUsername = username;
		sp.clientRcatZone = comm.clientUserZone = zone;
		sp.proxyUser = comm.proxyUsername = username;
		sp.proxyRcatZone = comm.proxyUserZone = zone;
		sp.option = appName + "request_server_negotiation";
		var msgbody = XmlUtil.toXmlString(sp);

		// Create the header describing the StartupPack message.
		var hdr = new MsgHeader_PI();
		hdr.type = MsgHeader_PI.MsgType.RODS_CONNECT;
		hdr.msgLen = msgbody.length();

		// Send the message header and StartupPack (i.e. the message body).
		Network.write(comm.socket, hdr);
		Network.writeXml(comm.socket, sp);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.intInfo < 0) {
			throw new IRODSException(mh.intInfo, "StartupPack failure");
		}

		// Negotiation

		// Prepare to the negotiate whether a secure communication
		// channel is needed.
		var csneg = Network.readObject(comm.socket, mh.msgLen, CS_NEG_PI.class);
		log.debug("Received CS_NEG_PI: {}", XmlUtil.toXmlString(csneg));

		// TODO Add support for SSL/TLS.

		// No TLS support implemented at this time, so tell the server
		// we want to move forward without TLS.
		csneg.result = "cs_neg_result_kw=CS_NEG_USE_TCP;";
		msgbody = XmlUtil.toXmlString(csneg);
		hdr.type = MsgHeader_PI.MsgType.RODS_CS_NEG_T;
		hdr.msgLen = msgbody.length();
		Network.write(comm.socket, hdr);
		Network.writeXml(comm.socket, csneg);

		// Read the message header from the server.
		mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.intInfo < 0) {
			throw new IRODSException(mh.intInfo, "Client-Server negotiation failure");
		}

		// Capture the server version information.
		var vers = Network.readObject(comm.socket, mh.msgLen, Version_PI.class);
		log.debug("Received Version_PI: {}", XmlUtil.toXmlString(vers));
		comm.apiVersion = vers.apiVersion;
		comm.relVersion = vers.relVersion;
		comm.status = vers.status;
		comm.cookie = vers.cookie;

		return comm;
	}

	public static void rcDisconnect(RcComm comm) throws IOException {
		final var hdr = new MsgHeader_PI();
		hdr.type = MsgHeader_PI.MsgType.RODS_DISCONNECT;
		Network.write(comm.socket, hdr);
		comm.socket.close();
	}

	public static void authenticate(RcComm comm, String authScheme, String password)
			throws IOException, NoSuchAlgorithmException, IRODSException {
		var msg = new HashMap<String, Object>() {
			private static final long serialVersionUID = 1L;

			{
				put("a_ttl", "0");
				put("force_password_prompt", Boolean.TRUE);
				put("next_operation", "auth_agent_auth_request");
				put("scheme", "native");
				put("user_name", comm.clientUsername);
				put("zone_name", comm.clientUserZone);
			}
		};
		var json = JsonUtil.toJsonString(msg);
		var msgbody = XmlUtil.toXmlString(new BinBytesBuf_PI(json));

		var hdr = new MsgHeader_PI();
		hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
		hdr.msgLen = msgbody.length();
		hdr.intInfo = 110000; // New auth plugin framework API number.

		Network.write(comm.socket, hdr);
		Network.writeXml(comm.socket, new BinBytesBuf_PI(json));

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		// Check for errors.
		if (mh.intInfo < 0) {
			throw new IRODSException(mh.intInfo, "Authentication failure");
		}

		var bbbuf = Network.readObject(comm.socket, mh.msgLen, BinBytesBuf_PI.class);
		log.debug("received BinBytesBuf_PI: {}", XmlUtil.toXmlString(bbbuf));
		log.debug("BinBytesBuf_PI contents: {}", bbbuf.decode());

		var tr = new HashMap<String, Object>();
		var jsonContent = JsonUtil.fromJsonString(bbbuf.decode(), tr.getClass());
		var requestResult = (String) jsonContent.get("request_result");
		var signature = requestResult.substring(0, 16);
		log.debug("signature = {}", signature);
		log.debug("signature length = {}", signature.length());

		// Generate the MD5 hash for challenge response.
		var pwsb = new StringBuilder();
		pwsb.append(password);
		pwsb.setLength(50); // Pad the string with null bytes until it has a length of 50 bytes.
		var digest = MessageDigest.getInstance("md5");
		digest.update(requestResult.getBytes());
		digest.update(pwsb.toString().getBytes());
		var challengeResponse = Base64.getEncoder().encodeToString(digest.digest());
		log.debug("challengeResponse = {}", challengeResponse);

		msg.put("next_operation", "auth_agent_auth_response");
		msg.put("digest", challengeResponse);

		json = JsonUtil.toJsonString(msg);
		bbbuf = new BinBytesBuf_PI(json);
		msgbody = XmlUtil.toXmlString(bbbuf);
		hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
		hdr.msgLen = msgbody.length();
		hdr.intInfo = 110000; // New auth plugin framework API number.
		Network.write(comm.socket, hdr);
		Network.writeXml(comm.socket, bbbuf);

		// Read the message header from the server.
		mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		// Check for errors.
		if (mh.intInfo < 0) {
			throw new IRODSException(mh.intInfo, "Authentication failure");
		}

		bbbuf = Network.readObject(comm.socket, mh.msgLen, BinBytesBuf_PI.class);
		log.debug("received BinBytesBuf_PI: {}", XmlUtil.toXmlString(bbbuf));
		log.debug("BinBytesBuf_PI contents: {}", bbbuf.decode());

		comm.loggedIn = true;

		log.debug("Authentication Successful!");
	}

	public static RodsObjStat_PI rcObjStat(RcComm comm, String logicalPath) throws IOException, IRODSException {
		var input = new DataObjInp_PI();
		input.objPath = logicalPath;
		input.KeyValPair_PI = new KeyValPair_PI();
		input.KeyValPair_PI.ssLen = 0;
		var msgbody = XmlUtil.toXmlString(input);

		// Create the header describing the message.
		var mh = new MsgHeader_PI();
		mh.type = MsgHeader_PI.MsgType.RODS_API_REQ;
		mh.msgLen = msgbody.length();
		mh.intInfo = 633; // OBJ_STAT_AN

		// Send the message header and DataObjInp (i.e. the message body).
		Network.write(comm.socket, mh);
		Network.writeXml(comm.socket, input);

		// Read the message header from the server.
		mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.intInfo < 0) {
			throw new IRODSException(mh.intInfo, "rcObjStat failure");
		}

		return Network.readObject(comm.socket, mh.msgLen, RodsObjStat_PI.class);
	}

	public static String rcGenQuery2(RcComm comm, String query, Optional<String> zone, boolean sqlOnly,
			boolean columnMappings) throws IOException, IRODSException {
		var input = new Genquery2Input_PI();
		input.query_string = query;
		input.zone = zone.orElse(comm.clientUserZone);
		input.sql_only = sqlOnly ? 1 : 0;
		input.column_mappings = columnMappings ? 1 : 0;
		var msgbody = XmlUtil.toXmlString(input);

		// Create the header describing the message.
		var mh = new MsgHeader_PI();
		mh.type = MsgHeader_PI.MsgType.RODS_API_REQ;
		mh.msgLen = msgbody.length();
		mh.intInfo = 10221; // GENQUERY2_AN

		// Send the message header and Genquery2Input (i.e. the message body).
		Network.write(comm.socket, mh);
		Network.writeXml(comm.socket, input);

		// Read the message header from the server.
		mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.intInfo < 0) {
			throw new IRODSException(mh.intInfo, "rcGenQuery2 failure");
		}

		return Network.readObject(comm.socket, mh.msgLen, STR_PI.class).myStr;
	}
	
	public static final class ReplicaOpenResult {
		public int fd;
		public String l1descInfo;
	}

	public static ReplicaOpenResult rcReplicaOpen(RcComm comm) {
		return new ReplicaOpenResult();
	}

	public static int rcDataObjRead(RcComm comm) {
		return 0;
	}

	public static int rcDataObjWrite(RcComm comm) {
		return 0;
	}

	public static int rcReplicaClose(RcComm comm) {
		return 0;
	}

}
