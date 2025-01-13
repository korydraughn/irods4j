package org.irods.irods4j.api;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.common.Reference;
import org.irods.irods4j.common.XmlUtil;
import org.irods.irods4j.low_level.network.Network;
import org.irods.irods4j.low_level.protocol.packing_instructions.BinBytesBuf_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.BytesBuf_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.CS_NEG_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.CollEnt_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.CollInpNew_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.CollOprStat_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjCopyInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInfo_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.DelayRuleLockInput_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.DelayRuleUnlockInput_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.DelayServerMigrationInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.ExecMyRuleInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.FileLseekOut_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.GenQueryOut_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.GeneralAdminInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.Genquery2Input_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.GridConfigurationInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.GridConfigurationOut_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.INT_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.IRODS_STR_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.MiscSvrInfo_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.ModAVUMetadataInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.ModAccessControlInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.ModDataObjMeta_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.MsParamArray_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.MsgHeader_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.OpenedDataObjInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.ProcStatInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RULE_EXEC_DEL_INP_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RULE_EXEC_MOD_INP_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RegReplica_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RodsObjStat_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.STR_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.SpecificQueryInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.StartupPack_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.SwitchUserInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.TicketAdminInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.TransferStat_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.UnregDataObj_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.UserAdminInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.Version_PI;

public class IRODSApi {

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

	private static void sendApiRequest(Socket socket, int apiNumber) throws IOException {
		// Create the header describing the message.
		var mh = new MsgHeader_PI();
		mh.type = MsgHeader_PI.MsgType.RODS_API_REQ;
		mh.intInfo = apiNumber;

		// Send request.
		Network.write(socket, mh);
	}

	private static void sendApiRequest(Socket socket, int apiNumber, Object data) throws IOException {
		var msgbody = XmlUtil.toXmlString(data);

		// Create the header describing the message.
		var mh = new MsgHeader_PI();
		mh.type = MsgHeader_PI.MsgType.RODS_API_REQ;
		mh.intInfo = apiNumber;
		mh.msgLen = msgbody.length();

		// Send request.
		Network.write(socket, mh);
		Network.writeXml(socket, data);
	}

	private static void sendApiRequest(Socket socket, int apiNumber, Object data, byte[] bytes) throws IOException {
		var msgbody = XmlUtil.toXmlString(data);

		// Create the header describing the message.
		var mh = new MsgHeader_PI();
		mh.type = MsgHeader_PI.MsgType.RODS_API_REQ;
		mh.intInfo = apiNumber;
		mh.msgLen = msgbody.length();
		mh.bsLen = bytes.length;

		// Send request.
		Network.write(socket, mh);
		Network.writeXml(socket, data);
		Network.writeBytes(socket, bytes);
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
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		// Check for errors.
		if (mh.intInfo < 0) {
			throw new IRODSException(mh.intInfo, "Authentication failure");
		}

		var bbbuf = Network.readObject(comm.socket, mh.msgLen, BinBytesBuf_PI.class);
		log.debug("Received BinBytesBuf_PI: {}", XmlUtil.toXmlString(bbbuf));
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
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		// Check for errors.
		if (mh.intInfo < 0) {
			throw new IRODSException(mh.intInfo, "Authentication failure");
		}

		bbbuf = Network.readObject(comm.socket, mh.msgLen, BinBytesBuf_PI.class);
		log.debug("Received BinBytesBuf_PI: {}", XmlUtil.toXmlString(bbbuf));
		log.debug("BinBytesBuf_PI contents: {}", bbbuf.decode());

		comm.loggedIn = true;

		log.debug("Authentication Successful!");
	}

	public static int rcObjStat(RcComm comm, DataObjInp_PI input, Reference<RodsObjStat_PI> output) throws IOException {
		sendApiRequest(comm.socket, 633, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.intInfo < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			output.value = Network.readObject(comm.socket, mh.msgLen, RodsObjStat_PI.class);
		}

		if (mh.errorLen > 0) {
			// TODO
		}

		if (mh.bsLen > 0) {
			// TODO
		}

		return mh.intInfo;
	}

	public static int rcGenQuery2(RcComm comm, Genquery2Input_PI input, Reference<String> output) throws IOException {
		sendApiRequest(comm.socket, 10221, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.intInfo < 0) {
			return mh.intInfo;
		}

		var str = Network.readObject(comm.socket, mh.msgLen, STR_PI.class);
		output.value = str.myStr;

		return mh.intInfo;
	}

	public static int rcReplicaOpen(RcComm comm, DataObjInp_PI input, Reference<String> l1descInfo) throws IOException {
		sendApiRequest(comm.socket, 20003, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		// Check for errors. We should never see a L1descInx less than 3.
		// If we do, something horrible has happened on the server side.
		if (mh.intInfo < 3) {
			return mh.intInfo;
		}

		// Capture the L1descInx (i.e. the iRODS file descriptor) and
		// l1desc information.
		var bbbuf = Network.readObject(comm.socket, mh.msgLen, BinBytesBuf_PI.class);
		l1descInfo.value = bbbuf.decode();

		return mh.intInfo;
	}

	public static int rcReplicaTruncate(RcComm comm, DataObjInp_PI input, Reference<String> output) throws IOException {
		sendApiRequest(comm.socket, 802, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			var tmp = Network.readObject(comm.socket, mh.msgLen, STR_PI.class);
			output.value = tmp.myStr;
		}

		return mh.intInfo;
	}

	public static int rcDataObjLseek(RcComm comm, OpenedDataObjInp_PI input, Reference<FileLseekOut_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 674, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		// Check for errors.
		if (mh.intInfo < 0) {
			return mh.intInfo;
		}

		output.value = Network.readObject(comm.socket, mh.msgLen, FileLseekOut_PI.class);

		// For a read operation, remember the error code represents the
		// total number of bytes read.
		return mh.intInfo;
	}

	public static int rcDataObjRead(RcComm comm, OpenedDataObjInp_PI input, byte[] buffer) throws IOException {
		sendApiRequest(comm.socket, 675, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		// Check for errors.
		if (mh.intInfo < 0) {
			return mh.intInfo;
		}

		// Check to see if the byte stream contains data. If the server
		// returns any data via the byte stream, it will appear directly
		// after the MsgHeader_PI message.
		if (mh.bsLen > 0) {
			var bytes = Network.readBytes(comm.socket, mh.bsLen);
			System.arraycopy(bytes, 0, buffer, 0, bytes.length);
		}

		// For a read operation, remember the error code represents the
		// total number of bytes read.
		return mh.intInfo;
	}

	public static int rcDataObjWrite(RcComm comm, OpenedDataObjInp_PI input, byte[] buffer) throws IOException {
		sendApiRequest(comm.socket, 676, input, buffer);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		// For a write operation, remember the error code represents the
		// total number of bytes written.
		return mh.intInfo;
	}

	public static int rcReplicaClose(RcComm comm, String closeOptions) throws IOException {
		var input = new BinBytesBuf_PI(closeOptions);

		sendApiRequest(comm.socket, 20004, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		return mh.intInfo;
	}

	public static int rcAtomicApplyMetadataOperations(RcComm comm, String input, Reference<String> output)
			throws IOException {
		var bbbuf = new BinBytesBuf_PI(input);

		sendApiRequest(comm.socket, 20002, bbbuf);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			bbbuf = Network.readObject(comm.socket, mh.msgLen, BinBytesBuf_PI.class);
			output.value = bbbuf.decode();
		}

		return mh.intInfo;
	}

	public static int rcAtomicApplyAclOperations(RcComm comm, String input, Reference<String> output)
			throws IOException {
		var bbbuf = new BinBytesBuf_PI(input);

		sendApiRequest(comm.socket, 20005, bbbuf);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			bbbuf = Network.readObject(comm.socket, mh.msgLen, BinBytesBuf_PI.class);
			output.value = bbbuf.decode();
		}

		return mh.intInfo;
	}

	public static int rcTouch(RcComm comm, String input) throws IOException {
		var bbbuf = new BinBytesBuf_PI(input);

		sendApiRequest(comm.socket, 20007, bbbuf);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcGetGridConfigurationValue(RcComm comm, GridConfigurationInp_PI input,
			Reference<GridConfigurationOut_PI> output) throws IOException {
		sendApiRequest(comm.socket, 20009, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			output.value = Network.readObject(comm.socket, mh.msgLen, GridConfigurationOut_PI.class);
		}

		return mh.intInfo;
	}

	public static int rcSetGridConfigurationValue(RcComm comm, GridConfigurationInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 20010, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcSetDelayServerMigrationInfo(RcComm comm, DelayServerMigrationInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 20011, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcGetDelayRuleInfo(RcComm comm, String input, Reference<String> output) throws IOException {
		var strPI = new STR_PI();
		strPI.myStr = input;

		sendApiRequest(comm.socket, 20013, strPI);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			var bbbuf = Network.readObject(comm.socket, mh.msgLen, BinBytesBuf_PI.class);
			output.value = bbbuf.decode();
		}

		return mh.intInfo;
	}

	public static int rcGetFileDescriptorInfo(RcComm comm, String input, Reference<String> output) throws IOException {
		var bbbuf = new BinBytesBuf_PI(input);

		sendApiRequest(comm.socket, 20000, bbbuf);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			bbbuf = Network.readObject(comm.socket, mh.msgLen, BinBytesBuf_PI.class);
			output.value = bbbuf.decode();
		}

		return mh.intInfo;
	}

	public static int rcSwitchUser(RcComm comm, SwitchUserInp_PI input) throws IOException {
		// Accepting empty strings for either parameter is not allowed.
		// This is especially true for the "_zone" parameter because this function
		// copies the string pointed to by "_zone" into the RcComm. Copying an empty
		// string into the RcComm's.
		if (null == input.username || input.username.isEmpty()) {
			throw new IllegalArgumentException("Username is null or empty");
		}

		if (null == input.zone || input.zone.isEmpty()) {
			throw new IllegalArgumentException("Zone is null or empty");
		}

		if (comm.clientUsername.equals(input.username) && comm.clientUserZone.equals(input.zone)) {
			return 0;
		}

		sendApiRequest(comm.socket, 20012, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.intInfo == 0) {
			comm.clientUsername = input.username;
			comm.clientUserZone = input.zone;

			if (null != input.KeyValPair_PI && input.KeyValPair_PI.ssLen > 0
					&& input.KeyValPair_PI.keyWord.contains(SwitchUserInp_PI.KW_SWITCH_PROXY_USER)) {
				comm.proxyUsername = input.username;
				comm.proxyUserZone = input.zone;
			}
		}

		return mh.intInfo;
	}

	public static int rcCheckAuthCredentials(RcComm comm, DataObjInp_PI input, Reference<Integer> correct)
			throws IOException {
		sendApiRequest(comm.socket, 800, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			var i = Network.readObject(comm.socket, mh.msgLen, INT_PI.class);
			correct.value = i.myInt;
		}

		return mh.intInfo;
	}

	// TODO Should this API be exposed to clients?
	public static int rcRegisterPhysicalPath(RcComm comm, DataObjInp_PI input, Reference<String> output)
			throws IOException {
		sendApiRequest(comm.socket, 20008, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			var bbbuf = Network.readObject(comm.socket, mh.msgLen, BinBytesBuf_PI.class);
			output.value = bbbuf.decode();
		}

		return mh.intInfo;
	}

	public static int rcModAVUMetadata(RcComm comm, ModAVUMetadataInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 706, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcModAccessControl(RcComm comm, ModAccessControlInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 707, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcModDataObjMeta(RcComm comm, ModDataObjMeta_PI input) throws IOException {
		sendApiRequest(comm.socket, 622, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcDataObjectModifyInfo(RcComm comm, ModDataObjMeta_PI input) throws IOException {
		sendApiRequest(comm.socket, 20001, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcDataObjRename(RcComm comm, DataObjCopyInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 627, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcDataObjCopy(RcComm comm, DataObjCopyInp_PI input, Reference<TransferStat_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 696, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			output.value = Network.readObject(comm.socket, mh.msgLen, TransferStat_PI.class);
		}

		return mh.intInfo;
	}

	public static int rcDataObjRepl(RcComm comm, DataObjInp_PI input, Reference<TransferStat_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 695, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			output.value = Network.readObject(comm.socket, mh.msgLen, TransferStat_PI.class);
		}

		return mh.intInfo;
	}

	public static int rcDataObjCreate(RcComm comm, DataObjInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 601, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcDataObjChksum(RcComm comm, DataObjInp_PI input, Reference<String> output) throws IOException {
		sendApiRequest(comm.socket, 629, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			var tmp = Network.readObject(comm.socket, mh.msgLen, STR_PI.class);
			output.value = tmp.myStr;
		}

		return mh.intInfo;
	}

	public static int rcDataObjUnlink(RcComm comm, DataObjInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 615, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcDataObjTrim(RcComm comm, DataObjInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 632, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcDelayRuleLock(RcComm comm, DelayRuleLockInput_PI input) throws IOException {
		sendApiRequest(comm.socket, 10222, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcDelayRuleUnlock(RcComm comm, DelayRuleUnlockInput_PI input) throws IOException {
		sendApiRequest(comm.socket, 10223, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcCollCreate(RcComm comm, CollInpNew_PI input) throws IOException {
		sendApiRequest(comm.socket, 681, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcModColl(RcComm comm, CollInpNew_PI input) throws IOException {
		sendApiRequest(comm.socket, 680, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcOpenCollection(RcComm comm, CollInpNew_PI input) throws IOException {
		sendApiRequest(comm.socket, 678, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcReadCollection(RcComm comm, int handle, Reference<CollEnt_PI> output) throws IOException {
		var input = new INT_PI();
		input.myInt = handle;
		sendApiRequest(comm.socket, 713, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			output.value = Network.readObject(comm.socket, mh.msgLen, CollEnt_PI.class);
		}

		return mh.intInfo;
	}

	public static int rcCloseCollection(RcComm comm, int handle) throws IOException {
		var input = new INT_PI();
		input.myInt = handle;
		sendApiRequest(comm.socket, 661, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcRmColl(RcComm comm, CollInpNew_PI input, Reference<CollOprStat_PI> output) throws IOException {
		sendApiRequest(comm.socket, 679, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			output.value = Network.readObject(comm.socket, mh.msgLen, CollOprStat_PI.class);
		}

		return mh.intInfo;
	}

	public static int rcTicketAdmin(RcComm comm, TicketAdminInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 723, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcUnregDataObj(RcComm comm, UnregDataObj_PI input) throws IOException {
		sendApiRequest(comm.socket, 620, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcUserAdmin(RcComm comm, UserAdminInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 714, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcSpecificQuery(RcComm comm, SpecificQueryInp_PI input, Reference<GenQueryOut_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 722, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			output.value = Network.readObject(comm.socket, mh.msgLen, GenQueryOut_PI.class);
		}

		return mh.intInfo;
	}

	public static int rcGetResourceInfoForOperation(RcComm comm, DataObjInp_PI input, Reference<String> output)
			throws IOException {
		sendApiRequest(comm.socket, 10220, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			var tmp = Network.readObject(comm.socket, mh.msgLen, STR_PI.class);
			output.value = tmp.myStr;
		}

		return mh.intInfo;
	}

	public static int rcZoneReport(RcComm comm, SpecificQueryInp_PI input, Reference<BytesBuf_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 10205, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			output.value = Network.readObject(comm.socket, mh.msgLen, BytesBuf_PI.class);
		}

		return mh.intInfo;
	}

	public static int rcGetMiscSvrInfo(RcComm comm, Reference<MiscSvrInfo_PI> output) throws IOException {
		sendApiRequest(comm.socket, 700);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			output.value = Network.readObject(comm.socket, mh.msgLen, MiscSvrInfo_PI.class);
		}

		return mh.intInfo;
	}

	public static int rcGeneralAdmin(RcComm comm, GeneralAdminInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 701, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcExecMyRule(RcComm comm, ExecMyRuleInp_PI input, Reference<MsParamArray_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 625);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			output.value = Network.readObject(comm.socket, mh.msgLen, MsParamArray_PI.class);
		}

		return mh.intInfo;
	}

	public static int rcProcStat(RcComm comm, ProcStatInp_PI input, Reference<GenQueryOut_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 625);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			output.value = Network.readObject(comm.socket, mh.msgLen, GenQueryOut_PI.class);
		}

		return mh.intInfo;
	}

	public static int rcRuleExecSubmit(RcComm comm, RULE_EXEC_DEL_INP_PI input, Reference<String> output)
			throws IOException {
		sendApiRequest(comm.socket, 623, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			var tmp = Network.readObject(comm.socket, mh.msgLen, IRODS_STR_PI.class);
			output.value = tmp.myStr;
		}

		return mh.intInfo;
	}

	public static int rcRuleExecMod(RcComm comm, RULE_EXEC_MOD_INP_PI input) throws IOException {
		sendApiRequest(comm.socket, 708, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static int rcRuleExecDel(RcComm comm, RULE_EXEC_DEL_INP_PI input) throws IOException {
		sendApiRequest(comm.socket, 624, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	// TODO This API is likely for server-side use only due to it only being invoked
	// within the server. Consider removing this.
	public static int rcRegReplica(RcComm comm, RegReplica_PI input) throws IOException {
		sendApiRequest(comm.socket, 621, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	// TODO This API is not used in server at all. We should probably deprecate it or remove
	// it from the public interface. Consider removing this.
	public static int rcRegDataOb(RcComm comm, DataObjInfo_PI input, Reference<DataObjInfo_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 619, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		if (mh.msgLen < 0) {
			return mh.intInfo;
		}

		if (mh.msgLen > 0) {
			output.value = Network.readObject(comm.socket, mh.msgLen, DataObjInfo_PI.class);
		}

		return mh.intInfo;
	}

}
