package org.irods.irods4j.api;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.authentication.AuthManager;
import org.irods.irods4j.authentication.AuthPlugin;
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
import org.irods.irods4j.low_level.protocol.packing_instructions.RErrMsg_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RError_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RULE_EXEC_DEL_INP_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RULE_EXEC_MOD_INP_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RegReplica_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RodsObjStat_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.SSLEndInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.SSLStartInp_PI;
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
		public Socket socket;
		public Socket plainSocket;
		public SSLSocket sslSocket;

		public boolean usingTLS = false;
		public boolean secure = false;
		public boolean loggedIn = false;

		public String clientUsername;
		public String clientUserZone;

		public String proxyUsername;
		public String proxyUserZone;

		public String sessionSignature;

		public String relVersion;
		public String apiVersion;
		public int status;
		public int cookie;

		public RError_PI rError;
	}

	public static class ByteArrayReference {
		public byte[] data;
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

	private static <T> int readServerResponse(RcComm comm, Class<T> targetClass, Reference<T> output,
			ByteArrayReference bsBuffer) throws IOException {
		var mh = Network.readMsgHeader_PI(comm.socket);
		if (log.isDebugEnabled()) {
			log.debug("Received MsgHeader_PI:\n{}", XmlUtil.toXmlString(mh));
		}

		if (mh.msgLen > 0 && null != targetClass) {
			output.value = Network.readObject(comm.socket, mh.msgLen, targetClass);
		}

		if (mh.errorLen > 0) {
			comm.rError = Network.readObject(comm.socket, mh.msgLen, RError_PI.class);
		}

		if (mh.bsLen > 0 && null != bsBuffer) {
			bsBuffer.data = Network.readBytes(comm.socket, mh.bsLen);
		}

		return mh.intInfo;
	}

	public static RcComm rcConnect(String host, int port, String clientUsername, String clientUserZone,
			String proxyUsername, String proxyUserZone, RErrMsg_PI errorInfo) throws Exception {
		if (null == host || host.isEmpty()) {
			throw new IllegalArgumentException("Host is null or empty");
		}

		if (port <= 0) {
			throw new IllegalArgumentException("Port is less than or equal to 0");
		}

		if (null == clientUsername || clientUsername.isEmpty()) {
			throw new IllegalArgumentException("Client username is null or empty");
		}

		if (null == clientUserZone || clientUserZone.isEmpty()) {
			throw new IllegalArgumentException("Client zone is null or empty");
		}

		// iRODS expects the client and proxy information to be identical if
		// the proxy user info is not defined.
		if (null == proxyUsername) {
			proxyUsername = clientUsername;
		}

		if (null == proxyUserZone) {
			proxyUserZone = clientUserZone;
		}

		RcComm comm = new RcComm();
		comm.socket = comm.plainSocket = new Socket(host, port);

		// Create the StartupPack message.
		// This is how a connection to iRODS is always initiated.
		var sp = new StartupPack_PI();
		sp.clientUser = comm.clientUsername = clientUsername;
		sp.clientRcatZone = comm.clientUserZone = clientUserZone;
		sp.proxyUser = comm.proxyUsername = proxyUsername;
		sp.proxyRcatZone = comm.proxyUserZone = proxyUserZone;
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
			if (null != errorInfo) {
				errorInfo.status = mh.intInfo;
				errorInfo.msg = "StartupPack error";
			}
			return null;
		}

		// Negotiation

		// Prepare to the negotiate whether a secure communication
		// channel is needed. The server's response will contain its
		// choice for secure communication.
		var csneg = Network.readObject(comm.socket, mh.msgLen, CS_NEG_PI.class);
		log.debug("Received CS_NEG_PI: {}", XmlUtil.toXmlString(csneg));

		// Check for negotiation errors.
		// 1 = CS_NEG_STATUS_SUCCESS
		// 0 = CS_NEG_STATUS_FAILURE
		// See irods_client_server_negotiation.hpp for these.
		if (1 != csneg.status) {
			// TODO Handle error. Server may be in a bad state.
			log.error("Client-Server negotiation error: CS_NEG_STATUS={}", csneg.status);
		}

		// Get the client's negotiation policy and resolve it against
		// the server's policy.
		// TODO This part MUST be configurable by the client (e.g. read from
		// a config file).
		// TODO For now, let's just try to meet the server's demands. The
		// solution is to implement the negotiate function in
		// irods_client_negotiation.cpp#L268 from the main branch.
		var closeSocket = false;
		if (CS_NEG_PI.RESULT_CS_NEG_REQUIRE.equals(csneg.result)) {
			log.debug("Client-Server negotiation will use TLS");
			csneg.result = "cs_neg_result_kw=CS_NEG_USE_SSL;";
			comm.usingTLS = true;
		} else if (CS_NEG_PI.RESULT_CS_NEG_DONT_CARE.equals(csneg.result)) {
			log.debug("Client-Server negotiation will use TLS");
			csneg.result = "cs_neg_result_kw=CS_NEG_USE_SSL;";
			comm.usingTLS = true;
		} else if (CS_NEG_PI.RESULT_CS_NEG_REFUSE.equals(csneg.result)) {
			log.debug("Client-Server negotiation will not use TLS");
			csneg.result = "cs_neg_result_kw=CS_NEG_USE_TCP;";
		} else {
			// TODO Handle error. Unknown negotiation result.
			// Send the server a CS_NEG_PI request telling it the
			// negotiation failed. Then close the socket and return
			// an error (or throw an exception).
			csneg.status = 0; // CS_NEG_STATUS_FAILURE
			csneg.result = "cs_neg_result_kw=CS_NEG_FAILURE;";
			closeSocket = true;
		}

		msgbody = XmlUtil.toXmlString(csneg);
		hdr.type = MsgHeader_PI.MsgType.RODS_CS_NEG_T;
		hdr.msgLen = msgbody.length();
		Network.write(comm.socket, hdr);
		Network.writeXml(comm.socket, csneg);

		// Read the message header from the server.
		mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		// TODO Does the server automatically close the socket after a
		// failed client-server negotiation attempt?
//		if (closeSocket) {
//			comm.socket.close();
//		}

		if (mh.intInfo < 0) {
			if (null != errorInfo) {
				errorInfo.status = mh.intInfo;
				errorInfo.msg = "Client-Server negotiation error";
			}
			return null;
		}

		// Capture the server version information.
		var vers = Network.readObject(comm.socket, mh.msgLen, Version_PI.class);
		log.debug("Received Version_PI: {}", XmlUtil.toXmlString(vers));
		comm.apiVersion = vers.apiVersion;
		comm.relVersion = vers.relVersion;
		comm.status = vers.status;
		comm.cookie = vers.cookie;

		// TODO In the C implementation, this is where the network_plugin
		// is instantiated and the decision to use TLS happens. That decision
		// is based on the negotiation results, which are stored in the RcComm.
		// The RcComm holds information about encryption and other parameters.
		// That's why messages appear to be encrypted following the version
		// response from the server.
		enableTLS(comm);

		return comm;
	}

	private static void enableTLS(RcComm comm) throws Exception {
		if (comm.secure) {
			log.debug("SSL/TLS is already in use.");
			return;
		}

		if (!comm.usingTLS) {
			log.debug("Skipping enabling of SSL/TLS communication.");
			return;
		}

		// TODO The code below is needed by the PamPasswordAuthPlugin.
		// for when the client isn't using TLS initially, but wants to
		// use PAM.
		//
		// This TODO can be ignored if we avoid making the PamPasswordAuthPlugin
		// magically enable TLS. Perhaps it should throw an exception when
		// the RcComm isn't using a secure communication channel?

		// Load the truststore.
		log.debug("Loading Truststore.");
		var trustStore = KeyStore.getInstance("JKS");
		// TODO Make the truststore file configurable.
		try (var fis = new FileInputStream("/home/kory/eclipse-workspace/irods4j/truststore.jks")) {
			// TODO Make the password configurable and optional.
			trustStore.load(fis, "changeit".toCharArray());
		}

		// Initialize the TrustManager.
		log.debug("Initializing Truststore.");
		var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(trustStore);

		// Configure SSLContext with the TrustManager.
		log.debug("Initializing SSL context.");
		var sslContext = SSLContext.getInstance("TLSv1.2");
		sslContext.init(null, tmf.getTrustManagers(), null);

		// Create SSLSocket and connect.
		log.debug("Upgrading socket to use TLS.");
		var factory = sslContext.getSocketFactory();
		var host = comm.socket.getInetAddress().getHostAddress();
		var port = comm.socket.getPort();
		var autoCloseUnderlyingSocket = true;
		comm.sslSocket = (SSLSocket) factory.createSocket(comm.socket, host, port, autoCloseUnderlyingSocket);
		log.debug("Supported SSL/TLS protocols: {}", Arrays.toString(comm.sslSocket.getSupportedProtocols()));
		comm.sslSocket.startHandshake();
		log.debug("Connected securely using self-signed certificate.");

		// See ssl.cpp in irods/irods to understand the following sequence of
		// operations. The code below follows ssl_client_start() and ssl_agent_start().

		// Send SSL encryption information to server.
		// TODO Allow developer to configure these options. These are the defaults for
		// testing.
		var encryptionAlgorithm = "AES-256-CBC";
		var encryptionKeySize = 32;
		var encryptionNumHashRounds = 16;
		var encryptionSaltSize = 8;

		var mh = new MsgHeader_PI();
		mh.type = encryptionAlgorithm;
		mh.msgLen = encryptionKeySize;
		mh.errorLen = encryptionSaltSize;
		mh.bsLen = encryptionNumHashRounds;

		Network.write(comm.sslSocket, mh);

		// Generate a random byte sequence as a key and send it to the server.
		var key = new byte[encryptionKeySize];
		var secureRandom = new SecureRandom();
		secureRandom.nextBytes(key);

		var bbuf = new BytesBuf_PI();
		bbuf.buflen = key.length;
		bbuf.buf = key;
		var msgbody = XmlUtil.toXmlString(bbuf);

		mh.type = "SHARED_SECRET";
		mh.msgLen = msgbody.length();
		mh.errorLen = 0;
		mh.bsLen = 0;
		mh.intInfo = 0;

		Network.write(comm.sslSocket, mh);
		Network.writeXml(comm.sslSocket, bbuf);

		// TODO Do equivalent of sslPostConnectionCheck().
		// See sslSockComm.cpp#L90 on the main branch. The function of
		// interest is sslStart(). If the check fails, sslEnd() is called,
		// which invokes rcSslEnd() and returns an error of SSL_CERT_ERROR.

		// This keeps the rest of the code from needing to know
		// the type of the socket used for communication.
		comm.socket = comm.sslSocket;

		// Used as a signal to this function to guard against this
		// function being executed multiple times.
		comm.secure = true;
	}

	// TODO Consider removing this function.
	private static int rcSslStart(RcComm comm) throws IOException {
		var input = new SSLStartInp_PI();
		input.arg0 = null;
		sendApiRequest(comm.socket, 1100, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	// TODO Consider removing this function.
	private static int rcSslEnd(RcComm comm) throws IOException {
		var input = new SSLEndInp_PI();
		sendApiRequest(comm.socket, 1101, input);

		// Read the message header from the server.
		var mh = Network.readMsgHeader_PI(comm.socket);
		log.debug("Received MsgHeader_PI: {}", XmlUtil.toXmlString(mh));

		return mh.intInfo;
	}

	public static void rcDisconnect(RcComm comm) throws IOException {
		final var hdr = new MsgHeader_PI();
		hdr.type = MsgHeader_PI.MsgType.RODS_DISCONNECT;
		Network.write(comm.socket, hdr);
		comm.socket.close();
	}

	public static void rcAuthenticateClient(RcComm comm, String authScheme, String password) throws Exception {
		var input = JsonUtil.getJsonMapper().createObjectNode();
		input.put("password", password);
		input.put(AuthPlugin.AUTH_TTL_KEY, "0"); // TODO Expose this option and others.
		AuthManager.authenticateClient(comm, authScheme, input);
	}

	public static int rcObjStat(RcComm comm, DataObjInp_PI input, Reference<RodsObjStat_PI> output) throws IOException {
		sendApiRequest(comm.socket, 633, input);
		return readServerResponse(comm, RodsObjStat_PI.class, output, null);
	}

	public static int rcGenQuery2(RcComm comm, Genquery2Input_PI input, Reference<String> output) throws IOException {
		sendApiRequest(comm.socket, 10221, input);
		var outputPI = new Reference<STR_PI>();
		var ec = readServerResponse(comm, STR_PI.class, outputPI, null);
		if (null != outputPI.value) {
			output.value = outputPI.value.myStr;
		}
		return ec;
	}

	public static int rcReplicaOpen(RcComm comm, DataObjInp_PI input, Reference<String> output) throws IOException {
		sendApiRequest(comm.socket, 20003, input);
		var outputPI = new Reference<BinBytesBuf_PI>();
		var ec = readServerResponse(comm, BinBytesBuf_PI.class, outputPI, null);
		if (null != outputPI.value) {
			output.value = outputPI.value.decode();
		}
		return ec;
	}

	public static int rcReplicaTruncate(RcComm comm, DataObjInp_PI input, Reference<String> output) throws IOException {
		sendApiRequest(comm.socket, 802, input);
		var outputPI = new Reference<STR_PI>();
		var ec = readServerResponse(comm, STR_PI.class, outputPI, null);
		if (null != outputPI.value) {
			output.value = outputPI.value.myStr;
		}
		return ec;
	}

	public static int rcDataObjLseek(RcComm comm, OpenedDataObjInp_PI input, Reference<FileLseekOut_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 674, input);
		return readServerResponse(comm, FileLseekOut_PI.class, output, null);
	}

	public static int rcDataObjRead(RcComm comm, OpenedDataObjInp_PI input, ByteArrayReference byteArray)
			throws IOException {
		sendApiRequest(comm.socket, 675, input);
		return readServerResponse(comm, null, null, byteArray);
	}

	public static int rcDataObjWrite(RcComm comm, OpenedDataObjInp_PI input, byte[] buffer) throws IOException {
		sendApiRequest(comm.socket, 676, input, buffer);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcReplicaClose(RcComm comm, String closeOptions) throws IOException {
		var input = new BinBytesBuf_PI(closeOptions);
		sendApiRequest(comm.socket, 20004, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcAtomicApplyMetadataOperations(RcComm comm, String input, Reference<String> output)
			throws IOException {
		var bbbuf = new BinBytesBuf_PI(input);
		sendApiRequest(comm.socket, 20002, bbbuf);

		var outputPI = new Reference<BinBytesBuf_PI>();
		var ec = readServerResponse(comm, BinBytesBuf_PI.class, outputPI, null);
		if (null != outputPI.value) {
			output.value = outputPI.value.decode();
		}

		return ec;
	}

	public static int rcAtomicApplyAclOperations(RcComm comm, String input, Reference<String> output)
			throws IOException {
		var bbbuf = new BinBytesBuf_PI(input);
		sendApiRequest(comm.socket, 20005, bbbuf);

		var outputPI = new Reference<BinBytesBuf_PI>();
		var ec = readServerResponse(comm, BinBytesBuf_PI.class, outputPI, null);
		if (null != outputPI.value) {
			output.value = outputPI.value.decode();
		}

		return ec;
	}

	public static int rcTouch(RcComm comm, String input) throws IOException {
		var bbbuf = new BinBytesBuf_PI(input);
		sendApiRequest(comm.socket, 20007, bbbuf);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcGetGridConfigurationValue(RcComm comm, GridConfigurationInp_PI input,
			Reference<GridConfigurationOut_PI> output) throws IOException {
		sendApiRequest(comm.socket, 20009, input);
		return readServerResponse(comm, GridConfigurationOut_PI.class, output, null);
	}

	public static int rcSetGridConfigurationValue(RcComm comm, GridConfigurationInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 20010, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcSetDelayServerMigrationInfo(RcComm comm, DelayServerMigrationInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 20011, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcGetDelayRuleInfo(RcComm comm, String input, Reference<String> output) throws IOException {
		var strPI = new STR_PI();
		strPI.myStr = input;
		sendApiRequest(comm.socket, 20013, strPI);

		var outputPI = new Reference<BinBytesBuf_PI>();
		var ec = readServerResponse(comm, BinBytesBuf_PI.class, outputPI, null);
		if (null != outputPI.value) {
			output.value = outputPI.value.decode();
		}

		return ec;
	}

	public static int rcGetFileDescriptorInfo(RcComm comm, String input, Reference<String> output) throws IOException {
		var bbbuf = new BinBytesBuf_PI(input);
		sendApiRequest(comm.socket, 20000, bbbuf);

		var outputPI = new Reference<BinBytesBuf_PI>();
		var ec = readServerResponse(comm, BinBytesBuf_PI.class, outputPI, null);
		if (null != outputPI.value) {
			output.value = outputPI.value.decode();
		}

		return ec;
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
		var ec = readServerResponse(comm, null, null, null);

		if (0 == ec) {
			comm.clientUsername = input.username;
			comm.clientUserZone = input.zone;

			if (null != input.KeyValPair_PI && input.KeyValPair_PI.ssLen > 0
					&& input.KeyValPair_PI.keyWord.contains(SwitchUserInp_PI.KW_SWITCH_PROXY_USER)) {
				comm.proxyUsername = input.username;
				comm.proxyUserZone = input.zone;
			}
		}

		return ec;
	}

	public static int rcCheckAuthCredentials(RcComm comm, DataObjInp_PI input, Reference<Integer> output)
			throws IOException {
		sendApiRequest(comm.socket, 800, input);
		var outputPI = new Reference<INT_PI>();
		var ec = readServerResponse(comm, INT_PI.class, outputPI, null);
		if (null != outputPI.value) {
			output.value = outputPI.value.myInt;
		}
		return ec;
	}

	// TODO Should this API be exposed to clients?
	public static int rcRegisterPhysicalPath(RcComm comm, DataObjInp_PI input, Reference<String> output)
			throws IOException {
		sendApiRequest(comm.socket, 20008, input);
		var outputPI = new Reference<BinBytesBuf_PI>();
		var ec = readServerResponse(comm, BinBytesBuf_PI.class, outputPI, null);
		if (null != outputPI.value) {
			output.value = outputPI.value.decode();
		}
		return ec;
	}

	public static int rcModAVUMetadata(RcComm comm, ModAVUMetadataInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 706, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcModAccessControl(RcComm comm, ModAccessControlInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 707, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcModDataObjMeta(RcComm comm, ModDataObjMeta_PI input) throws IOException {
		sendApiRequest(comm.socket, 622, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcDataObjectModifyInfo(RcComm comm, ModDataObjMeta_PI input) throws IOException {
		sendApiRequest(comm.socket, 20001, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcDataObjRename(RcComm comm, DataObjCopyInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 627, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcDataObjCopy(RcComm comm, DataObjCopyInp_PI input, Reference<TransferStat_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 696, input);
		return readServerResponse(comm, TransferStat_PI.class, output, null);
	}

	public static int rcDataObjRepl(RcComm comm, DataObjInp_PI input, Reference<TransferStat_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 695, input);
		return readServerResponse(comm, TransferStat_PI.class, output, null);
	}

	public static int rcDataObjCreate(RcComm comm, DataObjInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 601, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcDataObjChksum(RcComm comm, DataObjInp_PI input, Reference<String> output) throws IOException {
		sendApiRequest(comm.socket, 629, input);
		var outputPI = new Reference<STR_PI>();
		var ec = readServerResponse(comm, STR_PI.class, outputPI, null);
		if (null != outputPI.value) {
			output.value = outputPI.value.myStr;
		}
		return ec;
	}

	public static int rcDataObjUnlink(RcComm comm, DataObjInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 615, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcDataObjTrim(RcComm comm, DataObjInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 632, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcDelayRuleLock(RcComm comm, DelayRuleLockInput_PI input) throws IOException {
		sendApiRequest(comm.socket, 10222, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcDelayRuleUnlock(RcComm comm, DelayRuleUnlockInput_PI input) throws IOException {
		sendApiRequest(comm.socket, 10223, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcCollCreate(RcComm comm, CollInpNew_PI input) throws IOException {
		sendApiRequest(comm.socket, 681, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcModColl(RcComm comm, CollInpNew_PI input) throws IOException {
		sendApiRequest(comm.socket, 680, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcOpenCollection(RcComm comm, CollInpNew_PI input) throws IOException {
		sendApiRequest(comm.socket, 678, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcReadCollection(RcComm comm, int handle, Reference<CollEnt_PI> output) throws IOException {
		var input = new INT_PI();
		input.myInt = handle;
		sendApiRequest(comm.socket, 713, input);
		return readServerResponse(comm, CollEnt_PI.class, output, null);
	}

	public static int rcCloseCollection(RcComm comm, int handle) throws IOException {
		var input = new INT_PI();
		input.myInt = handle;
		sendApiRequest(comm.socket, 661, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcRmColl(RcComm comm, CollInpNew_PI input, Reference<CollOprStat_PI> output) throws IOException {
		sendApiRequest(comm.socket, 679, input);
		return readServerResponse(comm, CollOprStat_PI.class, output, null);
	}

	public static int rcTicketAdmin(RcComm comm, TicketAdminInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 723, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcUnregDataObj(RcComm comm, UnregDataObj_PI input) throws IOException {
		sendApiRequest(comm.socket, 620, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcUserAdmin(RcComm comm, UserAdminInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 714, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcSpecificQuery(RcComm comm, SpecificQueryInp_PI input, Reference<GenQueryOut_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 722, input);
		return readServerResponse(comm, GenQueryOut_PI.class, output, null);
	}

	public static int rcGetResourceInfoForOperation(RcComm comm, DataObjInp_PI input, Reference<String> output)
			throws IOException {
		sendApiRequest(comm.socket, 10220, input);
		var outputPI = new Reference<STR_PI>();
		var ec = readServerResponse(comm, STR_PI.class, outputPI, null);
		if (null != outputPI.value) {
			output.value = outputPI.value.myStr;
		}
		return ec;
	}

	public static int rcZoneReport(RcComm comm, SpecificQueryInp_PI input, Reference<BytesBuf_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 10205, input);
		return readServerResponse(comm, BytesBuf_PI.class, output, null);
	}

	public static int rcGetMiscSvrInfo(RcComm comm, Reference<MiscSvrInfo_PI> output) throws IOException {
		sendApiRequest(comm.socket, 700);
		return readServerResponse(comm, MiscSvrInfo_PI.class, output, null);
	}

	public static int rcGeneralAdmin(RcComm comm, GeneralAdminInp_PI input) throws IOException {
		sendApiRequest(comm.socket, 701, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcExecMyRule(RcComm comm, ExecMyRuleInp_PI input, Reference<MsParamArray_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 625, input);
		return readServerResponse(comm, MsParamArray_PI.class, output, null);
	}

	public static int rcProcStat(RcComm comm, ProcStatInp_PI input, Reference<GenQueryOut_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 690);
		return readServerResponse(comm, GenQueryOut_PI.class, output, null);
	}

	public static int rcRuleExecSubmit(RcComm comm, RULE_EXEC_DEL_INP_PI input, Reference<String> output)
			throws IOException {
		sendApiRequest(comm.socket, 623, input);
		var outputPI = new Reference<IRODS_STR_PI>();
		var ec = readServerResponse(comm, IRODS_STR_PI.class, outputPI, null);
		if (null != outputPI.value) {
			output.value = outputPI.value.myStr;
		}
		return ec;
	}

	public static int rcRuleExecMod(RcComm comm, RULE_EXEC_MOD_INP_PI input) throws IOException {
		sendApiRequest(comm.socket, 708, input);
		return readServerResponse(comm, null, null, null);
	}

	public static int rcRuleExecDel(RcComm comm, RULE_EXEC_DEL_INP_PI input) throws IOException {
		sendApiRequest(comm.socket, 624, input);
		return readServerResponse(comm, null, null, null);
	}

	// TODO This API is likely for server-side use only due to it only being invoked
	// within the server. Consider removing this.
	private static int rcRegReplica(RcComm comm, RegReplica_PI input) throws IOException {
		sendApiRequest(comm.socket, 621, input);
		return readServerResponse(comm, null, null, null);
	}

	// TODO This API is not used in server at all. We should probably deprecate it
	// or remove
	// it from the public interface. Consider removing this.
	private static int rcRegDataOb(RcComm comm, DataObjInfo_PI input, Reference<DataObjInfo_PI> output)
			throws IOException {
		sendApiRequest(comm.socket, 619, input);
		return readServerResponse(comm, DataObjInfo_PI.class, output, null);
	}

	public static int rcGetLibraryFeatures(RcComm comm, Reference<String> output) throws IOException {
		sendApiRequest(comm.socket, 801);
		var outputPI = new Reference<STR_PI>();
		var ec = readServerResponse(comm, STR_PI.class, outputPI, null);
		if (null != outputPI.value) {
			output.value = outputPI.value.myStr;
		}
		return ec;
	}

}
