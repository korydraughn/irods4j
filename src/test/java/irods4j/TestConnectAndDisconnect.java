package irods4j;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;

import org.irods.irods4j.low_level.network.Network;
import org.irods.irods4j.low_level.protocol.packing_instructions.BinBytesBuf_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.Genquery2Input_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.MsgHeader_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.StartupPack_PI;
import org.irods.irods4j.low_level.util.NullSerializer;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.XmlSerializerProvider;
import com.fasterxml.jackson.dataformat.xml.util.XmlRootNameLookup;

class TestConnectAndDisconnect {

	@Test
	void test() throws UnknownHostException, IOException, InterruptedException, NoSuchAlgorithmException {
		var provider = new XmlSerializerProvider(new XmlRootNameLookup());
		provider.setNullValueSerializer(new NullSerializer());

		var xm = new XmlMapper();
		xm.setSerializerProvider(provider);

		var socket = new Socket("localhost", 1247);

		try {
			// Create the StartupPack message.
			// This is how a connection to iRODS is always initiated.
			var sp = new StartupPack_PI();
			sp.clientUser = "kory";
			sp.clientRcatZone = "tempZone";
			sp.proxyUser = "kory";
			sp.proxyRcatZone = "tempZone";
			sp.option = "irods4jrequest_server_negotiation";
			var msgbody = xm.writeValueAsString(sp);

			// Create the header describing the StartupPack message.
			var hdr = new MsgHeader_PI();
			hdr.type = MsgHeader_PI.MsgType.RODS_CONNECT;
			hdr.msgLen = msgbody.length();

			// Send the message header and StartupPack (i.e. the message body).
			Network.send(socket, hdr);
			Network.sendXml(socket, sp);

			// Read the message header from the server.
			var mh = Network.readMsgHeader_PI(socket);
			System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

			if (mh.intInfo < 0) {
				System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
				return;
			}

			// Prepare to the negotiate whether a secure communication
			// channel is needed.
			var csneg = Network.readCS_NEG_PI(socket, mh.msgLen);
			System.out.println("received CS_NEG_PI: " + xm.writeValueAsString(csneg));

			// No TLS support implemented at this time, so tell the server
			// we want to move forward without TLS.
			csneg.result = "cs_neg_result_kw=CS_NEG_USE_TCP;";
			msgbody = xm.writeValueAsString(csneg);
			hdr.type = MsgHeader_PI.MsgType.RODS_CS_NEG_T;
			hdr.msgLen = msgbody.length();
			Network.send(socket, hdr);
			Network.sendXml(socket, csneg);

			// Read the message header from the server.
			mh = Network.readMsgHeader_PI(socket);
			System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

			if (mh.intInfo < 0) {
				System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
				return;
			}

			// Capture the server version information.
			var vers = Network.readVersion_PI(socket, mh.msgLen);
			System.out.println("received Version_PI: " + xm.writeValueAsString(vers));

			// Send API request.
			var jm = new ObjectMapper();
			var msg = new HashMap<String, Object>() {
				private static final long serialVersionUID = 1L;

				{
					put("a_ttl", "0");
					put("force_password_prompt", Boolean.TRUE);
					put("next_operation", "auth_agent_auth_request");
					put("scheme", "native");
					put("user_name", "kory");
					put("zone_name", "tempZone");
				}
			};
			var json = jm.writeValueAsString(msg);
			msgbody = xm.writeValueAsString(new BinBytesBuf_PI(json));
			hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
			hdr.msgLen = msgbody.length();
			hdr.intInfo = 110000; // New auth plugin framework API number.
			Network.send(socket, hdr);
			Network.sendXml(socket, new BinBytesBuf_PI(json));

			// Read the message header from the server.
			mh = Network.readMsgHeader_PI(socket);
			System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

			if (mh.intInfo < 0) {
				System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
				return;
			}

			var bbbuf = Network.readBinBytesBuf_PI(socket, mh.msgLen);
			System.out.println("received BinBytesBuf_PI: " + xm.writeValueAsString(bbbuf));
			System.out.println("BinBytesBuf_PI contents: " + bbbuf.decode());

			var jsonContent = jm.readValue(bbbuf.decode(), new TypeReference<HashMap<String, Object>>() {
			});
			var requestResult = (String) jsonContent.get("request_result");
			var signature = requestResult.substring(0, 16);
			System.out.println("signature = " + signature);
			System.out.println("signature length = " + signature.length());

			// Generate the MD5 hash for challenge response.
			var pwsb = new StringBuilder();
			pwsb.append("rods");
			pwsb.setLength(50); // Pad the string with null bytes until it has a length of 50 bytes.
			var digest = MessageDigest.getInstance("md5");
			digest.update(requestResult.getBytes());
			digest.update(pwsb.toString().getBytes());
			var challengeResponse = Base64.getEncoder().encodeToString(digest.digest());
			System.out.println("challengeResponse = " + challengeResponse);

			msg.put("next_operation", "auth_agent_auth_response");
			msg.put("digest", challengeResponse);

			json = jm.writeValueAsString(msg);
			bbbuf = new BinBytesBuf_PI(json);
			msgbody = xm.writeValueAsString(bbbuf);
			hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
			hdr.msgLen = msgbody.length();
			hdr.intInfo = 110000; // New auth plugin framework API number.
			Network.send(socket, hdr);
			Network.sendXml(socket, bbbuf);

			// Read the message header from the server.
			mh = Network.readMsgHeader_PI(socket);
			System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

			if (mh.intInfo < 0) {
				System.out.println("Authentication Error: " + mh.intInfo);
				return;
			}

			bbbuf = Network.readBinBytesBuf_PI(socket, mh.msgLen);
			System.out.println("received BinBytesBuf_PI: " + xm.writeValueAsString(bbbuf));
			System.out.println("BinBytesBuf_PI contents: " + bbbuf.decode());

			System.out.println("Authentication Successful!");

			// ---------------
			// ENTER MAIN LOOP

			// Stat the home collection.
			var doi = new DataObjInp_PI();
			doi.objPath = "/tempZone/home/kory";
			msgbody = xm.writeValueAsString(doi);

			// Create the header describing the message.
			hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
			hdr.msgLen = msgbody.length();
			hdr.intInfo = 633; // OBJ_STAT_AN

			// Send the message header and DataObjInp (i.e. the message body).
			Network.send(socket, hdr);
			Network.sendXml(socket, doi);

			// Read the message header from the server.
			mh = Network.readMsgHeader_PI(socket);
			System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

			if (mh.intInfo < 0) {
				System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
				return;
			}

			var stat = Network.readRodsObjStat_PI(socket, mh.msgLen);
			System.out.println("received RodsObjStat_PI: " + xm.writeValueAsString(stat));

			// Execute GenQuery2 query.
			var gq2i = new Genquery2Input_PI();
			gq2i.query_string = "select COLL_NAME, DATA_NAME where COLL_NAME = '/tempZone/home/kory'";
			gq2i.zone = "tempZone";
			msgbody = xm.writeValueAsString(gq2i);

			// Create the header describing the message.
			hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
			hdr.msgLen = msgbody.length();
			hdr.intInfo = 10221; // GENQUERY2_AN

			// Send the message header and Genquery2Input (i.e. the message body).
			Network.send(socket, hdr);
			Network.sendXml(socket, gq2i);

			// Read the message header from the server.
			mh = Network.readMsgHeader_PI(socket);
			System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

			if (mh.intInfo < 0) {
				System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
				return;
			}

			var queryResults = Network.readSTR_PI(socket, mh.msgLen);
			System.out.println("received STR_PI: " + xm.writeValueAsString(queryResults));
			System.out.println("query results: " + queryResults.myStr);

			// ---------------

			hdr.type = MsgHeader_PI.MsgType.RODS_DISCONNECT;
			hdr.msgLen = 0;
			hdr.intInfo = 0; // New auth plugin framework API number.
			Network.send(socket, hdr);

			System.out.println("Disconnect Successful.");
		} finally {
			socket.close();
		}
	}

}
