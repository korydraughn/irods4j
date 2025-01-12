package irods4j;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;

import org.irods.irods4j.low_level.network.Network;
import org.irods.irods4j.low_level.protocol.packing_instructions.BinBytesBuf_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.CollInpNew_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI.PosixOpenFlags;
import org.irods.irods4j.low_level.protocol.packing_instructions.Genquery2Input_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.KeyValPair_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.ModAVUMetadataInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.MsgHeader_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.OpenedDataObjInp_PI;
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
			{
				var input = new DataObjInp_PI();
				input.objPath = "/tempZone/home/kory";
				msgbody = xm.writeValueAsString(input);

				// Create the header describing the message.
				hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
				hdr.msgLen = msgbody.length();
				hdr.intInfo = 633; // OBJ_STAT_AN

				// Send the message header and DataObjInp (i.e. the message body).
				Network.send(socket, hdr);
				Network.sendXml(socket, input);

				// Read the message header from the server.
				mh = Network.readMsgHeader_PI(socket);
				System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

				if (mh.intInfo < 0) {
					System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
					return;
				}

				var stat = Network.readRodsObjStat_PI(socket, mh.msgLen);
				System.out.println("received RodsObjStat_PI: " + xm.writeValueAsString(stat));
			}

			// Execute GenQuery2 query.
			{
				var input = new Genquery2Input_PI();
				input.query_string = "select COLL_NAME, DATA_NAME where COLL_NAME = '/tempZone/home/kory'";
				input.zone = "tempZone";
				msgbody = xm.writeValueAsString(input);

				// Create the header describing the message.
				hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
				hdr.msgLen = msgbody.length();
				hdr.intInfo = 10221; // GENQUERY2_AN

				// Send the message header and Genquery2Input (i.e. the message body).
				Network.send(socket, hdr);
				Network.sendXml(socket, input);

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
			}

			// Attach metadata.
			{
				var input = new ModAVUMetadataInp_PI();
				input.arg0 = "set";
				input.arg1 = "-d";
				input.arg2 = "/tempZone/home/kory/foo";
				input.arg3 = "irods4j::name";
				input.arg4 = "irods4j::value";
				input.arg5 = "irods4j::unit";
				msgbody = xm.writeValueAsString(input);

				// Create the header describing the message.
				hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
				hdr.msgLen = msgbody.length();
				hdr.intInfo = 706; // MOD_AVU_METADATA_AN

				// Send the message header and Genquery2Input (i.e. the message body).
				Network.send(socket, hdr);
				Network.sendXml(socket, input);

				// Read the message header from the server.
				mh = Network.readMsgHeader_PI(socket);
				System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

				if (mh.intInfo < 0) {
					System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
					return;
				}
			}

			// Calculate checksum.
			{
				var input = new DataObjInp_PI();
				input.objPath = "/tempZone/home/kory/foo";
				input.KeyValPair_PI = new KeyValPair_PI();
				input.KeyValPair_PI.ssLen = 1;
				input.KeyValPair_PI.keyWord = new ArrayList<>();
				input.KeyValPair_PI.keyWord.add("forceFlag"); // FORCE_FLAG_KW
				input.KeyValPair_PI.svalue = new ArrayList<>();
				input.KeyValPair_PI.svalue.add("");
				msgbody = xm.writeValueAsString(input);

				// Create the header describing the message.
				hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
				hdr.msgLen = msgbody.length();
				hdr.intInfo = 629; // DATA_OBJ_CHKSUM_AN

				// Send the message header.
				Network.send(socket, hdr);
				Network.sendXml(socket, input);

				// Read the message header from the server.
				mh = Network.readMsgHeader_PI(socket);
				System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

				// Check for errors.
				if (mh.intInfo < 0) {
					System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
					return;
				}

				var result = Network.readSTR_PI(socket, mh.msgLen);
				System.out.println("received STR_PI: " + xm.writeValueAsString(result));
				System.out.println("result: " + result.myStr);
			}

			// Delete and create a new collection.
			{
				// Delete collection.
//				var input = new CollInpNew_PI();
//				input.collName = "/tempZone/home/kory/new_coll";
//				msgbody = xm.writeValueAsString(input);
//
//				// Create the header describing the message.
//				hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
//				hdr.msgLen = msgbody.length();
//				hdr.intInfo = 681; // COLL_CREATE_AN
//
//				// Send the message header.
//				Network.send(socket, hdr);
//				Network.sendXml(socket, input);
//
//				// Read the message header from the server.
//				mh = Network.readMsgHeader_PI(socket);
//				System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));
//
//				// Check for errors.
//				if (mh.intInfo < 0) {
//					System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
//					return;
//				}

				// Create collection.
				var input = new CollInpNew_PI();
				input.collName = "/tempZone/home/kory/new_coll";
				msgbody = xm.writeValueAsString(input);

				// Create the header describing the message.
				hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
				hdr.msgLen = msgbody.length();
				hdr.intInfo = 681; // COLL_CREATE_AN

				// Send the message header.
				Network.send(socket, hdr);
				Network.sendXml(socket, input);

				// Read the message header from the server.
				mh = Network.readMsgHeader_PI(socket);
				System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

				// Check for errors.
				if (mh.intInfo < 0) {
					System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
					// return; // TODO Ignore for now. Don't care that the collection already
					// exists.
				}
			}

			// Open, read, and close a data object.
			{
				// Open
				var openInput = new DataObjInp_PI();
				openInput.objPath = "/tempZone/home/kory/foo";
				openInput.dataSize = -1;
				openInput.createMode = 0600;
				openInput.openFlags = PosixOpenFlags.O_RDONLY;
				openInput.KeyValPair_PI = new KeyValPair_PI();
				openInput.KeyValPair_PI.ssLen = 0;
				msgbody = xm.writeValueAsString(openInput);

				// Create the header describing the message.
				hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
				hdr.msgLen = msgbody.length();
				hdr.intInfo = 20003; // REPLICA_OPEN_APN

				// Send the message header.
				Network.send(socket, hdr);
				Network.sendXml(socket, openInput);

				// Read the message header from the server.
				mh = Network.readMsgHeader_PI(socket);
				System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

				// Check for errors. We should never see a L1descInx less than 3.
				// If we do, something horrible has happened on the server side.
				if (mh.intInfo < 0) {
					System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
					return;
				}

				// Capture the L1descInx (i.e. the iRODS file descriptor).
				var fd = mh.intInfo;

				bbbuf = Network.readBinBytesBuf_PI(socket, mh.msgLen);
				System.out.println("received BinBytesBuf_PI: " + xm.writeValueAsString(bbbuf));
				System.out.println("BinBytesBuf_PI contents: " + bbbuf.decode());

				// Read
				final int numBytesToRead = 8192;
				var readInput = new OpenedDataObjInp_PI();
				readInput.l1descInx = fd;
				readInput.len = numBytesToRead;
				msgbody = xm.writeValueAsString(readInput);

				// Create the header describing the message.
				hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
				hdr.msgLen = msgbody.length();
				hdr.intInfo = 675; // DATA_OBJ_READ_AN

				// Send the message header.
				Network.send(socket, hdr);
				Network.sendXml(socket, readInput);

				// Read the message header from the server.
				mh = Network.readMsgHeader_PI(socket);
				System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

				// Check for errors.
				if (mh.intInfo < 0) {
					System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
					return;
				}

				// For a read operation, remember the error code represents the
				// total number of bytes read.
				System.out.println("Total bytes read: " + mh.intInfo);

				// Check to see if the byte stream contains data. If the server
				// return any data via the byte stream, it will appear directly
				// after the MsgHeader_PI message.
				if (mh.bsLen > 0) {
					var bytes = Network.readBytes(socket, mh.bsLen);
					System.out.print("read: ");
					System.out.write(bytes);
				}

				// Close
				msg.clear();
				msg.put("fd", fd);
				json = jm.writeValueAsString(msg);
				var closeInput = new BinBytesBuf_PI(json);
				msgbody = xm.writeValueAsString(closeInput);

				// Create the header describing the message.
				hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
				hdr.msgLen = msgbody.length();
				hdr.intInfo = 20004; // REPLICA_CLOSE_APN

				// Send the message header.
				Network.send(socket, hdr);
				Network.sendXml(socket, closeInput);

				// Read the message header from the server.
				mh = Network.readMsgHeader_PI(socket);
				System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

				// Check for errors.
				if (mh.intInfo < 0) {
					System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
					return;
				}
			}

			// Open, write, and close a data object.
			{
				// Open
				var openInput = new DataObjInp_PI();
				openInput.objPath = "/tempZone/home/kory/irods4j";
				openInput.dataSize = -1;
				openInput.createMode = 0600;
				openInput.openFlags = PosixOpenFlags.O_CREAT | PosixOpenFlags.O_WRONLY | PosixOpenFlags.O_TRUNC;
				openInput.KeyValPair_PI = new KeyValPair_PI();
				openInput.KeyValPair_PI.keyWord = new ArrayList<>();
				openInput.KeyValPair_PI.svalue = new ArrayList<>();
				msgbody = xm.writeValueAsString(openInput);

				// Create the header describing the message.
				hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
				hdr.msgLen = msgbody.length();
				hdr.intInfo = 20003; // REPLICA_OPEN_APN

				// Send the message header.
				Network.send(socket, hdr);
				Network.sendXml(socket, openInput);

				// Read the message header from the server.
				mh = Network.readMsgHeader_PI(socket);
				System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

				// Check for errors. We should never see a L1descInx less than 3.
				// If we do, something horrible has happened on the server side.
				if (mh.intInfo < 3) {
					System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
					return;
				}

				// Capture the L1descInx (i.e. the iRODS file descriptor).
				var fd = mh.intInfo;

				bbbuf = Network.readBinBytesBuf_PI(socket, mh.msgLen);
				System.out.println("received BinBytesBuf_PI: " + xm.writeValueAsString(bbbuf));
				System.out.println("BinBytesBuf_PI contents: " + bbbuf.decode());

				// Write
				final var data = "Can't believe it's not butter!".getBytes(); // TODO Perhaps these should be encoded as UTF-8.
				final var numBytesToWrite = data.length;
				var writeInput = new OpenedDataObjInp_PI();
				writeInput.l1descInx = fd;
				writeInput.len = numBytesToWrite;
				writeInput.KeyValPair_PI = new KeyValPair_PI();
				writeInput.KeyValPair_PI.ssLen = 0;
				msgbody = xm.writeValueAsString(writeInput);

				// Create the header describing the message.
				hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
				hdr.msgLen = msgbody.length();
				hdr.intInfo = 676; // DATA_OBJ_WRITE_AN
				// Tell the server we're including a byte stream. This is required
				// by the rxDataObjWrite API operation.
				hdr.bsLen = numBytesToWrite;

				// Send the message header.
				Network.send(socket, hdr);
				Network.sendXml(socket, writeInput);
				
				// Send the bytes to write to the replica.
				Network.sendBytes(socket, data);

				// Read the message header from the server.
				mh = Network.readMsgHeader_PI(socket);
				System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

				// Check for errors.
				if (mh.intInfo < 0) {
					System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
					return;
				}

				// For a write operation, remember the error code represents the
				// total number of bytes written.
				System.out.println("Total bytes written: " + mh.intInfo);

				// Close
				msg.clear();
				msg.put("fd", fd);
				json = jm.writeValueAsString(msg);
				var closeInput = new BinBytesBuf_PI(json);
				msgbody = xm.writeValueAsString(closeInput);

				// Create the header describing the message.
				hdr.type = MsgHeader_PI.MsgType.RODS_API_REQ;
				hdr.msgLen = msgbody.length();
				hdr.intInfo = 20004; // REPLICA_CLOSE_APN
				hdr.bsLen = 0;

				// Send the message header.
				Network.send(socket, hdr);
				Network.sendXml(socket, closeInput);

				// Read the message header from the server.
				mh = Network.readMsgHeader_PI(socket);
				System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

				// Check for errors.
				if (mh.intInfo < 0) {
					System.out.println("Error reading MsgHeader_PI from socket: " + mh.intInfo);
					return;
				}
			}

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
