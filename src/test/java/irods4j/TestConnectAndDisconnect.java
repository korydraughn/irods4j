package irods4j;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import network.Network;
import protocol.BinBytesBuf_PI;
import protocol.MsgHeader_PI;
import protocol.StartupPack_PI;

class TestConnectAndDisconnect {

	@Test
	void test() throws UnknownHostException, IOException, InterruptedException {
		var xm = new XmlMapper();
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

			// Capture the server version information.
			var vers = Network.readVersion_PI(socket, mh.msgLen);
			System.out.println("received Version_PI: " + xm.writeValueAsString(vers));
			
			// Send API request.
			var jm = new ObjectMapper();
			var msg = new HashMap<String, Object>() {{
				put("a_ttl", "0");
				put("force_password_prompt", Boolean.TRUE);
//				put("force_password_prompt", "true");
				put("next_operation", "auth_agent_auth_request");
				put("scheme", "native");
				put("user_name", "kory");
				put("zone_name", "tempZone");
			}};
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

			var bbbuf = Network.readBinBytesBuf_PI(socket, mh.msgLen);
			System.out.println("received BinBytesBuf_PI: " + xm.writeValueAsString(bbbuf));
			System.out.println("BinBytesBuf_PI contents: " + bbbuf.decode());
		}
		finally {
			socket.close();
		}
	}

}
