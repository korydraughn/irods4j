package irods4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import network.Network;
import protocol.MsgHeader_PI;
import protocol.StartupPack_PI;

class TestConnectAndDisconnect {

	@Test
	void test() throws UnknownHostException, IOException, InterruptedException {
		var socket = new Socket("localhost", 1247);

		try {
			var sp = new StartupPack_PI();
			sp.clientUser = "kory";
			sp.clientRcatZone = "tempZone";
			sp.option = "irods4jrequest_server_negotiation";
			
			var xm = new XmlMapper();
			var msgbody = xm.writeValueAsString(sp);
			
			var hdr = new MsgHeader_PI();
			hdr.type = MsgHeader_PI.MsgType.RODS_CONNECT;
			hdr.msgLen = msgbody.length();
			
			Network.send(socket, hdr);
			Network.send(socket, sp);

			var mh = Network.readMsgHeader_PI(socket);
			System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));

			var csneg = Network.readCS_NEG_PI(socket, mh.msgLen);
			System.out.println("received CS_NEG_PI: " + xm.writeValueAsString(csneg));
			
			csneg.result = "cs_neg_result_kw=CS_NEG_USE_TCP;";
			msgbody = xm.writeValueAsString(csneg);
			hdr.type = MsgHeader_PI.MsgType.RODS_CS_NEG_T;
			hdr.msgLen = msgbody.length();
			Network.send(socket, hdr);
			Network.send(socket, csneg);

			mh = Network.readMsgHeader_PI(socket);
			System.out.println("received MsgHeader_PI: " + xm.writeValueAsString(mh));
		}
		finally {
			socket.close();
		}
	}

}
