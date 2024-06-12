package network;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import protocol.BinBytesBuf_PI;
import protocol.CS_NEG_PI;
import protocol.MsgHeader_PI;
import protocol.Version_PI;

public class Network {

	public static void send(Socket socket, MsgHeader_PI msgHeader) throws IOException {
		var xm = new XmlMapper();
		var msg = xm.writeValueAsString(msgHeader);

		var bbuf = ByteBuffer.allocate(4);
		bbuf.order(ByteOrder.BIG_ENDIAN);
		bbuf.putInt(msg.length());

		var out = socket.getOutputStream();
		out.write(bbuf.array());
		out.write(msg.getBytes());
		out.flush();
	}
	
	public static <T> void sendXml(Socket socket, T object) throws IOException {
		var xm = new XmlMapper();
		var msg = xm.writeValueAsString(object);
		System.out.println(msg);
		var out = socket.getOutputStream();
		out.write(msg.getBytes());
		out.flush();
	}
	
	public static <T> void sendJson(Socket socket, T object) throws IOException {
		var jm = new ObjectMapper();
		var msg = jm.writeValueAsString(object);
		System.out.println(msg);
		var out = socket.getOutputStream();
		out.write(msg.getBytes());
		out.flush();
	}
	
	public static MsgHeader_PI readMsgHeader_PI(Socket socket) throws IOException {
		var bbuf = ByteBuffer.allocate(4);
		bbuf.order(ByteOrder.BIG_ENDIAN);

		var in = socket.getInputStream();
		var msgHeaderLengthBytes = in.readNBytes(4);
		bbuf.put(msgHeaderLengthBytes);
		bbuf.flip(); // Flip so we can read from the buffer.
		var msgHeaderLength = bbuf.getInt();
		
		var msgHeaderBytes = in.readNBytes(msgHeaderLength);
		var xm = new XmlMapper();
		return xm.readValue(msgHeaderBytes, MsgHeader_PI.class);
	}
	
	public static CS_NEG_PI readCS_NEG_PI(Socket socket, int size) throws IOException {
		var in = socket.getInputStream();
		var bytes = in.readNBytes(size);
		var xm = new XmlMapper();
		return xm.readValue(bytes, CS_NEG_PI.class);
	}
	
	public static Version_PI readVersion_PI(Socket socket, int size) throws IOException {
		var in = socket.getInputStream();
		var bytes = in.readNBytes(size);
		var xm = new XmlMapper();
		return xm.readValue(bytes, Version_PI.class);
	}
	
	public static BinBytesBuf_PI readBinBytesBuf_PI(Socket socket, int size) throws IOException {
		var in = socket.getInputStream();
		var bytes = in.readNBytes(size);
		var xm = new XmlMapper();
		return xm.readValue(bytes, BinBytesBuf_PI.class);
	}

}
