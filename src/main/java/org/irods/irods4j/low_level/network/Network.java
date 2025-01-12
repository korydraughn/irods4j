package org.irods.irods4j.low_level.network;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.irods.irods4j.low_level.protocol.packing_instructions.BinBytesBuf_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.BytesBuf_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.CS_NEG_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.MsgHeader_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.RodsObjStat_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.STR_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.Version_PI;
import org.irods.irods4j.low_level.util.NullSerializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.XmlSerializerProvider;
import com.fasterxml.jackson.dataformat.xml.util.XmlRootNameLookup;

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
		var provider = new XmlSerializerProvider(new XmlRootNameLookup());
		provider.setNullValueSerializer(new NullSerializer());
		var xm = new XmlMapper();
		xm.setSerializerProvider(provider);
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

	public static RodsObjStat_PI readRodsObjStat_PI(Socket socket, int size) throws IOException {
		var in = socket.getInputStream();
		var bytes = in.readNBytes(size);
		var xm = new XmlMapper();
		return xm.readValue(bytes, RodsObjStat_PI.class);
	}

	public static STR_PI readSTR_PI(Socket socket, int size) throws IOException {
		var in = socket.getInputStream();
		var bytes = in.readNBytes(size);
		var xm = new XmlMapper();
		return xm.readValue(bytes, STR_PI.class);
	}

	public static BytesBuf_PI readBytesBuf_PI(Socket socket, int size) throws IOException {
		var in = socket.getInputStream();
		var bytes = in.readNBytes(size);
		var xm = new XmlMapper();
		return xm.readValue(bytes, BytesBuf_PI.class);
	}

	public static byte[] readBytes(Socket socket, int size) throws IOException {
		return socket.getInputStream().readNBytes(size);
	}

	public static void sendBytes(Socket socket, byte[] bytes) throws IOException {
		socket.getOutputStream().write(bytes);
	}

	public static <T> T readObject(Socket socket, int size) throws IOException {
		var in = socket.getInputStream();
		var bytes = in.readNBytes(size);
		var xm = new XmlMapper();
		return xm.readValue(bytes, new TypeReference<T>() {});
	}

}
