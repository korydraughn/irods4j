package org.irods.irods4j.low_level.network;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.common.XmlUtil;
import org.irods.irods4j.low_level.protocol.packing_instructions.MsgHeader_PI;

public class Network {

	private static final Logger log = LogManager.getLogger();

	public static void write(Socket socket, MsgHeader_PI msgHeader) throws IOException {
		var msg = XmlUtil.toXmlString(msgHeader);

		var bbuf = ByteBuffer.allocate(4);
		bbuf.order(ByteOrder.BIG_ENDIAN);
		bbuf.putInt(msg.length());

		var out = socket.getOutputStream();
		out.write(bbuf.array());
		out.write(msg.getBytes());
		out.flush();

		if (log.isDebugEnabled()) {
			log.debug("Wrote {} bytes", msg.length());
			log.debug("Message:\n{}", msg);
		}
	}

	public static <T> void writeXml(Socket socket, T object) throws IOException {
		var msg = XmlUtil.toXmlString(object);
		var out = socket.getOutputStream();
		out.write(msg.getBytes());
		out.flush();
		log.debug("Wrote:\n{}", msg);
	}

	public static <T> void writeJson(Socket socket, T object) throws IOException {
		var msg = JsonUtil.toJsonString(object);
		var out = socket.getOutputStream();
		out.write(msg.getBytes());
		out.flush();
		log.debug("Wrote:\n{}", msg);
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
		if (log.isDebugEnabled()) {
			log.debug("Received:\n{}", new String(msgHeaderBytes, StandardCharsets.UTF_8));
		}
		return XmlUtil.fromBytes(msgHeaderBytes, MsgHeader_PI.class);
	}

	public static <T> T readObject(Socket socket, int size, Class<T> clazz) throws IOException {
		var in = socket.getInputStream();
		var bytes = in.readNBytes(size);
		if (log.isDebugEnabled()) {
			log.debug("Received:\n{}", new String(bytes, StandardCharsets.UTF_8));
		}
		return XmlUtil.fromBytes(bytes, clazz);
	}

	public static byte[] readBytes(Socket socket, int size) throws IOException {
		var bytes = socket.getInputStream().readNBytes(size);
		if (log.isDebugEnabled()) {
			log.debug("Received:\n{}", new String(bytes, StandardCharsets.UTF_8));
		}
		return bytes;
	}

	public static void writeBytes(Socket socket, byte[] bytes) throws IOException {
		var out = socket.getOutputStream();
		out.write(bytes);
		out.flush();
	}

}
