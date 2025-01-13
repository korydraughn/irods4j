package org.irods.irods4j.low_level.network;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
		log.debug("Wrote {}", msg);
	}

	public static <T> void writeJson(Socket socket, T object) throws IOException {
		var msg = JsonUtil.toJsonString(object);
		var out = socket.getOutputStream();
		out.write(msg.getBytes());
		out.flush();
		log.debug("Wrote {}", msg);
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
		return XmlUtil.fromBytes(msgHeaderBytes, MsgHeader_PI.class);
	}

	public static <T> T readObject(Socket socket, int size, Class<T> clazz) throws IOException {
		var in = socket.getInputStream();
		var bytes = in.readNBytes(size);
		return XmlUtil.fromBytes(bytes, clazz);
	}

	public static byte[] readBytes(Socket socket, int size) throws IOException {
		return socket.getInputStream().readNBytes(size);
	}

	public static void writeBytes(Socket socket, byte[] bytes) throws IOException {
		socket.getOutputStream().write(bytes);
	}

}
