package org.irods.irods4j.high_level.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;

import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.api.IRODSException;
import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.common.Reference;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.FileLseekOut_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.KeyValPair_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.OpenedDataObjInp_PI;

/**
 * 
 */
public class DataObjectStream implements Closeable {

	private RcComm comm;
	private int fd = -1;
	private int replicaNumber = -1;
	private String replicaToken;

	/**
	 * 
	 */
	public static class OnCloseSuccess {
		public boolean updateSize = true;
		public boolean updateStatus = true;
		public boolean computeChecksum;
		public boolean sendNotifications;
		public boolean preserveReplicaStateTable;
	}

	/**
	 * 
	 */
	public static enum SeekDirection {
		BEGIN, CURRENT, END
	}

	/**
	 * 
	 * @param comm
	 */
	public DataObjectStream(RcComm comm) {
		this.comm = comm;
	}

	/**
	 * 
	 * @param logicalPath
	 * @param openMode
	 * @throws IOException
	 * @throws IRODSException
	 */
	public void open(String logicalPath, int openMode) throws IOException, IRODSException {
		if (null == logicalPath || logicalPath.isEmpty()) {
			throw new IllegalArgumentException("Logical path is null or empty");
		}

		var input = new DataObjInp_PI();
		input.objPath = logicalPath;
		input.dataSize = -1;
		input.createMode = 0600;
		input.openFlags = openMode;
		input.KeyValPair_PI = new KeyValPair_PI();
		input.KeyValPair_PI.ssLen = 0;

		var output = new Reference<String>();

		var ec = IRODSApi.rcReplicaOpen(comm, input, output);
		if (ec < 0) {
			throw new IRODSException(ec, "Open error: " + logicalPath);
		}

		fd = ec;

		// TODO Parse JSON output and capture the following:
		// - replica number
		// - replica token
	}

	/**
	 * 
	 * @param logicalPath
	 * @param replicaNumber
	 * @param openMode
	 */
	public void open(String logicalPath, int replicaNumber, int openMode) {
		// TODO
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * 
	 * @param logicalPath
	 * @param rootResource
	 * @param openMode
	 */
	public void open(String logicalPath, String rootResource, int openMode) {
		// TODO
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * 
	 * @param replicaToken
	 * @param logicalPath
	 * @param replicaNumber
	 * @param openMode
	 */
	public void open(String replicaToken, String logicalPath, int replicaNumber, int openMode) {
		// TODO
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * 
	 * @param replicaToken
	 * @param logicalPath
	 * @param rootResource
	 * @param openMode
	 */
	public void open(String replicaToken, String logicalPath, String rootResource, int openMode) {
		// TODO
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * 
	 */
	@Override
	public void close() throws IOException {
		close(null);
	}

	/**
	 * 
	 * @param onCloseSuccess
	 * @throws IOException
	 */
	public void close(OnCloseSuccess onCloseSuccess) throws IOException {
		if (-1 == fd) {
			return;
		}

		var input = new HashMap<String, Object>();
		input.put("fd", fd);

		if (null != onCloseSuccess) {
			input.put("update_size", onCloseSuccess.updateSize);
			input.put("update_status", onCloseSuccess.updateStatus);
			input.put("compute_checksum", onCloseSuccess.computeChecksum);
			input.put("send_notifications", onCloseSuccess.sendNotifications);
			input.put("preserve_replica_state_table", onCloseSuccess.preserveReplicaStateTable);
		}

		var ec = IRODSApi.rcReplicaClose(comm, JsonUtil.toJsonString(input));
		if (ec < 0) {
			// TODO Log error and keep going?
		}

		fd = -1;
	}

	/**
	 * 
	 * @param offset
	 * @param direction
	 * @return
	 * @throws IOException
	 * @throws IRODSException
	 */
	public long seek(int offset, SeekDirection direction) throws IOException, IRODSException {
		throwIfInvalidL1Descriptor(fd);

		var input = new OpenedDataObjInp_PI();
		input.l1descInx = fd;
		input.offset = offset;

		switch (direction) {
		case BEGIN:
			input.whence = 0; // SEEK_SET
			break;

		case CURRENT:
			input.whence = 1; // SEEK_CUR
			break;

		case END:
			input.whence = 2; // SEEK_END
			break;
		}

		var output = new Reference<FileLseekOut_PI>();

		var ec = IRODSApi.rcDataObjLseek(comm, input, output);
		if (ec < 0) {
			throw new IRODSException(ec, "Seek error");
		}

		return output.value.offset;
	}

	/**
	 * 
	 * @param buffer
	 * @param count
	 * @return
	 * @throws IOException
	 * @throws IRODSException
	 */
	public int read(byte[] buffer, int count) throws IOException, IRODSException {
		throwIfInvalidL1Descriptor(fd);
		throwIfInvalidBufferSize(buffer.length, count);

		var input = new OpenedDataObjInp_PI();
		input.l1descInx = fd;
		input.len = count;

		var bytesRead = IRODSApi.rcDataObjRead(comm, input, buffer);
		if (bytesRead < 0) {
			throw new IRODSException(bytesRead, "Read error");
		}

		return bytesRead;
	}

	/**
	 * 
	 * @param buffer
	 * @param count
	 * @return
	 * @throws IOException
	 * @throws IRODSException
	 */
	public int write(byte[] buffer, int count) throws IOException, IRODSException {
		throwIfInvalidL1Descriptor(fd);
		throwIfInvalidBufferSize(buffer.length, count);

		var input = new OpenedDataObjInp_PI();
		input.l1descInx = fd;
		input.len = count;
		input.KeyValPair_PI = new KeyValPair_PI();
		input.KeyValPair_PI.ssLen = 0;

		var bytesWritten = IRODSApi.rcDataObjWrite(comm, input, buffer);
		if (bytesWritten < 0) {
			throw new IRODSException(bytesWritten, "Write error");
		}

		return bytesWritten;
	}

	/**
	 * 
	 * @return
	 */
	public boolean isOpen() {
		return fd >= 3;
	}

	/**
	 * 
	 * @return
	 */
	public int getNativeHandle() {
		throwIfInvalidL1Descriptor(fd);
		return fd;
	}

	/**
	 * 
	 * @return
	 */
	public int getReplicaNumber() {
		throwIfInvalidL1Descriptor(fd);
		return replicaNumber;
	}

	/**
	 * 
	 * @return
	 */
	public String getReplicaToken() {
		throwIfInvalidL1Descriptor(fd);
		return replicaToken;
	}

	private static void throwIfInvalidL1Descriptor(int fd) {
		if (-1 == fd) {
			throw new IllegalStateException("Stream not open");
		}
	}

	private static void throwIfInvalidBufferSize(int bufferSize, int count) {
		if (count < 0 || count > bufferSize) {
			throw new IllegalArgumentException("Byte count is less than 0 or exceeds buffer size");
		}
	}

}
