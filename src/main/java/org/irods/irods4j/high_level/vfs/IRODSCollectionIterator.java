package org.irods.irods4j.high_level.vfs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.api.IRODSException;
import org.irods.irods4j.common.Reference;
import org.irods.irods4j.high_level.vfs.ObjectStatus.ObjectType;
import org.irods.irods4j.low_level.protocol.packing_instructions.CollEnt_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.CollInpNew_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.KeyValPair_PI;

/**
 * A class which makes it easy to iterate over the contents of a collection.
 * 
 * Instances of this class do not visit subcollections.
 * 
 * @since 0.1.0
 */
public class IRODSCollectionIterator implements Iterable<CollectionEntry>, AutoCloseable {

	private static final Logger log = LogManager.getLogger();

	private RcComm comm;
	private String logicalPath;

	@SuppressWarnings("unused")
	private CollectionOptions collOptions;

	// 0 is a valid handle, so we use -1 to avoid issues with checks specific to the
	// handle.
	private int collHandle = -1;
	private Reference<CollEnt_PI> collEntry;

	/**
	 * Options which affect the behavior of the iterator.
	 * 
	 * @since 0.1.0
	 */
	public static enum CollectionOptions {
		NONE, SKIP_PERMISSION_DENIED
	}

	/**
	 * Initializes a newly created iterator such that all entries within the
	 * collection will be visited.
	 * 
	 * @param comm        A connection to the iRODS server.
	 * @param logicalPath The absolute path to a collection.
	 * 
	 * @throws IllegalArgumentException If invalid inputs are passed.
	 * 
	 * @since 0.1.0
	 */
	public IRODSCollectionIterator(RcComm comm, String logicalPath) {
		this(comm, logicalPath, CollectionOptions.NONE);
	}

	/**
	 * Initializes a newly created iterator.
	 * 
	 * @param comm        A connection to the iRODS server.
	 * @param logicalPath The absolute path to a collection.
	 * @param options     Options affecting the behavior of the iterator. Currently
	 *                    unused.
	 * 
	 * @since 0.1.0
	 */
	public IRODSCollectionIterator(RcComm comm, String logicalPath, CollectionOptions options) {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == logicalPath || logicalPath.isEmpty()) {
			throw new IllegalArgumentException("Logical path is null or empty");
		}

		if (null == options) {
			throw new IllegalArgumentException("Collection options is null");
		}

		this.comm = comm;
		this.logicalPath = logicalPath;
		collOptions = options;
	}

	/**
	 * Do not use directly.
	 */
	@Override
	public Iterator<CollectionEntry> iterator() {
		try {
			return new CollectionEntryIterator(this);
		} catch (IOException e) {
			log.error(e.getMessage());
			return null;
		} catch (IRODSException e) {
			log.error(e.getMessage());
			return null;
		}
	}

	/**
	 * Do not use directly.
	 */
	@Override
	public void close() throws Exception {
		if (collHandle >= 0) {
			var ec = IRODSApi.rcCloseCollection(comm, collHandle);
			log.debug("rcCloseCollection returned [{}].", ec);
			collHandle = -1;
		}
	}

	/**
	 * Do not use directly.
	 */
	public static final class CollectionEntryIterator implements Iterator<CollectionEntry> {

		private IRODSCollectionIterator iter;

		private CollectionEntryIterator(IRODSCollectionIterator iter) throws IOException, IRODSException {
			this.iter = iter;

			// The iterator is already open, so there's nothing to do. The iterator will
			// share the same state as sibling iterators derived from the
			// IRODSCollectionIterator instance.
			if (iter.collHandle >= 0) {
				log.debug("Collection handle [{}] for iterator is already open.", iter.collHandle);
				return;
			}

			var input = new CollInpNew_PI();
			input.collName = iter.logicalPath;
			input.flags = 0;
			input.KeyValPair_PI = new KeyValPair_PI();
			input.KeyValPair_PI.ssLen = 0;

			var ec = IRODSApi.rcOpenCollection(iter.comm, input);
			log.debug("rcOpenCollection returned [{}].", ec);
			if (ec < 0) {
				throw new IRODSException(ec, "rcOpenCollection error");
			}

			iter.collHandle = ec;
			iter.collEntry = new Reference<>();
		}

		@Override
		public boolean hasNext() {
			try {
				// TODO Replace this with the rcl* versions.
				var ec = IRODSApi.rcReadCollection(iter.comm, iter.collHandle, iter.collEntry);
				log.debug("rcReadCollection returned [{}].", ec);
				return ec >= 0;
			} catch (Exception e) {
				log.error(e.getMessage());
				return false;
			}
		}

		@Override
		public CollectionEntry next() {
			var e = new CollectionEntry();

			e.dataMode = iter.collEntry.value.dataMode;
			e.dataSize = iter.collEntry.value.dataSize;
			e.dataId = iter.collEntry.value.dataId;

			if (null != iter.collEntry.value.createTime) {
				e.ctime = Long.parseLong(iter.collEntry.value.createTime);
			}

			if (null != iter.collEntry.value.modifyTime) {
				e.mtime = Long.parseLong(iter.collEntry.value.modifyTime);
			}

			e.checksum = iter.collEntry.value.chksum;
			e.owner = iter.collEntry.value.ownerName;
			e.dataType = iter.collEntry.value.dataType;
			e.status = new ObjectStatus();

			switch (iter.collEntry.value.objType) {
			case 1: // DATA_OBJ_T
				e.status.setType(ObjectType.DATA_OBJECT);
				e.path = Paths.get(iter.logicalPath, iter.collEntry.value.dataName).toString();
				break;
			case 2: // COLL_OBJ_T
				e.status.setType(ObjectType.COLLECTION);
				e.path = iter.collEntry.value.collName;
				break;
			default:
				e.status.setType(ObjectType.NONE);
				break;
			}

			return e;
		}

	}

}
