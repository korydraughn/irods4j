package org.irods.irods4j.high_level.vfs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.api.IRODSException;
import org.irods.irods4j.high_level.catalog.IRODSQuery;
import org.irods.irods4j.high_level.vfs.ObjectStatus.ObjectType;

/**
 * A class which makes it easy to iterate over the contents of a collection.
 * 
 * Instances of this class do not visit subcollections.
 * 
 * @since 0.1.0
 */
public class IRODSCollectionIterator implements Iterable<CollectionEntry> {

	private static final Logger log = LogManager.getLogger();

	private RcComm comm;
	private String logicalPath;

	@SuppressWarnings("unused")
	private CollectionOptions collOptions;

	// Holds a reference to the current query results.
	private List<List<String>> rows;

	// The index of a row within the query results.
	private int rowIndex = 0;

	// Instructs the iterator to include a condition which allows the query to
	// return the next set of rows.
	private boolean addPivotCondition = false;

	// Instructs the iterator to query for collections. See the iterator
	// implementation for details on when this gets set to true.
	private boolean searchForCollections = false;

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
	 * @throws IRODSException
	 * @throws IOException
	 * @throws IRODSFilesystemException
	 * 
	 * @throws IllegalArgumentException If invalid inputs are passed.
	 * 
	 * @since 0.1.0
	 */
	public IRODSCollectionIterator(RcComm comm, String logicalPath)
			throws IRODSFilesystemException, IOException, IRODSException {
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
	 * @throws IRODSException
	 * @throws IOException
	 * @throws IRODSFilesystemException
	 * 
	 * @since 0.1.0
	 */
	public IRODSCollectionIterator(RcComm comm, String logicalPath, CollectionOptions options)
			throws IRODSFilesystemException, IOException, IRODSException {
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

	@Override
	public Iterator<CollectionEntry> iterator() {
		return new CollectionEntryIterator(this);
	}

	/**
	 * The class providing the iterator implementation over a collection.
	 * 
	 * @since 0.1.0
	 */
	public static final class CollectionEntryIterator implements Iterator<CollectionEntry> {

		private static final String collQueryFmtStr = "select COLL_ID, COLL_NAME, COLL_CREATE_TIME, COLL_MODIFY_TIME where COLL_PARENT_NAME = '%s'";
		private static final String dataObjectQueryFmtStr = "select DATA_ID, DATA_NAME, DATA_SIZE, DATA_CHECKSUM, DATA_MODE, DATA_CREATE_TIME, DATA_MODIFY_TIME where COLL_NAME = '%s'";

		private IRODSCollectionIterator iter;

		private CollectionEntryIterator(IRODSCollectionIterator iter) {
			this.iter = iter;

			// Return immediately if the iterator has been previously constructed.
			if (null != iter.rows) {
				return;
			}

			iter.rows = null;
			iter.rowIndex = 0;
			iter.addPivotCondition = false;
			iter.searchForCollections = false;
		}

		@Override
		public boolean hasNext() {
			// We're working with an existing set of rows.
			if (null != iter.rows) {
				if (++iter.rowIndex < iter.rows.size()) {
					return true;
				}

				// At this point, we know we need the next page of data.
				iter.addPivotCondition = true;

				// Unlike the constructor, the row index must be set to the first row since it
				// will be used to retrieve an entry from the page, if rows exist.
				iter.rowIndex = 0;
			}

			if (!iter.searchForCollections) {
				// TODO Consider building this string one time.
				var query = String.format(dataObjectQueryFmtStr, iter.logicalPath);

				// All rows have been visited within the current page.
				if (iter.addPivotCondition) {
					iter.addPivotCondition = false;

					// Update the query to find the next page of rows. The next page begins after
					// the ID of the last row within the current set of rows.
					query += String.format(" and DATA_ID > '%s' order by DATA_ID limit 256",
							iter.rows.get(iter.rows.size() - 1).get(0));
				} else {
					query += " order by DATA_ID limit 256";
				}

				try {
					iter.rows = IRODSQuery.executeGenQuery2(iter.comm, iter.comm.proxyUserZone, query);
					if (!iter.rows.isEmpty()) {
						return true;
					}

					iter.searchForCollections = true;
					iter.rowIndex = 0;
				} catch (IOException | IRODSException e) {
					log.error(e.getMessage());
				}
			}

			if (iter.searchForCollections) {
				// TODO Consider building this string one time.
				var query = String.format(collQueryFmtStr, iter.logicalPath);

				// All rows have been visited within the current page.
				if (iter.addPivotCondition) {
					iter.addPivotCondition = false;

					// Update the query to find the next page of rows. The next page begins after
					// the ID of the last row within the current set of rows.
					query += String.format(" and COLL_ID > '%s' order by COLL_ID limit 256",
							iter.rows.get(iter.rows.size() - 1).get(0));
				} else {
					query += " order by COLL_ID limit 256";
				}

				try {
					iter.rows = IRODSQuery.executeGenQuery2(iter.comm, iter.comm.proxyUserZone, query);
					// Terminate because collections are processed after data objects.
					return !iter.rows.isEmpty();
				} catch (IOException | IRODSException e) {
					log.error(e.getMessage());
				}
			}

			return false;
		}

		@Override
		public CollectionEntry next() {
			var row = iter.rows.get(iter.rowIndex);
			var e = new CollectionEntry();

			if (iter.searchForCollections) {
				e.dataId = row.get(0);
				e.path = row.get(1);
				e.ctime = Long.parseLong(row.get(2));
				e.mtime = Long.parseLong(row.get(3));
				e.status = new ObjectStatus();
				e.status.setType(ObjectType.COLLECTION);

				return e;
			}

			e.dataId = row.get(0);
			e.path = Paths.get(iter.logicalPath, row.get(1)).toString();
			e.dataSize = Long.parseLong(row.get(2));
			e.checksum = row.get(3);
			e.dataMode = Integer.parseInt(row.get(4));
			e.ctime = Long.parseLong(row.get(5));
			e.mtime = Long.parseLong(row.get(6));
			e.status = new ObjectStatus();
			e.status.setType(ObjectType.DATA_OBJECT);

			return e;
		}

	}

}
