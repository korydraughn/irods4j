package org.irods.irods4j.high_level.vfs;

import java.io.IOException;
import java.util.Iterator;
import java.util.Stack;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.api.IRODSException;
import org.irods.irods4j.high_level.vfs.IRODSCollectionIterator.CollectionOptions;

/**
 * A class which makes it easy to iterate over the contents of a collection,
 * including subcollections.
 * 
 * @since 0.1.0
 */
public class IRODSRecursiveCollectionIterator implements Iterable<CollectionEntry> {

	private static final Logger log = LogManager.getLogger();

	private static final int DEFAULT_NUMBER_OF_ROWS_PER_PAGE = 512;

	private RcComm comm;
	private Stack<IRODSCollectionIterator> stack;
	private IRODSCollectionIterator.CollectionEntryIterator curIter;
	private CollectionOptions options;
	private boolean recurse = true;

	/**
	 * Initializes a newly created recursive iterator such that all entries within
	 * the collection and its subcollections will be visited.
	 * 
	 * @param comm        A connection to the iRODS server.
	 * @param logicalPath The absolute path to a collection.
	 * 
	 * @throws IRODSException
	 * @throws IOException
	 * @throws IRODSFilesystemException
	 * 
	 * @since 0.1.0
	 */
	public IRODSRecursiveCollectionIterator(RcComm comm, String logicalPath)
			throws IRODSFilesystemException, IOException, IRODSException {
		this(comm, logicalPath, CollectionOptions.NONE);
	}

	/**
	 * Initializes a newly created recursive iterator.
	 * 
	 * @param comm        A connection to the iRODS server.
	 * @param logicalPath The absolute path to a collection.
	 * @param rowsPerPage The max number of rows to fetch when a new page of data is
	 *                    needed.
	 * 
	 * @throws IRODSException
	 * @throws IOException
	 * @throws IRODSFilesystemException
	 * 
	 * @since 0.1.0
	 */
	public IRODSRecursiveCollectionIterator(RcComm comm, String logicalPath, int rowsPerPage)
			throws IRODSFilesystemException, IOException, IRODSException {
		this(comm, logicalPath, rowsPerPage, CollectionOptions.NONE);
	}

	/**
	 * Initializes a newly created recursive iterator.
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
	public IRODSRecursiveCollectionIterator(RcComm comm, String logicalPath, CollectionOptions options)
			throws IRODSFilesystemException, IOException, IRODSException {
		this(comm, logicalPath, DEFAULT_NUMBER_OF_ROWS_PER_PAGE, options);
	}

	/**
	 * Initializes a newly created recursive iterator.
	 * 
	 * @param comm        A connection to the iRODS server.
	 * @param logicalPath The absolute path to a collection.
	 * @param rowsPerPage The max number of rows to fetch when a new page of data is
	 *                    needed.
	 * @param options     Options affecting the behavior of the iterator. Currently
	 *                    unused.
	 * 
	 * @throws IRODSException
	 * @throws IOException
	 * @throws IRODSFilesystemException
	 * 
	 * @since 0.1.0
	 */
	public IRODSRecursiveCollectionIterator(RcComm comm, String logicalPath, int rowsPerPage, CollectionOptions options)
			throws IRODSFilesystemException, IOException, IRODSException {
		var iter = new IRODSCollectionIterator(comm, logicalPath, rowsPerPage, options);
		curIter = (IRODSCollectionIterator.CollectionEntryIterator) iter.iterator();
		stack = new Stack<>();
		stack.push(iter);

		this.comm = comm;
		this.options = options;
	}

	/**
	 * Returns the collection options used to construct the iterator.
	 * 
	 * @since 0.1.0
	 */
	public CollectionOptions getCollectionOptions() {
		return options;
	}

	/**
	 * Returns the recursion depth from the starting collection.
	 * 
	 * The depth is 0-indexed.
	 * 
	 * @since 0.1.0
	 */
	public int getDepth() {
		return stack.size() - 1;
	}

	/**
	 * Checks whether recursion is enabled.
	 * 
	 * @since 0.1.0
	 */
	public boolean recursionPending() {
		return recurse;
	}

	/**
	 * Moves the current position of the iterator up a collection.
	 * 
	 * @since 0.1.0
	 */
	public void pop() {
		if (stack.isEmpty()) {
			return;
		}
		stack.pop();
		curIter = stack.isEmpty() ? null : (IRODSCollectionIterator.CollectionEntryIterator) stack.peek().iterator();
	}

	/**
	 * Temporarily disables recursion for the current iterator.
	 * 
	 * Recursion is re-enabled after the iterator moves forward.
	 * 
	 * @since 0.1.0
	 */
	public void disableRecursionPending() {
		recurse = false;
	}

	@Override
	public Iterator<CollectionEntry> iterator() {
		return new RecursiveCollectionEntryIterator(this);
	}

	/**
	 * The class providing the iterator implementation over a collection.
	 * 
	 * @since 0.1.0
	 */
	public static final class RecursiveCollectionEntryIterator implements Iterator<CollectionEntry> {

		private IRODSRecursiveCollectionIterator iter;

		private RecursiveCollectionEntryIterator(IRODSRecursiveCollectionIterator iter) {
			this.iter = iter;
		}

		@Override
		public boolean hasNext() {
			// Find the next iterator in the stack which has entries.
			while (true) {
				if (null == iter.curIter) {
					return false;
				}

				if (iter.curIter.hasNext()) {
					return true;
				}

				// The iterator at the top of the stack has visited all entries.
				// Remove it from the stack.
				try {
					iter.stack.pop();
				} catch (Exception e) {
					log.error(e.getMessage());
				}

				if (iter.stack.isEmpty()) {
					iter.curIter = null;
					return false;
				}

				// The stack isn't empty, so setup the iterator state to use
				// the iterator at the top of the stack.
				iter.curIter = (IRODSCollectionIterator.CollectionEntryIterator) iter.stack.peek().iterator();
			}
		}

		@Override
		public CollectionEntry next() {
			var addedNewCollection = false;
			var entry = iter.curIter.next();

			if (entry.isCollection() && iter.recurse) {
				IRODSCollectionIterator tmpIter = null;
				try {
					tmpIter = new IRODSCollectionIterator(iter.comm, entry.path, iter.stack.peek().getRowsPerPage(),
							iter.options);
				} catch (IOException | IRODSException e) {
					log.error(e.getMessage());
				}

				if (null != tmpIter) {
					var iterator = tmpIter.iterator();

					// Only add the collection if it contains entries.
					if (iterator.hasNext()) {
						addedNewCollection = true;
						iter.stack.push(tmpIter);
						iter.curIter = (IRODSCollectionIterator.CollectionEntryIterator) iterator;
						entry = iterator.next();
					}
				}
			}

			if (!addedNewCollection) {
				// TODO This block is probably not needed.
			}

			// Iterating forward must always reset the recurse flag. This mirrors the
			// behavior of the C++ implementation.
			iter.recurse = true;

			return entry;
		}

	}

}
