package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.collections.PrefixList;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.security.User;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.MountPairInfo;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.Subject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryNotEmptyException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Created by guoyejun on 2017/5/10.
 * This Class is for HDFS Unified namespace client proxy; it can proxy the request to HDFS, Alluxio or Both according
 * to MODE(ROUTE_MODE & CACHE_MODE) and UserMustCacheList;
 * Use case: All path is mount from the UFS except root;
 */
abstract class AbstractFileSystemProxy extends org.apache.hadoop.fs.FileSystem {
	private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
	// Always tell Hadoop that we have 3x replication.
	private static final int BLOCK_REPLICATION_CONSTANT = 3;
	/**
	 * Lock for initializing the contexts, currently only one set of contexts is supported.
	 */
	private static final Object INIT_LOCK = new Object();

	//private boolean MODE_ROUTE_ENABLED;
	private boolean MODE_CACHE_ENABLED;

	private HashMap<String, org.apache.hadoop.fs.FileSystem> hdfsFileSystemCache =
			new HashMap<>();
	private List<MountPairInfo> mMountPonitList = null;

	/**
	 * Flag for if the contexts have been initialized.
	 */
	@GuardedBy("INIT_LOCK")
	private static volatile boolean sInitialized = false;
	private FileSystemContext mContext = null;
	private alluxio.client.file.FileSystem mFileSystem = null;
	private URI mUri = null;
	private Path mWorkingDir = new Path(AlluxioURI.SEPARATOR);
	private Statistics mStatistics = null;
	private String mAlluxioHeader = null;
	private PrefixList mUserMustCacheList = null;
	private boolean mUserClientCacheEnabled;
	private org.apache.hadoop.conf.Configuration conf = null;

	AbstractFileSystemProxy() {
	}

	@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
	AbstractFileSystemProxy(FileSystem fileSystem) {
		mFileSystem = fileSystem;
		sInitialized = true;
	}

	public FSDataOutputStream appendv2(Path path, int bufferSize, Progressable progress)
			throws IOException {
		LOG.debug("appendV2({}, {}, {})", path, bufferSize, progress);

		if (mStatistics != null) {
			mStatistics.incrementBytesWritten(1);
		}

		String mPath = HadoopUtils.getPathWithoutScheme(path);
		if (MODE_CACHE_ENABLED && isInMustCacheList(mPath)) {
			LOG.error("Alluxio Space DoesNotSupport: Append({}) in UserMustCacheList({})",
					path, mUserMustCacheList);
			throw new UnsupportedOperationException();
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			return hdfsUfsInfo.getHdfsUfs().append(hdfsUfsInfo.getHdfsPath(), bufferSize, progress);
		} catch(IOException e) {
			LOG.error("Append Failed in HDFS Space ({})", path);
			throw e;
		}
	}

	@Override
	public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
			throws IOException {
		LOG.debug("append({}, {}, {})", path, bufferSize, progress);
		if (mStatistics != null) {
			mStatistics.incrementBytesWritten(1);
		}
		String mPath = HadoopUtils.getPathWithoutScheme(path);

		if (MODE_CACHE_ENABLED) {
			AlluxioURI mUri = new AlluxioURI(mPath);
			boolean isExistsInAlluxio = isExistsInAlluxio(mUri);
			if (isInMustCacheList(mPath)){
				LOG.error("Alluxio Space DoesNotSupport: Append({}) in UserMustCacheList({})",
						path, mUserMustCacheList);
				throw new UnsupportedOperationException();
			}

			if(isExistsInAlluxio){
				try {
					mFileSystem.delete(mUri);
				} catch (AlluxioException e) {
					LOG.error("Delete Failed in Alluxio Space ({})", path);
					throw new RuntimeException(e);
				}
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			return hdfsUfsInfo.getHdfsUfs().append(hdfsUfsInfo.getHdfsPath(), bufferSize, progress);
		}catch(IOException e){
			LOG.error("Append Failed in HDFS Space ({})", path);
			throw e;
		}
	}

	@Override
	public void close() throws IOException {
		LOG.info("Close FileSystem ({})", this.toString());
		if (mContext != FileSystemContext.INSTANCE) {
			mContext.close();
		}
		super.close();
	}

	@Override
	public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite,
			int bufferSize, short replication, long blockSize, Progressable progress)
			throws IOException {
		LOG.debug("create({}, {}, {}, {}, {}, {}, {})", path, permission,
				overwrite, bufferSize, replication,
				blockSize, progress);
		if (mStatistics != null) {
			mStatistics.incrementBytesWritten(1);
		}

		if (MODE_CACHE_ENABLED) {
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(mPath);
			boolean isExistsInAlluxio = isExistsInAlluxio(mUri);
			try {
				if (isExistsInAlluxio) {
					if (!overwrite) {
						throw new IOException(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(path));
					}
					if (mFileSystem.getStatus(mUri).isFolder()) {
						throw new IOException(ExceptionMessage.FILE_CREATE_IS_DIRECTORY.getMessage(path));
					}
					mFileSystem.delete(mUri);
				}
				if(isInMustCacheList(mPath)) {
					CreateFileOptions options = CreateFileOptions.defaults()
							.setWriteType(WriteType.MUST_CACHE).setMode(new Mode(permission.toShort()));
					return new FSDataOutputStream(mFileSystem.createFile(mUri, options), mStatistics);
				}
			} catch (AlluxioException e1) {
				LOG.error("Create Failed in Alluxio Space ({})", path);
				throw new RuntimeException(e1);
			}
		}

		//if create path is not in userMustCacheList, create through to HDFS directly;
		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			return new FSDataOutputStream(hdfsUfsInfo.getHdfsUfs().create(hdfsUfsInfo.getHdfsPath(),
					permission,overwrite,bufferSize,(short)BLOCK_REPLICATION_CONSTANT,blockSize,progress),
					mStatistics);
		} catch (IOException e) {
			LOG.error("Create Failed in HDFS Space ({})", path);
			throw e;
		}
	}

	@Deprecated
	@Override
	public FSDataOutputStream createNonRecursive(Path path, FsPermission permission,
		boolean overwrite, int bufferSize, short replication,
		long blockSize, Progressable progress) throws IOException {
		return create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
	}

	@Deprecated
	@Override
	public boolean delete(Path path) throws IOException {
		return delete(path, true);
	}

	@Override
	public boolean delete(Path path, boolean recursive) throws IOException {
		LOG.debug("delete({}, {})", path, recursive);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}
		//delete alluxio space data first;
		if (MODE_CACHE_ENABLED) {
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(mPath);
			DeleteOptions options = DeleteOptions.defaults().setRecursive(recursive);
			boolean inUserMustCacheList = isInMustCacheList(mPath);
			try {
				mFileSystem.delete(mUri, options);
				if (inUserMustCacheList) {
					return true;
				}
			} catch (FileDoesNotExistException e){
				if (inUserMustCacheList) {
					LOG.error("FileDoesnotExistException ({})", path);
					throw new FileNotFoundException(mPath);
				}
			} catch (DirectoryNotEmptyException e1){
				LOG.error("Directory is not empty, please set Recursive. Path: {}, isRecursive: {}",
					path, recursive);
				throw e1;
			} catch (AlluxioException e2) {
				LOG.error("Delete Failed in Alluxio Space ({})", path);
				if (inUserMustCacheList) {
					throw new RuntimeException(e2);
				}
			}
		}

		//If path is not in mUserMustCacheList, delete it in HDFS space;
		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		org.apache.hadoop.fs.FileSystem hdfs = hdfsUfsInfo.getHdfsUfs();
		Path hdfsPath = hdfsUfsInfo.getHdfsPath();
		try {
			return ((hdfs.delete(hdfsPath, true)));
		}catch (IOException e){
			LOG.error("Delete Failed in HDFS Space ({})", path);
			throw e;
		}
	}

	@Deprecated
	@Override
	public long getDefaultBlockSize() {
		return Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
	}

	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
			throws IOException {
		LOG.debug("getFileBlockLocations({}, {}, {})", file, start, len);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}

		String mPath = HadoopUtils.getPathWithoutScheme(file.getPath());
		if (MODE_CACHE_ENABLED) {
			AlluxioURI mUri = new AlluxioURI(mPath);
			boolean inUserMustCacheList = isInMustCacheList(mPath);
			try {
				List<FileBlockInfo> blocks = mFileSystem.getStatus(mUri).getFileBlockInfos();
				List<BlockLocation> blockLocations = new ArrayList<>();
				for (FileBlockInfo fileBlockInfo : blocks) {
					long offset = fileBlockInfo.getOffset();
					long end = offset + fileBlockInfo.getBlockInfo().getLength();
					// Check if there is any overlapping between [start, start+len] and [offset, end]
					if (end >= start && offset <= start + len) {
						ArrayList<String> names = new ArrayList<>();
						ArrayList<String> hosts = new ArrayList<>();
						// add the existing in-memory block locations
						for (alluxio.wire.BlockLocation location : fileBlockInfo.getBlockInfo().getLocations()) {
							HostAndPort address = HostAndPort.fromParts(location.getWorkerAddress().getHost(),
								location.getWorkerAddress().getDataPort());
							names.add(address.toString());
							hosts.add(address.getHostText());
						}
						blockLocations.add(new BlockLocation(CommonUtils.toStringArray(names),
							CommonUtils.toStringArray(hosts), offset, fileBlockInfo.getBlockInfo().getLength()));
					}
				}
				BlockLocation[] ret = new BlockLocation[blockLocations.size()];
				blockLocations.toArray(ret);
				return ret;
			} catch (FileDoesNotExistException e) {
				if (inUserMustCacheList) {
					LOG.error("FileDostNotExistException ({})", file);
					throw new FileNotFoundException(file.getPath().toString());
				}
			} catch (AlluxioException e1) {
				if (inUserMustCacheList) {
					LOG.error("getFileBlockLocations Faild in Alluxio Space ({})", file);
					throw new RuntimeException(e1);
				}
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(file.getPath());
		try {
			return hdfsUfsInfo.getHdfsUfs().getFileBlockLocations(hdfsUfsInfo.getHdfsPath(), start, len);
		}catch (IOException e){
			LOG.error("getFileBlockLocations Failed In HDFS Space ({})", mPath);
			throw e;
		}
	}

	@Override
	public FileStatus getFileStatus(Path path) throws IOException {
		LOG.debug("getFileStatus({})", path);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}
		if (MODE_CACHE_ENABLED) {
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
			boolean inMustCacheList = isInMustCacheList(mPath);
		//	if (inMustCacheList) {
				try {
					URIStatus fileStatus = mFileSystem.getStatus(mUri);
					return new FileStatus(fileStatus.getLength(), fileStatus.isFolder(), BLOCK_REPLICATION_CONSTANT,
						fileStatus.getBlockSizeBytes(), fileStatus.getLastModificationTimeMs(), fileStatus.getCreationTimeMs(),
						new FsPermission((short) fileStatus.getMode()), fileStatus.getOwner(), fileStatus.getGroup(),
						new Path(mAlluxioHeader + mUri));
				} catch (FileDoesNotExistException e) {
					if (inMustCacheList) {
						throw new FileNotFoundException(e.getMessage());
					}
				} catch (AlluxioException e1) {
					LOG.error("getFileStatus Failed in Alluxio Space ({})", path);
					throw new RuntimeException(e1);
				}
		//	}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			FileStatus fileStatus = hdfsUfsInfo.getHdfsUfs().getFileStatus(hdfsUfsInfo.getHdfsPath());
			String alluxioPath = mAlluxioHeader + path.toUri().getPath().concat(
				fileStatus.getPath().toString().substring(hdfsUfsInfo.getHdfsPath().toString().length()));
			fileStatus.setPath(new Path(alluxioPath));
			return fileStatus;
		} catch (FileNotFoundException e){
			throw e;
		} catch (IOException e1){
			LOG.error("getFileStatus Failed in HDFS Space ({})", path);
			throw e1;
		}
	}

	/**
	 * setOwner and group for specified path.
	 * set in Alluxio Space first if exists, and then to set in hdfs Space,
	 * if set failed in UFS after set succeed in alluxio space before, should roll back and throw IOException;
	 * @param path path to set
	 * @param username username to set
	 * @param groupname groupname to set
	 * @throws IOException
	 */
	@Override
	public void setOwner(Path path, final String username, final String groupname)
			throws IOException {
		LOG.debug("setOwner({}, {}, {})", path, username, groupname);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}

		URIStatus fileStatus = null;
		String mPath = HadoopUtils.getPathWithoutScheme(path);
		AlluxioURI mUri = new AlluxioURI(mPath);
		if (MODE_CACHE_ENABLED) {
			boolean isInUserCacheMustList = isInMustCacheList(mPath);
			SetAttributeOptions options = SetAttributeOptions.defaults();
			boolean ownerOrGroupChgd = false;
			if (username != null && !username.isEmpty()) {
				options.setOwner(username).setRecursive(false);
				ownerOrGroupChgd = true;
			}
			if (groupname != null && !groupname.isEmpty()) {
				options.setGroup(groupname).setRecursive(false);
				ownerOrGroupChgd = true;
			}
			if (ownerOrGroupChgd) {
				try {
					fileStatus = mFileSystem.getStatus(mUri);
					mFileSystem.setAttribute(mUri, options);
					if (isInUserCacheMustList) {
						return ;
					}
				} catch (FileDoesNotExistException e) {
					if (isInUserCacheMustList) {
						LOG.error("FileDoesNotExistException ({})", path);
						throw new FileNotFoundException(e.getMessage());
					}
				} catch (AlluxioException e1) {
					LOG.error("setOwner Failed in Alluxio Space ({})", path);
					throw new RuntimeException(e1);
				}
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			hdfsUfsInfo.getHdfsUfs().setOwner(hdfsUfsInfo.getHdfsPath(), username, groupname);
		} catch (IOException e) {
			SetAttributeOptions options = SetAttributeOptions.defaults().setOwner(fileStatus.getOwner())
					.setGroup(fileStatus.getGroup()).setRecursive(false);
			try {
				mFileSystem.setAttribute(mUri, options);
			} catch (AlluxioException e1) {
				LOG.error("setOwner Failed in HDFS Space(roll back in Alluxio Space {})", path);
				throw new RuntimeException(e1);
			}
			throw e;
		}
	}

	/**
	 * setPermission for specified path.
	 * set in Alluxio Space first if exists, and then to set in hdfs Space,
	 * if set failed in UFS after set succeed in alluxio space before, should roll back and throw IOException;
	 * @param path path to set
	 * @param permission permission to set
	 * @throws IOException
	 */
	@Override
	public void setPermission(Path path, FsPermission permission) throws IOException {
		LOG.debug("setPermission({}, {})", path, permission);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}

		URIStatus fileStatus = null;
		String mPath = HadoopUtils.getPathWithoutScheme(path);
		AlluxioURI mUri = new AlluxioURI(mPath);

		if (MODE_CACHE_ENABLED) {
			boolean isInUserMustCacheList = isInMustCacheList(mPath);
			try {
				SetAttributeOptions options = SetAttributeOptions.defaults()
					.setMode(new Mode(permission.toShort())).setRecursive(false);
				fileStatus = mFileSystem.getStatus(mUri);
				mFileSystem.setAttribute(mUri, options);
				if (isInUserMustCacheList) {
					return;
				}
			} catch (FileDoesNotExistException e) {
				if (isInUserMustCacheList) {
					LOG.error("FileDoseNotExistException({})", path);
					throw new FileNotFoundException(e.getMessage());
				}
			} catch (AlluxioException e1) {
				LOG.error("setOwner Failed in Alluxio Space({})", path);
				throw new IOException(e1);
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			hdfsUfsInfo.getHdfsUfs().setPermission(hdfsUfsInfo.getHdfsPath(), permission);
		} catch (IOException e) {
			SetAttributeOptions options = SetAttributeOptions.defaults().setMode(new Mode((short) fileStatus.getMode()))
					.setRecursive(false);
			try {
				mFileSystem.setAttribute(mUri, options);
			} catch (AlluxioException e1) {
				LOG.error("setPermission Failed in HDFS Space (roll back in Alluxio Space {})", path);
				throw new RuntimeException(e1);
			}
			throw e;
		}
	}

	public abstract String getScheme();

	@Override
	public URI getUri() {
		return mUri;
	}

	@Override
	public Path getWorkingDirectory() {
		LOG.debug("getWorkingDirectory: {}", mWorkingDir);
		return mWorkingDir;
	}


	@Override
	public void setWorkingDirectory(Path path) {
		LOG.debug("setWorkingDirectory({})", path);
		if (path.isAbsolute()) {
			mWorkingDir = path;
		} else {
			mWorkingDir = new Path(mWorkingDir, path);
		}
	}

	@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
	@Override
	public void initialize(URI uri, org.apache.hadoop.conf.Configuration mConf)
			throws IOException {
		Preconditions.checkNotNull(uri.getHost(), PreconditionMessage.URI_HOST_NULL);
		Preconditions.checkNotNull(uri.getPort(), PreconditionMessage.URI_PORT_NULL);

		super.initialize(uri, mConf);
		LOG.debug("initialize({}, {}). Connecting to Alluxio", uri, mConf);

		// Load Alluxio configuration if any and merge to the one in Alluxio file system. These
		// modifications to ClientContext are global, affecting all Alluxio clients in this JVM.
		// We assume here that all clients use the same configuration.
		HadoopUtils.addS3Credentials(mConf);
		HadoopUtils.addSwiftCredentials(mConf);
		ConfUtils.mergeHadoopConfiguration(mConf);

		MODE_CACHE_ENABLED = Configuration.getBoolean(PropertyKey.USER_MODE_CACHE_ENABLED);
		//MODE_ROUTE_ENABLED = Configuration.getBoolean(PropertyKey.USER_MODE_ROUTE_ENABLED);
		mUserClientCacheEnabled = Configuration.getBoolean(PropertyKey.USER_CLIENT_CACHE_ENABLED);

		setConf(mConf);
		mAlluxioHeader = getScheme() + "://" + uri.getHost() + ":" + uri.getPort();
		// Set the statistics member. Use mStatistics instead of the parent class's variable.
		mStatistics = statistics;
		mUri = URI.create(mAlluxioHeader);
		boolean masterAddIsSameAsDefault = checkMasterAddress();
		conf = mConf;
		if (sInitialized && masterAddIsSameAsDefault) {
			updateFileSystemAndContext();
			return;
		}
		synchronized (INIT_LOCK) {
		// If someone has initialized the object since the last check, return
			if (sInitialized) {
				if (masterAddIsSameAsDefault) {
					updateFileSystemAndContext();
					return;
				} else {
					LOG.warn(ExceptionMessage.DIFFERENT_MASTER_ADDRESS
							.getMessage(mUri.getHost() + ":" + mUri.getPort(),
									FileSystemContext.INSTANCE.getMasterAddress()));
					sInitialized = false;
				}
			}
			Configuration.set(PropertyKey.MASTER_HOSTNAME, uri.getHost());
			Configuration.set(PropertyKey.MASTER_RPC_PORT, uri.getPort());
			Configuration.set(PropertyKey.ZOOKEEPER_ENABLED, isZookeeperMode());
			initializeInternal(uri, mConf);
			sInitialized = true;
		}

		updateFileSystemAndContext();
	}

	private void initializeInternal(URI uri, org.apache.hadoop.conf.Configuration conf)
			throws IOException {
		// These must be reset to pick up the change to the master address.
		// TODO(andrew): We should reset key value system in this situation - see ALLUXIO-1706.
		FileSystemContext.INSTANCE.reset();

		// Try to connect to master, if it fails, the provided uri is invalid.
		FileSystemMasterClient client = FileSystemContext.INSTANCE.acquireMasterClient();
		try {
			client.connect();
		// Connected, initialize.
		} catch (ConnectionFailedException | IOException e) {
			LOG.error("Failed to connect to the provided master address {}: {}.",
					uri.toString(), e.toString());
			throw new IOException(e);
		} finally {
			FileSystemContext.INSTANCE.releaseMasterClient(client);
		}
	}

	private void updateFileSystemAndContext() {
		Subject subject = getHadoopSubject();
		if (subject != null) {
			mContext = FileSystemContext.create(subject);
			mFileSystem = FileSystem.Factory.get(mContext);
		} else {
			mContext = FileSystemContext.INSTANCE;
			mFileSystem = FileSystem.Factory.get();
		}
	}

	private boolean checkMasterAddress() {
		InetSocketAddress masterAddress = FileSystemContext.INSTANCE.getMasterAddress();
		boolean sameHost = masterAddress.getHostString().equals(mUri.getHost());
		boolean samePort = masterAddress.getPort() == mUri.getPort();
		if (sameHost && samePort) {
			return true;
		}
		return false;
	}

	private Subject getHadoopSubject() {
		try {
			UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
			String username = ugi.getShortUserName();
			if (username != null && !username.isEmpty()) {
				User user = new User(ugi.getShortUserName());
				HashSet<Principal> principals = new HashSet<>();
				principals.add(user);
				return new Subject(false, principals, new HashSet<>(), new HashSet<>());
			}
			return null;
		} catch (IOException e) {
			return null;
		}
	}

	protected abstract boolean isZookeeperMode();

	/**
	 * List status has two situation,
	 * 1. path just in Alluxio space, get metadata from Alluxio Master;
	 * 2. path both in Alluixo space and hdfs or just in hdfs space only, get metadata from hdfs namenode;
	 * after get the status from hdfs, should transfer to alluxio path uri with
	 * alluxio mountpoint and ufs mountpoint;
	 * @param path path to list status
	 * @return
	 * @throws IOException
	 */
	@Override
	public FileStatus[] listStatus(Path path) throws IOException {
		LOG.debug("listStatus({})", path);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}

		if (MODE_CACHE_ENABLED) {
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(mPath);
			boolean isInUserMustCacheList = isInMustCacheList(mPath);
			if (isInUserMustCacheList) {
				try {
					List<URIStatus> statuses = mFileSystem.listStatus(mUri);
					FileStatus[] ret = new FileStatus[statuses.size()];
					for (int k = 0; k < statuses.size(); k++) {
						URIStatus status = statuses.get(k);

						ret[k] = new FileStatus(status.getLength(), status.isFolder(), BLOCK_REPLICATION_CONSTANT,
							status.getBlockSizeBytes(), status.getLastModificationTimeMs(),
							status.getCreationTimeMs(), new FsPermission((short) status.getMode()), status.getOwner(),
							status.getGroup(), new Path(mAlluxioHeader + status.getPath()));
					}
					return ret;
				} catch (FileDoesNotExistException e) {
					LOG.error("FileDoesNotExistException({})", path);
					throw new FileNotFoundException(e.getMessage());
				} catch(InvalidPathException e1){
					LOG.error("InvaliedPathException({})", path);
					throw new RuntimeException(e1);
				} catch(AlluxioException e2){
					LOG.error("listStatus Failed in Alluxio Space ({})", path);
					throw new RuntimeException(e2);
				}
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		String hdfsMountPoint = hdfsUfsInfo.getHdfsPath().toString();
		try {
			FileStatus[] fileStatus = hdfsUfsInfo.getHdfsUfs().listStatus(new Path(hdfsMountPoint));
			for (int i = 0; i < fileStatus.length; i++) {
				FileStatus fileStatusInfo = fileStatus[i];
				String alluxioPath = path.toString().concat(
					fileStatusInfo.getPath().toString().substring(hdfsUfsInfo.getHdfsPath().toString().length()));
				fileStatusInfo.setPath(new Path(alluxioPath));
				fileStatus[i] = fileStatusInfo;
			}
			return fileStatus;
		}catch (IOException e){
			LOG.error("listStatus Failed in HDFS Space({})", path);
			throw e;
		}
	}

	@Override
	public boolean mkdirs(Path path, FsPermission permission) throws IOException {
		LOG.debug("mkdirs({}, {})", path, permission);
		if (mStatistics != null) {
			mStatistics.incrementBytesWritten(1);
		}

		if (MODE_CACHE_ENABLED) {
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(mPath);
			if (isInMustCacheList(mPath)) {
				try {
					CreateDirectoryOptions options = CreateDirectoryOptions.defaults().setRecursive(true)
							.setAllowExists(true).setMode(new Mode(permission.toShort()))
							.setWriteType(WriteType.MUST_CACHE);
					mFileSystem.createDirectory(mUri, options);
					return true;
				} catch (AlluxioException e) {
					LOG.error("createDirectory Failed in Alluxio Space({}, {})", path, permission);
					throw new RuntimeException(e);
				}
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			return hdfsUfsInfo.getHdfsUfs().mkdirs(hdfsUfsInfo.getHdfsPath(), permission);
		}catch (IOException e){
			LOG.error("mkdirs Failed in HDFS Space({}, {})", path, permission);
			throw e;
		}
	}

	/**
	 * Client open file, if cache mode is enabled, it will check the alluxio space first,
	 * and then through to hdfs space;
	 * @param path path to open
	 * @param bufferSize read bufferSize
	 * @return FSDataInputStream
	 * @throws IOException
	 */
	@Override
	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		LOG.debug("open({}, {})", path, bufferSize);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}

		if (MODE_CACHE_ENABLED) {
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(mPath);
			boolean isExistsInAlluxio = isExistsInAlluxio(mUri);
			if (isExistsInAlluxio) {
				return new FSDataInputStream(new HdfsFileInputStream(mContext, mUri,conf,bufferSize,mStatistics));
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			return hdfsUfsInfo.getHdfsUfs().open(hdfsUfsInfo.getHdfsPath());
		} catch (IOException e) {
			LOG.error("open Failed in HDFS Space ({})", path);
			throw e;
		}
	}

	/**
	 * Currently rename just support:
	 * 1. both in Alluxio Space with UserMustCacheList or with the Same HDFS authority;
	 * 2. both in HDFS with the same authority;
	 * @param src src path to rename
	 * @param dst dst path to rename
	 * @return true if rename success in Alluxio or HDFS or both, otherwise, false
	 * @throws IOException
	 */
	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		LOG.debug("rename({}, {})", src, dst);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}
		AlluxioURI srcUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(src));
		AlluxioURI dstUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(dst));
		String mSrc = HadoopUtils.getPathWithoutScheme(src);
		String mDst = HadoopUtils.getPathWithoutScheme(dst);
		HdfsUfsInfo hdfsUfsInfoSrc = PathResolve(src);
		HdfsUfsInfo hdfsUfsInfoDst = PathResolve(dst);
		org.apache.hadoop.fs.FileSystem hdfsSrc = hdfsUfsInfoSrc.getHdfsUfs();
		Path hdfsSrcPath = hdfsUfsInfoSrc.getHdfsPath();
		org.apache.hadoop.fs.FileSystem hdfsDst = hdfsUfsInfoDst.getHdfsUfs();
		Path hdfsDstPath = hdfsUfsInfoDst.getHdfsPath();
		boolean isSameHDFSAuthority = hdfsSrcPath.toUri().getAuthority().
				equals(hdfsDstPath.toUri().getAuthority());
		boolean srcInList = isInMustCacheList(mSrc);
		boolean dstInList = isInMustCacheList(mDst);

		if (MODE_CACHE_ENABLED) {
			if (srcInList && dstInList) {
				try {
					mFileSystem.rename(srcUri, dstUri);
					return true;
				} catch (AlluxioException e) {
					LOG.error("rename Failed in Alluxio Space (src {}, dst {})", src, dst);
					throw new RuntimeException(e);
				}
			}
			if (srcInList || dstInList) {
				LOG.error("DoesNotSupport: rename across the alluxio and HDFS");
				throw new UnsupportedOperationException();
			}

			if (isSameHDFSAuthority && isExistsInAlluxio(srcUri)) {
				try {
					mFileSystem.rename(srcUri,dstUri);
				} catch (AlluxioException e1) {
					throw new RuntimeException(e1);
				}
			}
		}

		if (!isSameHDFSAuthority) {
			LOG.error("DoseNotSupport: rename across the different HDFS({}, {})", hdfsSrcPath, hdfsDstPath);
			throw new UnsupportedOperationException();
		}
		try {
			return (hdfsSrc.rename(hdfsSrcPath, hdfsDstPath));
		} catch (IOException e) {
			LOG.error("rename Failed in HDFS Space(src {}, dst {})", src, dst);
			throw e;
		}
	}

	private boolean isExistsInAlluxio(AlluxioURI path) throws IOException{
		try {
			return mFileSystem.exists(path);
		} catch (AlluxioException e) {
			throw new RuntimeException(e);
		}
	}

	private boolean isInMustCacheList(String path) throws IOException{
		if(mUserMustCacheList == null || !mUserClientCacheEnabled){
			try {
				mUserMustCacheList = new PrefixList(mFileSystem.getUserMustCacheList());
			} catch (AlluxioException e) {
				throw new RuntimeException(e);
			}
		}
		return mUserMustCacheList.inList(path);
	}

	private HdfsUfsInfo getUfsFileSystem(String path,String alluxioMountPoint,String ufsMountPoint)
			throws IOException{
		URI hdfsUri = null;
		try {
			hdfsUri = new URI(ufsMountPoint);
		} catch (URISyntaxException e){
			LOG.error("URISyntaxException ({})", ufsMountPoint);
			throw new RuntimeException(e);
		}
		String authority = hdfsUri.getAuthority();
		org.apache.hadoop.fs.FileSystem hdfsUfs = hdfsFileSystemCache.get(authority);
		if(hdfsUfs == null){
			hdfsUfs = org.apache.hadoop.fs.FileSystem.get(hdfsUri,conf);
			hdfsFileSystemCache.put(authority,hdfsUfs);
		}
		String ufsPath = null;
		if (path.length() >= alluxioMountPoint.length()) {
			ufsPath = ufsMountPoint.concat(path.substring(alluxioMountPoint.length()));
		} else {
			ufsPath = ufsMountPoint;
		}
		return new HdfsUfsInfo(ufsPath, hdfsUfs);
	}


	private HdfsUfsInfo PathResolve(Path path) throws IOException {
		LOG.info("PathResove({})",path);
		if (!path.toString().startsWith("/")) {
			path = new Path(getHomeDirectory(), path);
		}
		String mPath = HadoopUtils.getPathWithoutScheme(path);
		String ufsMountPoint = null;

		if(mMountPonitList == null || !mUserClientCacheEnabled){
			try {
				mMountPonitList = mFileSystem.getMountPoint();
			} catch (AlluxioException e) {
				throw new RuntimeException(e);
			}
		}
		try {
			for (MountPairInfo mMountPointInfo : mMountPonitList) {
				String alluxioMountPoint = mMountPointInfo.getAlluxioPath();
				if (!alluxioMountPoint.equals("/") && PathUtils.hasPrefix(mPath, alluxioMountPoint)) {
					ufsMountPoint = mMountPointInfo.getUfsPath();
					return getUfsFileSystem(mPath, alluxioMountPoint, ufsMountPoint);
				} else if (alluxioMountPoint.startsWith(mPath)) {
					//handle the path for non mount Point (For Default FS	is local UFS)
					try {
						URI ufsUriForNonMountPoint = new URI(mMountPointInfo.getUfsPath());
						if (ufsUriForNonMountPoint.getScheme() != null && ufsMountPoint == null) {
							ufsMountPoint = ufsUriForNonMountPoint.getScheme() + "://" + ufsUriForNonMountPoint.getAuthority() + mPath;
							return getUfsFileSystem(mPath, alluxioMountPoint,ufsMountPoint);
						}
					} catch (URISyntaxException e){
						LOG.error("URISyntaxException ({})", ufsMountPoint);
						throw new RuntimeException(e);
					}
				}
			}
			LOG.error("PathDoesNotMounted({}, {})", path, mMountPonitList);
			return null;
		} catch (InvalidPathException e){
			throw new RuntimeException(e);
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("AbstractFileSystemProxy{");
		sb.append("MODE_CACHE_ENABLED=").append(MODE_CACHE_ENABLED);
		sb.append(", hdfsFileSystemCache=").append(hdfsFileSystemCache);
		sb.append(", mMountPonitList=").append(mMountPonitList);
		sb.append(", mContext=").append(mContext);
		sb.append(", mFileSystem=").append(mFileSystem);
		sb.append(", mUri=").append(mUri);
		sb.append(", mWorkingDir=").append(mWorkingDir);
		sb.append(", mStatistics=").append(mStatistics);
		sb.append(", mAlluxioHeader='").append(mAlluxioHeader).append('\'');
		sb.append(", mUserMustCacheList=").append(mUserMustCacheList);
		sb.append(", mUserClientCacheEnabled=").append(mUserClientCacheEnabled);
		sb.append(", conf=").append(conf);
		sb.append('}');
		return sb.toString();
	}
}
