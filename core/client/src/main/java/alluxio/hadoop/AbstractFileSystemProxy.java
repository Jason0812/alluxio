package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.lineage.LineageContext;
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
	//public static final String FIRST_COM_PATH = "alluxio_dep/";
	private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
	// Always tell Hadoop that we have 3x replication.
	private static final int BLOCK_REPLICATION_CONSTANT = 3;
	/**
	 * Lock for initializing the contexts, currently only one set of contexts is supported.
	 */
	private static final Object INIT_LOCK = new Object();

	private static final boolean MODE_ROUTE_ENABLED =
			Configuration.getBoolean(PropertyKey.USER_MODE_ROUTE_ENABLED);
	private static final boolean MODE_CACHE_ENABLED =
			Configuration.getBoolean(PropertyKey.USER_MODE_CACHE_ENABLED);

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

	private PrefixList mUserMustCacheList = new PrefixList(
			Configuration.getList(PropertyKey.USER_MUSTCACHELIST, ","));

	private org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

	AbstractFileSystemProxy() {
	}

	@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
	AbstractFileSystemProxy(FileSystem fileSystem) {
		mFileSystem = fileSystem;
		sInitialized = true;
	}

	@Override
	public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
		LOG.debug("append: {} {} {}", path, bufferSize, progress);
		if (mStatistics != null) {
			mStatistics.incrementBytesWritten(1);
		}
		String mPath = HadoopUtils.getPathWithoutScheme(path);

		if (MODE_CACHE_ENABLED) {
			AlluxioURI mUri = new AlluxioURI(mPath);
			boolean isExistsInAlluxio = isExistsInAlluxio(mUri);
			if (mUserMustCacheList.inList(mPath)){
				if(isExistsInAlluxio) {
					LOG.error("append path: {} in UserMustCacheList: {}, " +
							"Alluxio Space does not support append currently", path, mUserMustCacheList);
					throw new IOException();
				}else{
					CreateFileOptions options = CreateFileOptions.defaults()
							.setRecursive(true);
					try {
						mFileSystem.createFile(mUri, options);
					} catch (AlluxioException e) {
						throw new IOException(e);
					}
				}
			}
			if(isExistsInAlluxio){
				try {
					LOG.debug("Append file in Alluxio space, delete it first. Path: {}", path);
					//todo: Concurrently ISSUE;
					mFileSystem.delete(mUri);
				} catch (IOException | AlluxioException e1) {
					LOG.error("Delete Failed.");
					throw new IOException(e1);
				}
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			return hdfsUfsInfo.getHdfsUfs().append(hdfsUfsInfo.getHdfsPath(), bufferSize, progress);
		}catch(IOException e){
			LOG.error("HDFS append failed");
			throw new IOException(e);
		}
	}

	@Override
	public void close() throws IOException {
		if (mContext != FileSystemContext.INSTANCE) {
			mContext.close();
		}
		super.close();
	}

	@Override
	public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite,
			int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
		LOG.debug("create: {} {} {} {} {} {} {}", path, permission, overwrite, bufferSize, replication,
				blockSize, progress);
		if (mStatistics != null) {
			mStatistics.incrementBytesWritten(1);
		}

		//Alluxio just support write must Cache
		if (MODE_CACHE_ENABLED) {
			LOG.debug("Create File Path: {}, in UserMustCacheList: {}", path.toString(), mUserMustCacheList);
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(mPath);
			boolean isExistsInAlluxio = isExistsInAlluxio(mUri);
			try {
				if (isExistsInAlluxio) {
					if (!overwrite) {
						throw new IOException(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage());
					}
					if (mFileSystem.getStatus(mUri).isFolder()) {
						throw new IOException(ExceptionMessage.FILE_CREATE_IS_DIRECTORY.getMessage());
					}
					mFileSystem.delete(mUri);
				}
				CreateFileOptions options = CreateFileOptions.defaults()
						.setWriteType(WriteType.MUST_CACHE).setMode(new Mode(permission.toShort()));
				if(mUserMustCacheList.inList(mPath)) {
					return new FSDataOutputStream(mFileSystem.createFile(mUri, options), mStatistics);
				}
			} catch (IOException | AlluxioException e1) {
				LOG.error("create: {} {} {} {} {} {} {} failed in Alluxio Space", path, permission, overwrite, bufferSize, replication,
						blockSize, progress);
				throw new IOException(e1);
			}
		}


		//if create path is not in userMustCacheList, create through to HDFS directly;
		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			return new FSDataOutputStream(hdfsUfsInfo.getHdfsUfs().create(hdfsUfsInfo.getHdfsPath(),
					permission,overwrite,bufferSize,(short)BLOCK_REPLICATION_CONSTANT,blockSize,progress),
					mStatistics);
		} catch (IOException e) {
			LOG.error("HDFS Space create failed.");
			throw new IOException(e);
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
		LOG.debug("delete: {} {}", path, recursive);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}
		//delete alluxio space data first;
		if (MODE_CACHE_ENABLED) {
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(mPath);
			DeleteOptions options = DeleteOptions.defaults().setRecursive(recursive);
			boolean inUserMustCacheList = mUserMustCacheList.inList(mPath);
			try {
				mFileSystem.delete(mUri, options);
				if (inUserMustCacheList) {
					return true;
				}
			} catch (FileDoesNotExistException e){
				LOG.error("FileDoesnotExistException, path: {}", path);
				if (inUserMustCacheList) {
					throw new FileNotFoundException("FileNotFound");
				}
			} catch (DirectoryNotEmptyException e1){
				LOG.error("Directory is not empty, please set Recursive. Path: {}, isRecursive: {}",
					path, recursive);
				throw new IOException(e1);
			} catch (AlluxioException e2) {
				LOG.error("Delete in Alluxio space failed, path: {}", path);
				if (inUserMustCacheList) {
					throw new IOException(e2);
				}
			}
		}

		//If path is not in mUserMustCacheList, should delete it in HDFS space;
		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		org.apache.hadoop.fs.FileSystem hdfs = hdfsUfsInfo.getHdfsUfs();
		Path hdfsPath = hdfsUfsInfo.getHdfsPath();
		try {
			return ((hdfs.delete(hdfsPath, true)));
		}catch (IOException e){
			LOG.error("HDFS Space delete Failed, Path: {}", path);
			throw new IOException(e);
		}
	}

	@Deprecated
	@Override
	public long getDefaultBlockSize() {
		return Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
	}

	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		LOG.debug("Get File Block Location: {} {} {}", file, start, len);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}

		String mPath = HadoopUtils.getPathWithoutScheme(file.getPath());
		if (MODE_CACHE_ENABLED) {
			AlluxioURI mUri = new AlluxioURI(mPath);
			boolean isExistsInAlluxio = isExistsInAlluxio(mUri);
			boolean inUserMustCacheList = mUserMustCacheList.inList(mPath);
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
				LOG.error("FileDostNotExistException, file: {}", file);
				if (inUserMustCacheList) {
					throw new FileNotFoundException("FileNotFound");
				}
			} catch (AlluxioException e1) {
				LOG.error("getFileBlockLocations Faild, file: {}", file);
				if (inUserMustCacheList) {
					throw new IOException(e1);
				}
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(file.getPath());
		try {
			return hdfsUfsInfo.getHdfsUfs().getFileBlockLocations(hdfsUfsInfo.getHdfsPath(), start, len);
		}catch (IOException e){
			LOG.error("getFileBlockLocations from HDFS space failed. path: ", mPath);
			throw new IOException(e);
		}
	}

	@Override
	public FileStatus getFileStatus(Path path) throws IOException {
		LOG.debug("Get status: {}", path);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}

		if (MODE_CACHE_ENABLED) {
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
			boolean inMustCacheList = mUserMustCacheList.inList(mPath);
			if (inMustCacheList) {
				try {
					URIStatus fileStatus = mFileSystem.getStatus(mUri);
					return new FileStatus(fileStatus.getLength(), fileStatus.isFolder(), BLOCK_REPLICATION_CONSTANT,
						fileStatus.getBlockSizeBytes(), fileStatus.getLastModificationTimeMs(), fileStatus.getCreationTimeMs(),
						new FsPermission((short) fileStatus.getMode()), fileStatus.getOwner(), fileStatus.getGroup(),
						new Path(mAlluxioHeader + mUri));
				} catch (FileDoesNotExistException e) {
					throw new FileNotFoundException("FileNotFound");
				} catch (AlluxioException e1) {
					LOG.error("getStatus from Alluxio Space failed. path: {}", path);
					throw new IOException(e1);
				}
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			FileStatus fileStatus = hdfsUfsInfo.getHdfsUfs().getFileStatus(hdfsUfsInfo.getHdfsPath());
			//revert to Alluxio space path;
			String alluxioPath = path.toString().concat(
				fileStatus.getPath().toString().substring(hdfsUfsInfo.getHdfsPath().toString().length()));
			fileStatus.setPath(new Path(alluxioPath));
			return fileStatus;
		}catch(FileNotFoundException e){
			LOG.debug("File Not found, path: {}", path);
			throw e;
		} catch (IOException e1){
			LOG.error("getFileStatus for HDFS space failed, path: {}", path);
			throw new IOException(e1);
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
	public void setOwner(Path path, final String username, final String groupname) throws IOException {
		LOG.debug("Set owner: {} {} {}", path, username, groupname);

		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}

		URIStatus fileStatus = null;
		String mPath = HadoopUtils.getPathWithoutScheme(path);
		AlluxioURI mUri = new AlluxioURI(mPath);

		if (MODE_CACHE_ENABLED) {
			boolean isInUserCacheMustList = mUserMustCacheList.inList(mPath);
			//roll back after hdfs set failed, while alluxio space success.
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
					LOG.error("FileDoesNotExistException, path: {}", path);
					if (isInUserCacheMustList) {
						throw new IOException(e);
					}
				} catch (AlluxioException e1) {
					LOG.error("setOwner failed in Alluxio space.path: {}", path);
					throw new IOException(e1);
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
				LOG.error("HDFS setOwner failed, while setOwner back in Alluxio Space Failed, path: {}", path);
				throw new IOException(e1);
			}
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
		LOG.debug("Set permission: {} {}", path, permission);

		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}

		URIStatus fileStatus = null;
		String mPath = HadoopUtils.getPathWithoutScheme(path);
		AlluxioURI mUri = new AlluxioURI(mPath);

		if (MODE_CACHE_ENABLED) {
			boolean isInUserMustCacheList = mUserMustCacheList.inList(mPath);
			try {
				SetAttributeOptions options = SetAttributeOptions.defaults()
					.setMode(new Mode(permission.toShort())).setRecursive(false);
				fileStatus = mFileSystem.getStatus(mUri);
				mFileSystem.setAttribute(mUri, options);
				if (isInUserMustCacheList) {
					return;
				}
			} catch (FileDoesNotExistException e) {
				LOG.error("FileDoseNotExistException, path: {}", path);
				if (isInUserMustCacheList) {
					throw new IOException(e);
				}
			} catch (AlluxioException e1) {
				LOG.error("setOwner failed in Alluxio Space, path: {}", path);
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
				LOG.error("HDFS setPermission failed, while setPermission back in Alluxio Space Failed, path: {}", path);
				throw new IOException(e1);
			}
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

	/**
	 * TODO(Jason): need to investigate initilize method;
	 */
	@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
	@Override
	public void initialize(URI uri, org.apache.hadoop.conf.Configuration conf) throws IOException {
		Preconditions.checkNotNull(uri.getHost(), PreconditionMessage.URI_HOST_NULL);
		Preconditions.checkNotNull(uri.getPort(), PreconditionMessage.URI_PORT_NULL);

		super.initialize(uri, conf);
		conf.set("fs.hdfs.impl.disable.cache", System.getProperty("fs.hdfs.impl.disable.cache", "true"));
		LOG.debug("initialize({}, {}). Connecting to Alluxio", uri, conf);
		//todo(cr): check
		HadoopUtils.addS3Credentials(conf);
		HadoopUtils.addSwiftCredentials(conf);

		setConf(conf);
		mAlluxioHeader = getScheme() + "://" + uri.getHost() + ":" + uri.getPort();
		// Set the statistics member. Use mStatistics instead of the parent class's variable.
		mStatistics = statistics;
		mUri = URI.create(mAlluxioHeader);

		boolean masterAddIsSameAsDefault = checkMasterAddress();

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

			initializeInternal(uri, conf);
			sInitialized = true;
		}

		updateFileSystemAndContext();
	}

	void initializeInternal(URI uri, org.apache.hadoop.conf.Configuration conf) throws IOException {
		// Load Alluxio configuration if any and merge to the one in Alluxio file system. These
		// modifications to ClientContext are global, affecting all Alluxio clients in this JVM.
		// We assume here that all clients use the same configuration.
		ConfUtils.mergeHadoopConfiguration(conf);

		Configuration.set(PropertyKey.MASTER_HOSTNAME, uri.getHost());
		Configuration.set(PropertyKey.MASTER_RPC_PORT, uri.getPort());
		Configuration.set(PropertyKey.ZOOKEEPER_ENABLED, isZookeeperMode());

		// These must be reset to pick up the change to the master address.
		// TODO(andrew): We should reset key value system in this situation - see ALLUXIO-1706.
		LineageContext.INSTANCE.reset();
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

	//TODO(Jason): this method is need ????????????
	//try to disable this method;
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
	 * 1. path just in Alluxio space, load metadata from Alluxio Master;
	 * 2. path both in Alluixo space and hdfs or just in hdfs space only, load metadata from hdfs namenode;
	 * after get the status from hdfs, should transfer to alluxio path uri with
	 * alluxio mountpoint and ufs mountpoint;
	 * @param path path to list status
	 * @return
	 * @throws IOException
	 */
	@Override
	public FileStatus[] listStatus(Path path) throws IOException {
		LOG.debug("File Status: {}", path);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}

		if (MODE_CACHE_ENABLED) {
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(mPath);
			boolean isInUserMustCacheList = mUserMustCacheList.inList(mPath);
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
					LOG.error("FileDoesNotExistException, path: {}", path);
					throw new FileNotFoundException("FileNotFound");
				} catch(InvalidPathException e){
					LOG.error("Invalid Path Exception. path: {}", path);
					throw new IOException(e);
				} catch(AlluxioException e2){
					LOG.error("listStatus failed, path: {}", path);
					throw new IOException(e2);
				}
			}
		}

		//file not in alluxio space, while in hdfs space;
		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		String hdfsMountPoint = hdfsUfsInfo.getHdfsPath().toString();
		try {
			FileStatus[] fileStatus = hdfsUfsInfo.getHdfsUfs().listStatus(new Path(hdfsMountPoint));
			//Display the alluxio space Path
			for (int i = 0; i < fileStatus.length; i++) {
				FileStatus fileStatusInfo = fileStatus[i];
				String alluxioPath = path.toString().concat(
					fileStatusInfo.getPath().toString().substring(hdfsUfsInfo.getHdfsPath().toString().length()));
				fileStatusInfo.setPath(new Path(alluxioPath));
				fileStatus[i] = fileStatusInfo;
			}
			return fileStatus;
		}catch (IOException e){
			LOG.error("listStatus failed in HDFS Space, path: {}", path);
			throw new IOException(e);
		}
	}

	@Override
	public boolean mkdirs(Path path, FsPermission permission) throws IOException {
		LOG.debug("mkdirs: {} {}", path, permission);
		if (mStatistics != null) {
			mStatistics.incrementBytesWritten(1);
		}

		if (MODE_CACHE_ENABLED) {
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(mPath);
			if (mUserMustCacheList.inList(mPath)) {
				try {
					//TODO(Jason): if create directory is already exists.
					CreateDirectoryOptions options = CreateDirectoryOptions.defaults().setRecursive(true)
							.setAllowExists(true).setMode(new Mode(permission.toShort()))
							.setWriteType(WriteType.MUST_CACHE);
					mFileSystem.createDirectory(mUri, options);
					return true;
				} catch (AlluxioException e) {
					LOG.error("createDirectory in Alluxio space failed, path: {}, FsPermission: {}", path, permission);
					throw new IOException(e);
				}
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			return hdfsUfsInfo.getHdfsUfs().mkdirs(hdfsUfsInfo.getHdfsPath(), permission);
		}catch (IOException e){
			LOG.error("mkdirs in HDFS space failed, path: {}, permission: {}", path, permission);
			throw new IOException(e);
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
		LOG.debug("open: {} {}", path, bufferSize);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}

		if (MODE_CACHE_ENABLED) {
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(mPath);
			boolean isExistsInAlluxio = isExistsInAlluxio(mUri);
			boolean isInUserMustCacheList = mUserMustCacheList.inList(mPath);
			if (isExistsInAlluxio) {
				//OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
				return new FSDataInputStream(new HdfsFileInputStream(mContext, mUri,conf,bufferSize,mStatistics));
			}
			if(isInUserMustCacheList){
				LOG.error("open path: {} in Alluxio space failed", path);
				throw new FileNotFoundException();
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			return hdfsUfsInfo.getHdfsUfs().open(hdfsUfsInfo.getHdfsPath());
		}catch (IOException e){
			LOG.error("open path: {} in HDFS space failed", path);
			throw new IOException(e);
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
		LOG.debug("rename: {} {}", src, dst);

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
		boolean isSameHDFSAuthority = hdfsSrcPath.toUri().getAuthority().equals(hdfsDstPath.toUri().getAuthority());

		boolean srcInList = mUserMustCacheList.inList(mSrc);
		boolean dstInList = mUserMustCacheList.inList(mDst);

		if (MODE_CACHE_ENABLED) {
			if (srcInList && dstInList) {
				try {
					mFileSystem.rename(srcUri, dstUri);
					return true;
				} catch (AlluxioException e) {
					throw new IOException(e);
				}
			}
			if(srcInList || dstInList){
				LOG.error("Currently doesnot support rename");
				throw new IOException();
			}
			if (isSameHDFSAuthority) {
				try {
					mFileSystem.rename(srcUri, dstUri);
				} catch (FileNotFoundException e) {
					LOG.error("rename failed in alluxio space, src: {} to dst: {}", src, dst);
				} catch (AlluxioException e) {
					throw new IOException(e);
				}
			}
			else{
				LOG.error("Currently doesnot support rename path with different HDFS cluster");
				return false;
			}
		}

		if (!isSameHDFSAuthority) {
			LOG.error("Currently doest support rename across different HDFS cluster", hdfsSrcPath, hdfsDstPath);
			throw new IOException();
		}
		try {
			return (hdfsSrc.rename(hdfsSrcPath, hdfsDstPath));
		} catch (IOException e) {
			LOG.error("rename src: {} to dst: {} failed in HDFS space", src, dst);
			throw new IOException(e);
		}
	}

	/**
	 * Convenience method which ensures the given path exists, wrapping any {@link AlluxioException}
	 * in {@link IOException}.
	 *
	 * @param path the path to look up
	 * @throws IOException if an Alluxio exception occurs
	 */
	private void ensureExists(AlluxioURI path) throws IOException {
		try {
			mFileSystem.getStatus(path);
		} catch (AlluxioException e) {
			throw new IOException(e);
		}
	}

	private boolean isExistsInAlluxio(AlluxioURI path) throws IOException{
		try {
			return mFileSystem.exists(path);
		} catch (AlluxioException e) {
			throw new IOException(e);
		}
	}

	private HdfsUfsInfo PathResolve(Path path) throws IOException {
		LOG.debug("Path Resolve: {}", path);
		String mPath = HadoopUtils.getPathWithoutScheme(path);
		String alluxioMountPoint = null;
		String ufsMountPoint = null;
		URI hdfsUri = null;
		boolean isFoundMountPoint = false;

		if(mMountPonitList == null){
			try {
				mMountPonitList = mFileSystem.getMountPoint();
			} catch (AlluxioException e) {
				LOG.error("MountTable without any mountPoint");
				throw new IOException();
			}
		}
		try {
			for (MountPairInfo mMountPointInfo : mMountPonitList) {
				String alluxioPath = mMountPointInfo.getAlluxioPath();
				if (!alluxioPath.equals("/") && PathUtils.hasPrefix(mPath, alluxioPath)) {
					LOG.info("Enter it.");
					isFoundMountPoint = true;
					alluxioMountPoint = alluxioPath;
					ufsMountPoint = mMountPointInfo.getUfsPath();
					break;
				}
			}
		} catch (InvalidPathException e){
			LOG.error("InvalidPathException");
			throw new IOException(e);
		}
		if(!isFoundMountPoint){
			LOG.error("Cannot find MountPoint for path: {}, in MountTable: {}", path, mMountPonitList);
			throw new IOException();
		}
		try {
			hdfsUri = new URI(ufsMountPoint);
		} catch (URISyntaxException e){
			LOG.error("Construct URI, path: {}", ufsMountPoint);
		}
		String authority = hdfsUri.getAuthority();
		org.apache.hadoop.fs.FileSystem hdfsUfs = hdfsFileSystemCache.get(authority);
		if(hdfsUfs == null){
			hdfsUfs = org.apache.hadoop.fs.FileSystem.get(hdfsUri,conf);
			hdfsFileSystemCache.put(authority,hdfsUfs);
		}
		if(!ufsMountPoint.endsWith("/")){
			ufsMountPoint = ufsMountPoint.concat("/");
		}
		String ufsPath = ufsMountPoint.concat(mPath.substring(alluxioMountPoint.length()));
		LOG.info("UfsMountPoint: {}, alluxioMountPoint: {}, Ufs path: {}", ufsMountPoint, alluxioMountPoint, ufsPath);
		return new HdfsUfsInfo(ufsPath, hdfsUfs);
	}

}
