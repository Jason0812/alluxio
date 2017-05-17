package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.lineage.LineageContext;
import alluxio.collections.PrefixList;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.security.User;
import alluxio.security.authorization.Mode;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.CommonUtils;
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
import java.util.HashSet;
import java.util.List;

/**
 * Created by guoyejun on 2017/5/10.
 */
abstract class AbstractFileSystemThrough extends org.apache.hadoop.fs.FileSystem {
	public static final String FIRST_COM_PATH = "alluxio_dep/";
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

	AbstractFileSystemThrough() {

	}

	@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
	AbstractFileSystemThrough(FileSystem fileSystem) {
		mFileSystem = fileSystem;
		sInitialized = true;
	}

	@Override
	public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
		LOG.info("append: {} {} {}", path, bufferSize, progress);
		if (mStatistics != null) {
			mStatistics.incrementBytesWritten(1);
		}

		//Operation in alluxio
		String mPath = HadoopUtils.getPathWithoutScheme(path);
		boolean isExistsInAlluxio = false;
		if (MODE_CACHE_ENABLED) {
			AlluxioURI mUri = new AlluxioURI(mPath);
			if (mUserMustCacheList.inList(mPath)){
				LOG.error("append path: {} in UserMustCacheList: {}, " +
						"Alluxio Space does not support append currently", path, mUserMustCacheList);
				throw new UnsupportedOperationException("Append failed");
			}

			try {
				isExistsInAlluxio = mFileSystem.exists(mUri);
			}catch (InvalidPathException e) {
				LOG.info("InvalidPathException, path: {}", path);
			} catch (IOException | AlluxioException e1) {
				LOG.info("Alluxio(HDFS Proxy) Exception, {}", e1.getMessage());
			}

			if(isExistsInAlluxio){
				try {
					LOG.info("Append file in Alluxio space, delete it first. Path: {}", path);
					mFileSystem.delete(mUri);
				} catch (IOException | AlluxioException e) {
					LOG.error("Delete Failed.");
					throw new IOException(e);
				}
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		try {
			return hdfsUfsInfo.getHdfsUfs().append(hdfsUfsInfo.getHdfsPath(), bufferSize, progress);
		}catch(IOException e){
			LOG.info("HDFS append failed");
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

		LOG.info("create: {} {} {} {} {} {} {}", path, permission, overwrite, bufferSize, replication,
				blockSize, progress);

		if (mStatistics != null) {
			mStatistics.incrementBytesWritten(1);
		}
		//Alluxio just support write must Cache
		if (MODE_CACHE_ENABLED) {
			if (mUserMustCacheList.inList(HadoopUtils.getPathWithoutScheme(path))) {
				LOG.info("Create File Path: {}, in UserMustCacheList: {}", path.toString(), mUserMustCacheList);
				AlluxioURI mUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
				try {
					if (mFileSystem.exists(mUri)) {
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
					return new FSDataOutputStream(mFileSystem.createFile(mUri, options), mStatistics);
				} catch (IOException | AlluxioException e) {
					throw new IOException(e);
				}
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		//todo: verify is file or path, create file with option
		try {
			return new FSDataOutputStream(hdfsUfsInfo.getHdfsUfs().create(hdfsUfsInfo.getHdfsPath(),
					permission,overwrite,bufferSize,(short)BLOCK_REPLICATION_CONSTANT,blockSize,progress)
					mStatistics);
		}catch (IOException e){
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
		LOG.info("delete: {} {}", path, recursive);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}
		//delete alluxio space data first;
		if (MODE_CACHE_ENABLED) {
			AlluxioURI mUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
			DeleteOptions options = DeleteOptions.defaults().setRecursive(recursive);
			try {
				mFileSystem.delete(mUri);
			}catch (FileNotFoundException e){
				LOG.info("File not in Alluxio Space, path: {}", path);
			}catch (DirectoryNotEmptyException e1){
				LOG.error("Directory is not empty, please set Recursive. Path: {}, isRecursive: {}",
						path, recursive);
				throw new IOException(e1);
			}
			catch (IOException | AlluxioException e2) {
				LOG.error("Delete in Alluxio space failed, path: {}", path.toString());
				throw new IOException(e2);
			}
			if (mUserMustCacheList.inList(HadoopUtils.getPathWithoutScheme(path))) {
				return true;
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
		LOG.info("Get File Block Location: {} {} {}", file, start, len);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}
		//Get File Block locations from alluxio first;
		String mPath = HadoopUtils.getPathWithoutScheme(file.getPath());
		if (MODE_CACHE_ENABLED) {
			AlluxioURI mUri = new AlluxioURI(mPath);
			boolean isExistsInAlluxio = false;
			try {
				isExistsInAlluxio = mFileSystem.exists(mUri);
			} catch (IOException | AlluxioException e) {
				e.printStackTrace();
			}
			try {
				if (mFileSystem.exists(mUri) || mUserMustCacheList.inList(mPath)) {
					//getFileBlockLocations from alluxio space;
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
							// add under file system locations
							for (String location : fileBlockInfo.getUfsLocations()) {
								names.add(location);
								hosts.add(HostAndPort.fromString(location).getHostText());
							}
							blockLocations.add(new BlockLocation(CommonUtils.toStringArray(names),
									CommonUtils.toStringArray(hosts), offset, fileBlockInfo.getBlockInfo().getLength()));
						}
					}
					BlockLocation[] ret = new BlockLocation[blockLocations.size()];
					blockLocations.toArray(ret);
					return ret;
				}
			} catch (AlluxioException e) {
				LOG.error("getFileBlockLocations failed from Alluxio Space, file: {}", file);
				throw new IOException(e);
			}
		}
		HdfsUfsInfo hdfsUfsInfo = PathResolve(file.getPath());
		return hdfsUfsInfo.getHdfsUfs().getFileBlockLocations(hdfsUfsInfo.getHdfsPath(), start, len);
	}

	@Override
	public FileStatus getFileStatus(Path path) throws IOException {
		LOG.info("Get status: {}", path);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}
		if (MODE_CACHE_ENABLED) {
			AlluxioURI mUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
			try {
				if (mFileSystem.exists(mUri) || mUserMustCacheList.inList(HadoopUtils.getPathWithoutScheme(path))) {
					URIStatus fileStatus = mFileSystem.getStatus(mUri);
					return new FileStatus(fileStatus.getLength(), fileStatus.isFolder(), BLOCK_REPLICATION_CONSTANT,
							fileStatus.getBlockSizeBytes(), fileStatus.getLastModificationTimeMs(), fileStatus.getCreationTimeMs(),
							new FsPermission((short) fileStatus.getMode()), fileStatus.getOwner(), fileStatus.getGroup(),
							new Path(mAlluxioHeader + mUri));
				}
			} catch (AlluxioException e) {
				LOG.error("getFileStatus failed from Alluxio Space, path: {}", path);
				throw new IOException(e);
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		FileStatus fileStatus = hdfsUfsInfo.getHdfsUfs().getFileStatus(hdfsUfsInfo.getHdfsPath());
		//Display the Alluxio Space Path;
		String alluxioPath = path.toString().concat(
				fileStatus.getPath().toString().substring(hdfsUfsInfo.getHdfsPath().toString().length()));
		fileStatus.setPath(new Path(alluxioPath));
		return fileStatus;
	}

	@Override
	public void setOwner(Path path, final String username, final String groupname) throws IOException {
		LOG.info("Set owner: {} {} {}", path, username, groupname);

		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}
		URIStatus fileStatus = null;
		AlluxioURI mUri = null;
		if (MODE_CACHE_ENABLED) {
			mUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
			try {
				if (mFileSystem.exists(mUri) || mUserMustCacheList.inList(HadoopUtils.getPathWithoutScheme(path))) {
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
						mFileSystem.setAttribute(mUri, options);
					}
					if (mUserMustCacheList.inList(HadoopUtils.getPathWithoutScheme(path))) {
						return;
					}
				}
			} catch (AlluxioException e) {
				LOG.error("setOwner failed in Alluxio Space, path: {}", path);
				throw new IOException(e);
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

	@Override
	public void setPermission(Path path, FsPermission permission) throws IOException {
		LOG.info("Set permission: {} {}", path, permission);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}
		URIStatus fileStatus = null;
		AlluxioURI mUri = null;
		if (MODE_CACHE_ENABLED) {
			mUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
			try {
				if (mFileSystem.exists(mUri) || mUserMustCacheList.inList(HadoopUtils.getPathWithoutScheme(path))) {
					SetAttributeOptions options = SetAttributeOptions.defaults()
							.setMode(new Mode(permission.toShort())).setRecursive(false);
					fileStatus = mFileSystem.getStatus(mUri);
					mFileSystem.setAttribute(mUri, options);
					if (mUserMustCacheList.inList(HadoopUtils.getPathWithoutScheme(path))) {
						return;
					}
				}
			} catch (AlluxioException e) {
				LOG.error("setOwner failed in Alluxio Space, path: {}", path);
				throw new IOException(e);
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

	//todo: these two methods usage
	@Override
	public URI getUri() {
		return mUri;
	}

	@Override
	public Path getWorkingDirectory() {
		LOG.info("getWorkingDirectory: {}", mWorkingDir);
		return mWorkingDir;
	}

	//todo: the usage of this api
	@Override
	public void setWorkingDirectory(Path path) {
		LOG.info("setWorkingDirectory({})", path);
		if (path.isAbsolute()) {
			mWorkingDir = path;
		} else {
			mWorkingDir = new Path(mWorkingDir, path);
		}
	}

	@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
	@Override
	public void initialize(URI uri, org.apache.hadoop.conf.Configuration conf) throws IOException {
		Preconditions.checkNotNull(uri.getHost(), PreconditionMessage.URI_HOST_NULL);
		Preconditions.checkNotNull(uri.getPort(), PreconditionMessage.URI_PORT_NULL);

		super.initialize(uri, conf);
		LOG.info("initialize({}, {}). Connecting to Alluxio", uri, conf);
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

	@Override
	public FileStatus[] listStatus(Path path) throws IOException {
		LOG.info("File Status: {}", path);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}
		AlluxioURI mUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
		List<URIStatus> statuses = null;

		//file cache in alluxio space;
		if (MODE_CACHE_ENABLED) {
			try {
				if (mFileSystem.exists(mUri)) {
					statuses = mFileSystem.listStatus(mUri);
				}
			} catch (InvalidPathException e) {
				LOG.error("Invalid Path Exception. path: {}", path);
				throw new IOException(e);
			} catch (FileNotFoundException e1) {
				LOG.error("File Not Found, path: {}", path);
				throw new IOException(e1);
			} catch (AlluxioException e2) {
				LOG.error("listStatus failed, path: {}", path);
				throw new IOException(e2);
			}

			if (statuses != null || mUserMustCacheList.inList(HadoopUtils.getPathWithoutScheme(path))) {
				FileStatus[] ret = new FileStatus[statuses.size()];
				for (int k = 0; k < statuses.size(); k++) {
					URIStatus status = statuses.get(k);

					ret[k] = new FileStatus(status.getLength(), status.isFolder(), BLOCK_REPLICATION_CONSTANT,
							status.getBlockSizeBytes(), status.getLastModificationTimeMs(),
							status.getCreationTimeMs(), new FsPermission((short) status.getMode()), status.getOwner(),
							status.getGroup(), new Path(mAlluxioHeader + status.getPath()));
				}
				return ret;
			}
		}

		//file not in alluxio space, while in hdfs space;
		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		String hdfsMountPoint = hdfsUfsInfo.getHdfsPath().toString();
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
	}

	@Override
	public boolean mkdirs(Path path, FsPermission permission) throws IOException {
		LOG.info("mkdirs: {} {}", path, permission);
		if (mStatistics != null) {
			mStatistics.incrementBytesWritten(1);
		}

		if (MODE_CACHE_ENABLED) {
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(mPath);
			if (mUserMustCacheList.inList(mPath)) {
				try {
					CreateDirectoryOptions options = CreateDirectoryOptions.defaults().setRecursive(true)
							.setAllowExists(true).setMode(new Mode(permission.toShort()));
					mFileSystem.createDirectory(mUri, options);
					return true;
				} catch (AlluxioException e) {
					LOG.error("createDirectory in Alluxio space failed, path: {}, FsPermission: {}", path, permission);
					throw new IOException(e);
				}
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		return hdfsUfsInfo.getHdfsUfs().mkdirs(hdfsUfsInfo.getHdfsPath(), permission);
	}

	@Override
	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		LOG.info("open: {} {}", path, bufferSize);
		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}

		FileInStream fileInStream = null;
		if (MODE_CACHE_ENABLED) {
			String mPath = HadoopUtils.getPathWithoutScheme(path);
			AlluxioURI mUri = new AlluxioURI(mPath);
			try {
				if (mFileSystem.exists(mUri) || mUserMustCacheList.inList(mPath)) {
					OpenFileOptions options = OpenFileOptions.defaults();
					fileInStream = mFileSystem.openFile(mUri, options);
				}
			} catch (FileNotFoundException e) {
				LOG.error("open file failed in alluxio space. path: {}", path);
				e.printStackTrace();
			} catch (AlluxioException e1) {
				LOG.error("Open file in Alluxio space failed.");
				e1.printStackTrace();
			}

			if (fileInStream != null || mUserMustCacheList.inList(mPath)) {
				return new FSDataInputStream(fileInStream);
			}
		}

		HdfsUfsInfo hdfsUfsInfo = PathResolve(path);
		return new FSDataInputStream(hdfsUfsInfo.getHdfsUfs().open(hdfsUfsInfo.getHdfsPath()));
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		LOG.info("rename: {} {}", src, dst);

		if (mStatistics != null) {
			mStatistics.incrementBytesRead(1);
		}

		AlluxioURI srcUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(src));
		AlluxioURI dstUri = new AlluxioURI(HadoopUtils.getPathWithoutScheme(dst));
		String mSrc = HadoopUtils.getPathWithoutScheme(src);
		String mDst = HadoopUtils.getPathWithoutScheme(dst);
		boolean srcInCacheList = mUserMustCacheList.inList(mSrc);
		boolean dstInCacheList = mUserMustCacheList.inList(mDst);

		if (MODE_CACHE_ENABLED) {
			//src in userMustCacheList which stores in alluxio space, dst is not in alluxio space;
			if (srcInCacheList && !dstInCacheList) {
			//todo: alluxio worker write data to HDFS, and delete in alluxio space;
				LOG.error("Currently does not support rename alluxio space to HDFS space.", src, dst);
				return false;
			}
			//src in HDFS space, while dst is in userMustCacheList;
			if (!srcInCacheList && dstInCacheList) {
			//todo: hdfs load data to alluxio space and delete in HDFS space;
				LOG.error("Currently doest not support rename hdfs space to alluxio space.", src, dst);
				return false;
			}
			//src and dst in userMustCacheList both
			if (srcInCacheList && dstInCacheList) {
				try {
					mFileSystem.rename(srcUri, dstUri);
				} catch (FileNotFoundException e) {
					LOG.error("rename failed in alluxio space, src: {} to dst: {}", src, dst);
					return false;
				} catch (AlluxioException e) {
					ensureExists(srcUri);
					URIStatus dstStatus = null;
					try {
						dstStatus = mFileSystem.getStatus(dstUri);
					} catch (IOException | AlluxioException e1) {
						LOG.error("rename failed in alluxio space, src: {} to dst: {}", src, dst);
						return false;
					}
					//when dstpath is a path, move src path file to dstPath
					//todo: need to make sure the alluxio operation
					if (dstStatus != null && dstStatus.isFolder()) {
						dstUri = dstUri.join(srcUri.getName());
					} else {
						LOG.error("rename failed in alluxio space, src: {} to dst: {}", src, dst);
						return false;
					}
					try {
						mFileSystem.rename(srcUri, dstUri);
					} catch (AlluxioException e2) {
						LOG.error("rename failed in alluxio space, src: {} to dst: {}", src, dst);
						return false;
					}
				} catch (IOException e3) {
					LOG.error("rename failed in alluxio space, src: {} to dst: {}", src, dst);
					return false;
				}
			}
		}
		//src and dst both in HDFS;
		HdfsUfsInfo hdfsUfsInfoSrc = PathResolve(src);
		HdfsUfsInfo hdfsUfsInfoDst = PathResolve(dst);
		org.apache.hadoop.fs.FileSystem hdfsSrc = hdfsUfsInfoSrc.getHdfsUfs();
		Path hdfsSrcPath = hdfsUfsInfoSrc.getHdfsPath();
		org.apache.hadoop.fs.FileSystem hdfsDst = hdfsUfsInfoDst.getHdfsUfs();
		Path hdfsDstPath = hdfsUfsInfoDst.getHdfsPath();

		if (!((hdfsSrcPath.toUri().getAuthority().equals(hdfsDstPath.toUri().getAuthority())))) {
			//todo: src and path in different HDFS cluster, create it in dst HDFS cluster,
			//if success, delete in src hdfs cluster;
			LOG.error("Currently doest support rename across different HDFS cluster", hdfsSrcPath, hdfsDstPath);
		}
		return (hdfsSrc.rename(hdfsSrcPath, hdfsDstPath));
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

	private HdfsUfsInfo PathResolve(Path path) throws IOException {
		LOG.info("Path Resolve: {}", path);
		try {
			MountPairInfo mMountPairInfo = mFileSystem.getUfsPathWithMountTable(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path)));
			String alluxioMountPoint = mMountPairInfo.getAlluxioPath();
			String ufsMountPoint = mMountPairInfo.getUfsPath();
			//todo: ensure mounit is HDFS mount point;
			//todo: construct HDFS filesytem with conf
			URI hdfsUri = new URI(ufsMountPoint);
			if (!hdfsUri.getScheme().toLowerCase().equals("hdfs")) {
				LOG.error("Scheme of Ufs is not hdfs, is: {}", hdfsUri.getScheme());
				throw new IOException();
			}

			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
			conf.set("fs.hdfs.impl.disable.cache", System.getProperty("fs.hdfs.impl.disable.cach", "true"));
			org.apache.hadoop.fs.FileSystem hdfsUfs = org.apache.hadoop.fs.FileSystem.get(hdfsUri, conf);
			String ufsPath = ufsMountPoint.concat(HadoopUtils.getPathWithoutScheme(path).substring(alluxioMountPoint.length()));
			LOG.info("UfsMountPoint: {}, alluxioMountPoint: {}, Ufs path: {}", ufsMountPoint, alluxioMountPoint, ufsPath);
			return new HdfsUfsInfo(ufsPath, hdfsUfs);
		} catch (AlluxioException e) {
			throw new IOException(e);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}

	public final class HdfsUfsInfo {
		private final Path hdfsPath;
		private final org.apache.hadoop.fs.FileSystem hdfsUfs;

		public HdfsUfsInfo(String ufsPath, org.apache.hadoop.fs.FileSystem hdfsFs) {
			hdfsPath = new Path(ufsPath);
			hdfsUfs = hdfsFs;
		}

		public Path getHdfsPath() {
			return hdfsPath;
		}

		public org.apache.hadoop.fs.FileSystem getHdfsUfs() {
			return hdfsUfs;
		}
	}
}
