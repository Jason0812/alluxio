package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.file.*;
import alluxio.client.file.FileSystem;
import alluxio.client.lineage.LineageContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.security.User;
import alluxio.underfs.UnderFileSystem;
import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.Subject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.Principal;
import java.util.HashSet;

/**
 * Created by guoyejun on 2017/5/10.
 */
abstract class AbstractFileSystemThrough extends org.apache.hadoop.fs.FileSystem{
    public static final String FIRST_COM_PATH = "alluxio_dep/";
    private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
    // Always tell Hadoop that we have 3x replication.
    private static final int BLOCK_REPLICATION_CONSTANT = 3;
    /** Lock for initializing the contexts, currently only one set of contexts is supported. */
    private static final Object INIT_LOCK = new Object();

    /** Flag for if the contexts have been initialized. */
    @GuardedBy("INIT_LOCK")
    private static volatile boolean sInitialized = false;

    private FileSystemContext mContext = null;
    private alluxio.client.file.FileSystem mFileSystem = null;

    private URI mUri = null;
    private Path mWorkingDir = new Path(AlluxioURI.SEPARATOR);
    private Statistics mStatistics = null;
    private String mAlluxioHeader = null;

    AbstractFileSystemThrough(){

    }
    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    AbstractFileSystemThrough(FileSystem fileSystem){
        mFileSystem = fileSystem;
        sInitialized = true;
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        LOG.debug("append: {} {} {}", path, bufferSize, progress);
        if(mStatistics != null){
            mStatistics.incrementBytesWritten(1);
        }
        try {
            String ufsPath = mFileSystem
                    .getUfsPathWithMountTable(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path)))
                    .getUfsPath();
            UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
            if(ufs.exists(ufsPath)){
                throw new IOException(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(ufsPath));
            }
            //todo:
            return new FSDataOutputStream(ufs.create(ufsPath), mStatistics);
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if(mContext != FileSystemContext.INSTANCE){
            mContext.close();
        }
        super.close();
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite,
       int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        LOG.debug("create: {} {} {} {} {} {} {}", path, permission, overwrite, bufferSize, replication,
           blockSize,progress);
        if (mStatistics != null){
            mStatistics.incrementBytesWritten(1);
        }
        try {
            String ufsPath = mFileSystem
                    .getUfsPathWithMountTable(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path)))
                    .getUfsPath();
            UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
            if(ufs.exists(ufsPath)){
                if(!overwrite){
                    throw new IOException(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(ufsPath));
                }
                //todo: verify is file or path
                ufs.deleteFile(ufsPath);
            }
            return new FSDataOutputStream(ufs.create(ufsPath),mStatistics);
        }catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Deprecated
    @Override
    public FSDataOutputStream createNonRecursive(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        //todo: is needed to implement this method;
        return create(path,permission,overwrite,bufferSize,replication,blockSize,progress);
    }

    @Deprecated
    @Override
    public boolean delete(Path path) throws IOException{
        return delete(path,true);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException{
        LOG.debug("delete: {} {}", path, recursive);
        if(mStatistics != null){
            mStatistics.incrementBytesRead(1);
        }
        try {
            String ufsPath = mFileSystem
                    .getUfsPathWithMountTable(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path)))
                    .getUfsPath();
            UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
            //todo: check file exists??? no
            return ((ufs.isFile(ufsPath) && ufs.deleteFile(ufsPath))
                    || (ufs.isDirectory(ufsPath) && ufs.deleteDirectory(ufsPath)));
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Deprecated
    @Override
    public long getDefaultBlockSize(){
        return Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
    }

    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException{
        LOG.debug("Get File Block Location: {} {} {}", file, start, len);
        if (mStatistics != null){
            mStatistics.incrementBytesRead(1);
        }
        try {
            String ufsPath = mFileSystem
                    .getUfsPathWithMountTable(new AlluxioURI(HadoopUtils.getPathWithoutScheme(file.getPath())))
                    .getUfsPath();
            UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
            //todo: open the ufs getFileBlockLocations directly
            return null;
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        LOG.debug("Get status: {}", path);
        if(mStatistics != null){
            mStatistics.incrementBytesRead(1);
        }
        try {
            String ufsPath = mFileSystem
                    .getUfsPathWithMountTable(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path)))
                    .getUfsPath();
            UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
            //todo: open the ufs getStatus directly
            return null;
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void setOwner(Path path, final String username, final String groupname) throws IOException {
        LOG.debug("Set owner: {} {} {}",path, username, groupname);
        if(mStatistics != null){
            mStatistics.incrementBytesRead(1);
        }
        try {
            String ufsPath = mFileSystem
                    .getUfsPathWithMountTable(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path)))
                    .getUfsPath();
            UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
            ufs.setOwner(ufsPath, username, groupname);
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        LOG.debug("Set permission: {} {}", path, permission);
        if(mStatistics != null){
            mStatistics.incrementBytesRead(1);
        }
        try {
            String ufsPath = mFileSystem
                    .getUfsPathWithMountTable(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path)))
                    .getUfsPath();
            UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
            ufs.setMode(ufsPath, permission.toShort());
        } catch (AlluxioException e) {
            throw new IOException(e);
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
        LOG.debug("getWorkingDirectory: {}", mWorkingDir);
        return mWorkingDir;
    }

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    @Override
    public void initialize(URI uri, org.apache.hadoop.conf.Configuration conf) throws IOException {
        Preconditions.checkNotNull(uri.getHost(), PreconditionMessage.URI_HOST_NULL);
        Preconditions.checkNotNull(uri.getPort(), PreconditionMessage.URI_PORT_NULL);

        super.initialize(uri, conf);
        LOG.debug("initialize({}, {}). Connecting to Alluxio", uri, conf);
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
    public FileStatus[] listStatus(Path path) throws IOException{
        LOG.debug("File Status: {}", path);
        if(mStatistics != null){
            mStatistics.incrementBytesRead(1);
        }
        try {
            String ufsPath = mFileSystem
                    .getUfsPathWithMountTable(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path)))
                    .getUfsPath();
            UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
            //todo: open listStatus directly
            // return ufs.listStatus(ufsPath);
            return null;
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        LOG.debug("mkdirs: {} {}", path, permission);
        if(mStatistics != null){
            mStatistics.incrementBytesWritten(1);
        }
        try {
            String ufsPath = mFileSystem
                    .getUfsPathWithMountTable(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path)))
                    .getUfsPath();
            UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
            //todo: mdkirs with different options
            return ufs.mkdirs(ufsPath);
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        LOG.debug("open: {} {}", path, bufferSize);
        if(mStatistics != null){
            mStatistics.incrementBytesRead(1);
        }
        try {
            String ufsPath = mFileSystem
                    .getUfsPathWithMountTable(new AlluxioURI(HadoopUtils.getPathWithoutScheme(path)))
                    .getUfsPath();
            UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath);
            //todo: open with options
            return new FSDataInputStream(ufs.open(ufsPath));
        } catch (AlluxioException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.debug("rename: {} {}", src, dst);
        if(mStatistics != null){
            mStatistics.incrementBytesRead(1);
        }
        try {
            String ufsPathSrc = mFileSystem
                    .getUfsPathWithMountTable(new AlluxioURI(HadoopUtils.getPathWithoutScheme(src)))
                    .getUfsPath();
            String ufsPathDst = mFileSystem
                    .getUfsPathWithMountTable(new AlluxioURI(HadoopUtils.getPathWithoutScheme(dst)))
                    .getUfsPath();
            UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPathSrc);
            return ((ufs.isFile(ufsPathSrc) && ufs.isFile(ufsPathDst) && ufs.renameFile(ufsPathSrc, ufsPathDst))
                    || (ufs.isDirectory(ufsPathSrc) && ufs.isDirectory(ufsPathDst) && ufs.renameDirectory(ufsPathSrc,ufsPathDst)));
        } catch (AlluxioException e) {
            LOG.error("Failed to rename {} to {}", src, dst);
            return false;
        }
    }

    //todo: the usage of this api
    @Override
    public void setWorkingDirectory(Path path) {
        LOG.debug("setWorkingDirectory({})", path);
        if (path.isAbsolute()) {
            mWorkingDir = path;
        } else {
            mWorkingDir = new Path(mWorkingDir, path);
        }
    }
}
