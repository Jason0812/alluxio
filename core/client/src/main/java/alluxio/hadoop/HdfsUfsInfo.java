package alluxio.hadoop;

import org.apache.hadoop.fs.Path;

/**
 * Created by guoyejun on 2017/5/19.
 * This class constructs the ufsPath and HDFS filesystem instance according to
 * the alluxio path and mountTable
 */
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
