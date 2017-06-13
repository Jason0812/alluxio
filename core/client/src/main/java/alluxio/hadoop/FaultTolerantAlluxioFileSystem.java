package alluxio.hadoop;

import alluxio.Constants;
import alluxio.PropertyKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by 17030105 on 2017/6/13.
 */
public class FaultTolerantAlluxioFileSystem extends DelegateToFileSystem {
	private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

	/**
	 * This constructor has the signature needed by
	 * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}
	 * in Hadoop 2.x.
	 *
	 * @param uri  the uri for this Alluxio filesystem
	 * @param conf Hadoop configuration
	 * @throws IOException        if an I/O error occurs
	 * @throws URISyntaxException if <code>uri</code> has syntax error
	 */
	FaultTolerantAlluxioFileSystem(final URI uri, final Configuration conf)
			throws IOException, URISyntaxException {
		super(uri, new FaultTolerantFileSystem(), conf, Constants.SCHEME_FT, false);
	}

	@Override
	public int getUriDefaultPort() {
		return Integer.parseInt(PropertyKey.MASTER_RPC_PORT.getDefaultValue());
	}
}
