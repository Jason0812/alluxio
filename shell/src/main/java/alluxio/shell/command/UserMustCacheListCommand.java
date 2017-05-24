package alluxio.shell.command;

import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;

/**
 * Created by guoyejun on 2017/5/24.
 */
public final class UserMustCacheListCommand extends AbstractShellCommand{

	public UserMustCacheListCommand(FileSystem fs) { super(fs); }

	@Override
	public String getCommandName() {
		return "MCL";
	}

	@Override
	public int getNumOfArgs() { return 1; }

	private void userMustCacheList(boolean isGet, boolean isRefresh) throws AlluxioException, IOException {
		if (isGet) {
			List<String> userMustCacheList = mFileSystem.getUserMustCacheList();
			for (String userMustCachePath: userMustCacheList) {
				System.out.println(userMustCachePath);
			}
		}

		if (isRefresh) {
			mFileSystem.refreshUserMustCacheList();
		}
	}

	@Override
	public void run(CommandLine cl) throws AlluxioException, IOException {
		String[] args = cl.getArgs();
		boolean isGet = true;
		boolean isRefresh = false;
		for (int i = 0; i < args.length; i++){
			if (args[i].equals("refresh")) {
				isRefresh = true;
				isGet = false;
			}
		}
		userMustCacheList(isGet, isRefresh);
	}

	@Override
	public String getUsage() {
		return "MCL [get] [refresh]";
	}

	@Override
	public String getDescription() {
		return "Display and refresh UserMustCacheList. "
			+ "Specify get to display userMustCacheList."
			+ "Specify refresh to refresh the userMustCacheList in Alluxio Master";
	}

}
