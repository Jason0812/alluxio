package alluxio.shell.command;

import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.wire.MountPairInfo;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;

/**
 * Created by 17030105 on 2017/4/11.
 */
public final class ListMountPoint extends AbstractShellCommand{
  public ListMountPoint(FileSystem fs){
    super(fs);
  }

  /**
   * list mount point
   * @throws AlluxioException
   * @throws IOException
   */
  public void listMountPoint() throws AlluxioException,IOException{
    for(MountPairInfo mountPairInfo: mFileSystem.getMountTable()){
      System.out.println(mountPairInfo.getAlluxioPath() + " -> " + mountPairInfo.getUfsPath());
    }
  }
  @Override
  public int getNumOfArgs(){
    return 0;
  }

  @Override
  public void run(CommandLine cl) throws AlluxioException, IOException{
    listMountPoint();
  }

  @Override
  public String getCommandName(){
    return "getMountTable";
  }

  @Override
  public String getDescription(){
    return "list Mount Point Info";
  }

  @Override
  public String getUsage(){
    return "getMountTable";
  }
}
