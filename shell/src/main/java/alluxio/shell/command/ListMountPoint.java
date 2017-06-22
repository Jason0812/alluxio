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
  public void ListMountPoint() throws AlluxioException,IOException{
    for(MountPairInfo mountPairInfo: mFileSystem.getMountPoint()){
      System.out.println(mountPairInfo.getAlluxioPath() + " -> " + mountPairInfo.getUfsPath());
    }
  }
  @Override
  public int getNumOfArgs(){
    return 0;
  }

  @Override
  public void run(CommandLine cl) throws AlluxioException, IOException{
    ListMountPoint();
  }

  @Override
  public String getCommandName(){
    return "listMountPoint";
  }

  @Override
  public String getDescription(){
    return "list Mount Point Info";
  }

  @Override
  public String getUsage(){
    return "listMountPoint";
  }
}