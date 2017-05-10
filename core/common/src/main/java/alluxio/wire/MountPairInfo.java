package alluxio.wire;

/**
 * Created by 17030105 on 2017/5/10.
 */

import com.google.common.base.Objects;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.Serializable;

/**
 * Created by 17030105 on 2017/4/7.
 */
@NotThreadSafe
public class MountPairInfo implements Serializable {
  public static final long serialVersionUID = 7119966306934831779L;
  private String alluxioPath = "";
  private String ufsPath = "";

  public MountPairInfo(){}

  protected MountPairInfo(alluxio.thrift.MountPairInfo mountPairInfo){
    alluxioPath = mountPairInfo.getAlluxioPath();
    ufsPath = mountPairInfo.getUfsPath();
  }

  /**
   * @return the alluxio path
   */
  public String getAlluxioPath(){
    return alluxioPath;
  }

  /**
   * @return the ufs path
   */
  public String getUfsPath(){
    return ufsPath;
  }

  /**
   * @param path alluxio path to use
   * @return the MountPairInfo
   */
  public MountPairInfo setAlluxioPath(String path){
    alluxioPath = path;
    return this;
  }

  /**
   * @param path ufs path to use
   * @return the MountPairInfo
   */
  public MountPairInfo setUfsPath(String path){
    ufsPath = path;
    return this;
  }

  /**
   * @return thrift representation of the MountPairInfo
   */
  protected alluxio.thrift.MountPairInfo toThrift(){
    alluxio.thrift.MountPairInfo info = new alluxio.thrift.MountPairInfo(alluxioPath,ufsPath);
    return info;
  }

  @Override
  public boolean equals(Object o){
    if(this == o){
      return true;
    }
    if (!(o instanceof MountPairInfo)){
      return false;
    }
    MountPairInfo that = (MountPairInfo) o;
    return ufsPath.equals(that.ufsPath) && alluxioPath.equals(that.alluxioPath);
  }

  @Override
  public int hashCode(){
    return Objects.hashCode(alluxioPath,ufsPath);
  }

  @Override
  public String toString(){
    return Objects.toStringHelper(this).add("alluxioPath", alluxioPath)
        .add("ufsPath", ufsPath).toString();
  }

}
