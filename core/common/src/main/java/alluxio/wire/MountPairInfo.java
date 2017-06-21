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
  private String mAlluxioPath =null;
  private String mUfsPath = null;

  private MountPairInfo(){}

  protected MountPairInfo(alluxio.thrift.MountPairInfo mountPairInfo){
    mAlluxioPath = mountPairInfo.getAlluxioPath();
    mUfsPath = mountPairInfo.getUfsPath();
  }

  public MountPairInfo(String alluxioPath, String ufsPath){
    mAlluxioPath = alluxioPath;
    mUfsPath = ufsPath;
  }

  /**
   * @return the alluxio path
   */
  public String getAlluxioPath(){
    return mAlluxioPath;
  }

  /**
   * @return the ufs path
   */
  public String getUfsPath(){
    return mUfsPath;
  }

  /**
   * @param path alluxio path to use
   * @return the MountPairInfo
   */
  public MountPairInfo setAlluxioPath(String path){
    mAlluxioPath = path;
    return this;
  }

  /**
   * @param path ufs path to use
   * @return the MountPairInfo
   */
  public MountPairInfo setUfsPath(String path){
    mUfsPath = path;
    return this;
  }

  /**
   * @return thrift representation of the MountPairInfo
   */
  protected alluxio.thrift.MountPairInfo toThrift(){
    alluxio.thrift.MountPairInfo info = new alluxio.thrift.MountPairInfo(mAlluxioPath, mUfsPath);
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
    return mUfsPath.equals(that.mUfsPath) && mAlluxioPath.equals(that.mAlluxioPath);
  }

  @Override
  public int hashCode(){
    return Objects.hashCode(mAlluxioPath, mUfsPath);
  }

  @Override
  public String toString(){
    return Objects.toStringHelper(this).add("mAlluxioPath", mAlluxioPath)
        .add("mUfsPath", mUfsPath).toString();
  }

}
