require 'net/http'

class WebHDFS
  def initialize(namenode_host, namenode_port, hdfs_username)
    @namenode_port = namenode_port
    @namenode_host = namenode_host
    @hdfs_username = hdfs_username
  end
  
  def mkdir(path)
  end

  def rmdir(path)
  end

  def copy_from_local(source_path, target_path, replication=1)
  end

  def copy_to_local(source_path, target_path)
  end

  def listdir(path)
  end
end
