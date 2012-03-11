require 'net/http'
require 'json'
require 'rest_client'

class WebHDFS
  WEBHDFS_CONTEXT_ROOT="/webhdfs/v1"

  def initialize(namenode_host, namenode_port, hdfs_username)
    @namenode_port = namenode_port
    @namenode_host = namenode_host
    @username = hdfs_username
    @url_prefix = 'http://' + namenode_host + ':' + namenode_port.to_s
  end
  
  def mkdir(path)
    url_path = @url_prefix + WEBHDFS_CONTEXT_ROOT + path + '?op=MKDIRS&user.name=' + @username
    uri = URI(url_path)
    # Note: uri.path and uri.request_uri are different, 
    # uri.request_uri is uri.path plus request params
    req = Net::HTTP::Put.new(uri.request_uri)

    res = Net::HTTP.start(uri.hostname, uri.port) do |http|
      http.request(req)
    end

    case res
    when Net::HTTPSuccess, Net::HTTPRedirection
      return make_result(is_successful = true, message = '', body = JSON.parse(res.body))
    else
      return make_result(is_successful = false, message = '', body = JSON.parse(res.body))
    end
  end

  def rmdir(path)
    url_path = @url_prefix + WEBHDFS_CONTEXT_ROOT + path + '?op=DELETE&recursive=true&user.name=' + @username
    uri = URI(url_path)
    # Note: uri.path and uri.request_uri are different, 
    # uri.request_uri is uri.path plus request params
    req = Net::HTTP::Delete.new(uri.request_uri)

    res = Net::HTTP.start(uri.hostname, uri.port) do |http|
      http.request(req)
    end

    case res
    when Net::HTTPSuccess, Net::HTTPRedirection
      return make_result(is_successful = true, message = '', body = JSON.parse(res.body))
    else
      return make_result(is_successful = false, message = '', body = JSON.parse(res.body))
    end
  end

  def copy_from_local(source_path, target_path, replication=1)
    url_path = @url_prefix + WEBHDFS_CONTEXT_ROOT + target_path + '?op=CREATE&overwrite=true&user.name=' + @username
    uri = URI(url_path)
    # Note: uri.path and uri.request_uri are different, 
    # uri.request_uri is uri.path plus request params
    req = Net::HTTP::Put.new(uri.request_uri)

    res = Net::HTTP.start(uri.hostname, uri.port) do |http|
      http.request(req)
    end

    return make_result(is_successful = false, body = {}) if res.class != Net::HTTPTemporaryRedirect 

    redirect_location = res['location']
    puts redirect_location #debug
    
    # upload the target file using rest-client
    begin
      RestClient.put redirect_location, :myfile => File.new(source_path, 'r') #TODO: 'r' or 'rb' depends on file type?
      return make_result(is_successful = true)
    rescue Exception => e
      return make_result(is_successful = false, 
                         message = "Error when trying to upload the file #{source_path} to #{redirect_location}", 
                         body = {})
    end
  end

  def copy_to_local(source_path, target_path)
  end

  def listdir(path)
    url_path = @url_prefix + WEBHDFS_CONTEXT_ROOT + path + '?op=LISTSTATUS&user.name=' + @username
    uri = URI(url_path)
    # Note: uri.path and uri.request_uri are different, 
    # uri.request_uri is uri.path plus request params
    req = Net::HTTP::Get.new(uri.request_uri)

    res = Net::HTTP.start(uri.hostname, uri.port) do |http|
      http.request(req)
    end

    case res
    when Net::HTTPSuccess, Net::HTTPRedirection
      return make_result(is_successful = true, message = '', body = JSON.parse(res.body))
    else
      return make_result(is_successful = false, message = '', body = JSON.parse(res.body))
    end
  end

  private

  def make_result(is_successful, message="", body={})
    result = {'is_successful' => is_successful, 'message' => message, 'body' => body}
  end
end
