require 'net/http'
require 'json'
require 'rest_client'
require 'open-uri'

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
      RestClient.put redirect_location, :myfile => File.new(source_path, 'rb') # use 'rb' for both text or binary file
      return make_result(is_successful = true)
    rescue Exception => e
      return make_result(is_successful = false, 
                         message = "Error when trying to upload the file #{source_path} to #{redirect_location}", 
                         body = {})
    end
  end

  def copy_to_local(source_path, target_path)
    url_path = @url_prefix + WEBHDFS_CONTEXT_ROOT + source_path + '?op=OPEN&user.name=' + @username
    uri = URI(url_path)
    # Note: uri.path and uri.request_uri are different, 
    # uri.request_uri is uri.path plus request params
    req = Net::HTTP::Get.new(uri.request_uri)

    res = Net::HTTP.start(uri.hostname, uri.port) do |http|
      http.request(req)
    end

    if (res.class != Net::HTTPTemporaryRedirect) && (res.class != Net::HTTPForbidden) then
      return make_result(is_successful = false, 
                       message = "response type is #{res.class.to_s} insdead of Net::HTTPTemporaryRedirect", 
                       body = {})
    # if target file is empty, we will get a Net::HTTPForbidden
    elsif res.class == Net::HTTPForbidden then
      begin
        # create an empty file
        emptyfile = File.new(target_path, 'w')
        return make_result(is_successful = true)
      rescue
        return make_result(is_successful = false, message = "Error when creating empty file #{target_path}")
      ensure
        emptyfile.close
      end
    end

    redirect_location = res['location']

    # download file
    begin
      File.open(target_path, "wb") do |saved_file|
        # the following "open" is provided by open-uri
        open(redirect_location) do |read_file|
          saved_file.write(read_file.read)
        end
      end
      return make_result(is_successful = true, message = "HDFS file #{source_path} downloaded")
    rescue Exception => e
      return make_result(is_successful = false, 
                         message = "Error when downloading file from #{redirect_location}. Msg: #{e.message}")
    end
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
