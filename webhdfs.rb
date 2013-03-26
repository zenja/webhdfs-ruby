require 'net/http'
require 'json'
require 'uri'

class WebHDFS
  CONTEXT_ROOT="/webhdfs/v1"

  attr_accessor :host, :port, :user, :address

  def initialize(namenode_host, namenode_port, hdfs_username)
    @port    = namenode_port
    @host    = namenode_host
    @user    = hdfs_username
    @address = "http://#{@host}:#{@port}"
  end

  def listdir(path)
    uri = generate_uri(path, 'op=LISTSTATUS')
    req = Net::HTTP::Get.new(uri.request_uri)
    res = do_request(req)
    JSON.parse(res.body)
  end

  def mkdir(path)
    uri = generate_uri(path, 'op=MKDIRS')
    req = Net::HTTP::Put.new(uri.request_uri)
    do_request(req)
  end

  def rmdir(path)
    uri = generate_uri(path, 'op=DELETE&recursive=true')
    req = Net::HTTP::Delete.new(uri.request_uri)
    do_request(req)
  end

  def copy_from_local(source_path, target_path, replication=false)
    uri = generate_uri(target_path, 'op=CREATE&overwrite=true')
    req = Net::HTTP::Put.new(uri.request_uri)
    res = do_request(req)

    raise "Error while creating file, redirect not received" unless res.class == Net::HTTPTemporaryRedirect
    location          = URI(res['location'])
    location.hostname = host
    begin
      req                      = Net::HTTP::Put.new(location.request_uri)
      req['Content-Type']      = 'application/octet-stream'
      req['Transfer-Encoding'] = 'chunked'
      req.body_stream          = File.open(File.expand_path source_path)
      do_request(req, location.hostname, location.port)
    rescue => e
      raise "Error while trying to upload file #{source_path} to #{location}:\n#{e.message}"
    end
  end

  def copy_to_local(source_path, target_path)
    uri      = generate_uri(source_path, 'op=OPEN')
    req      = Net::HTTP::Get.new(uri.request_uri)
    res      = do_request(req)
    location = URI(res['location'])

    location.hostname = host

    if (res.class != Net::HTTPTemporaryRedirect) && (res.class != Net::HTTPForbidden)
      raise "Response type is #{res.class} instead of Net::HTTPTemporaryRedirect"
    elsif res.class == Net::HTTPForbidden
      begin
        emptyfile = File.new(target_path, 'w')
        return
      rescue
        raise "Error while creating empty file #{target_path}:\n#{e.message}"
      ensure
        emptyfile.close
      end
    end

    begin
      file = File.open(target_path, "wb")
      req  = Net::HTTP::Get.new(location.request_uri)

      Net::HTTP.start(location.hostname, location.port) do |http|
        http.request(req) do |res|
          assert_response(res)
          res.read_body { |chunk| file << chunk }
        end
      end
    rescue => e
      raise "Error while downloading file from #{location}:\n#{e.message}"
    ensure
      file.close
    end
  end

  private

  def generate_uri(path, params)
    URI("#{address + CONTEXT_ROOT + path}?#{params}&user.name=#{user}")
  end

  def do_request(req, h=host, p=port)
    res = Net::HTTP.new(h, p).request(req)
    assert_response(res)
  end

  def assert_response(res)
    case res
    when Net::HTTPNotFound then
      raise "Resource not found: #{JSON.parse(res.body)}"
    when Net::HTTPBadRequest then
      raise "Invalid request: #{JSON.parse(res.body)}"
    when Net::HTTPConflict then
      raise "Conflict error: #{JSON.parse(res.body)}"
    when Net::HTTPForbidden then
      raise "Not allowed: #{JSON.parse(res.body)}"
    else
      res
    end
  end
end

# ----- Examples -----
# require './webhdfs.rb'
# webhdfs = WebHDFS.new('localhost', 50070, 'wangxing')
# dirname = '/testdir'
# res = webhdfs.mkdir(dirname)
# res = webhdfs.rmdir(dirname)
# res = webhdfs.listdir('/')
# res = webhdfs.copy_from_local('/etc/hosts', '/user/wangxing/hosts.txt')
# res = webhdfs.rmdir('/user/wangxing/hosts.txt')
# res = webhdfs.copy_to_local('/user/wangxing/emptyfile', '/home/wangxing/temp/testfile')
# res = webhdfs.copy_to_local('/user/wangxing/input/core-site.xml', '/home/wangxing/temp/testxml')
