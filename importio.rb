'''

import.io client library

Dependencies=> Ruby 1.9, http-cookie

@author=> import.io
'''

require "net/http"
require "uri"
require "thread"
require "http-cookie"
require "cgi"
require "json"
require "securerandom"

class Query

  def initialize(callback, query)
    @query = query
    @jobsSpawned = 0
    @jobsStarted = 0
    @jobsCompleted = 0
    @_finished = false
    @_callback = callback
  end

  def _onMessage(data)

    msgType = data["type"]
    if msgType == "SPAWN"
      @jobsSpawned+=1
    elsif msgType == "INIT" or msgType == "START"
      @jobsStarted+=1
    elsif msgType == "STOP"
      @jobsCompleted+=1
    end

    @_finished = (@jobsStarted == @jobsCompleted and @jobsSpawned + 1 == @jobsStarted and @jobsStarted > 0)

    # if there is an error or the user is not authorised correctly then allow isFinished to return true by setting jobs to -1
    if msgType == "ERROR" or msgType == "UNAUTH" or msgType == "CANCEL"
      @_finished = True
    end

    @_callback.call(self, data)
  end

  def finished
    return @_finished
  end
end

class ImportIO

  def initialize(userId=nil, apiKey=nil, host="https://query.import.io")
    @host = host
    @proxyHost = nil
    @proxyPort = nil
    @msgId = 1
    @clientId = nil
    @url = "#{host}/query/comet/"
    @messagingChannel = "/messaging"
    @queries = Hash.new
    @userId = userId
    @apiKey = apiKey
    @cj = HTTP::CookieJar.new
    @queue = Queue.new
    @connected = false
  end

  def proxy(host,port)
    @proxyHost = host
    @proxyPort = port
  end

  def makeRequest(url, data)
    uri = URI(url)
    request = Net::HTTP::Post.new(uri.request_uri)
    request.body = data
    http = Net::HTTP.new(uri.host, uri.port, @proxyHost, @proxyPort)
    http.use_ssl = uri.scheme == "https"
    return uri, http, request
  end

  def open(uri, http, request)
    response = http.request(request)
    cookies = response.get_fields("set-cookie")
    if cookies != nil
      cookies.each { |value|
        @cj.parse(value, uri)
      }
    end
    return response
  end

  def encode(dict)
    dict.map{|k,v| "#{CGI.escape(k)}=#{CGI.escape(v)}"}.join("&")
  end

  def login(username, password, host="https://api.import.io")
    data = encode({'username' => username, 'password'=> password})
    uri, http, req = makeRequest("#{host}/auth/login", data )
    r = open(uri, http, req)

    if r.code != "200"
      raise "Could not log in, code #{r.code}"
    end
  end

  def request(channel, path="", data={}, throw=true)

    # add in the common values
    data["channel"] = channel
    data["connectionType"] = "long-polling"
    data["id"] = @msgId
    @msgId += 1

    if @clientId != nil
      data["clientId"] = @clientId
    end

    url = "#{@url}#{path}"

    if @apiKey != nil
      q = encode({ "_user" => @userId, "_apikey" => @apiKey })
      url = "#{url}?#{q}"
    end

    body = JSON.dump([data])
    uri, http, request = makeRequest(url, body)
    request.content_type = "application/json;charset=UTF-8"
    request["Cookie"] = HTTP::Cookie.cookie_value(@cj.cookies(uri))

    response = open(uri, http, request)
    if response.code != "200"
      raise "Connect failed, status #{response.code}"
    end

    response.body = JSON.parse(response.body)

    for msg in response.body do

      if msg.has_key?("successful") and msg["successful"] != true
        msg = "Unsuccessful request=> %s", msg
        if throw
          raise msg
        else
          print msg
        end

        next
      end

      if msg["channel"] != @messagingChannel
        next
      end

      @queue.push(msg["data"])

    end

    return response

  end

  def handshake
    handshake = request("/meta/handshake", path="handshake", data={"version"=>"1.0","minimumVersion"=>"0.9","supportedConnectionTypes"=>["long-polling"],"advice"=>{"timeout"=>60000,"interval"=>0}})
    @clientId = handshake.body[0]["clientId"]
  end

  def connect
    if @connected
      return
    end
    handshake

    request("/meta/subscribe", "", {"subscription"=>@messagingChannel})

    @connected = true

    @threads = []
    @threads << Thread.new(self) { |io|
      io.poll
    }
    @threads << Thread.new(self) { |io|
      io.pollQueue
    }
  end

  def disconnect
    request("/meta/disconnect");
    @connected = false
  end

  def stop
    @threads.each { |thread|
      thread.terminate
    }
  end

  def join
    while @connected
      if @queries.length == 0
        stop
        return
      end
      sleep 1
    end
  end

  def pollQueue
    while @connected
      begin
        processMessage @queue.pop
      rescue => exception
        puts exception.backtrace
      end
    end
  end

  def poll
    while @connected
      request("/meta/connect", "connect", {}, false)
    end
  end

  def processMessage(data)
    begin
      reqId = data["requestId"]
      query = @queries[reqId]

      if query == nil
        puts "No open query #{query}:"
        puts JSON.pretty_generate(data)
        return
      end

      query._onMessage(data)
      if query.finished
        @queries.delete(reqId)
      end
    rescue => exception
      puts exception.backtrace
    end
  end

  def query(query, callback)
    query["requestId"] = SecureRandom.uuid
    @queries[query["requestId"]] = Query::new(callback, query)
    request("/service/query", "", { "data"=>query })
  end

end
