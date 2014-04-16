require './importio.rb'
require 'json'

client = ImportIO::new('1c4db3cd-bf64-45af-9ad8-d5ccb1df5115', '#APIKEY#')
client.connect
callback = lambda do |query, message|
  if message['type'] == 'MESSAGE'
    json = message['data']

    json['results'].each do |bounty|
      host = URI(bounty['company_link']).host
      name = bounty['company_link/_text']
      filename  = name.downcase.gsub(/[^0-9a-z ]/i, '')
      result = `nmap -d -o log/heartbleed_#{host}_%y%m%d --script ssl-heartbleed --script-args vulns.showall -sV --script-trace #{host}`
      if result.include? 'State: VULNERABLE'
        File.open('vulnerable/' + filename, 'wb') { |file| file.write(result) }
        puts host + ' is vulnerable'
      else
        puts host + ' is not vulnerable'
      end
    end
  end
end

# Query for tile bugcrowd_list
client.query({ 'input' => { 'webpage/url' => 'https://bugcrowd.com/list-of-bug-bounty-programs' }, 'connectorGuids' => ['61a3f7f7-c736-41b3-b8b3-b365f68255da'] }, callback)

client.join
