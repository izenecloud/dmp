#!/usr/bin/env ruby

# Process Pig output to match PigUnit format

out = File.open('a.txt','w')
puts "processing #{ARGV[0]} ..."
File.open(ARGV[0]).each do |line|
  out.puts line.split.join(',').prepend('(').concat(')')
end
puts "output written to #{out.path}"
