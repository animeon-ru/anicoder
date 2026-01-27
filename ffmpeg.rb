# frozen_string_literal: true

require 'pg'
require 'streamio-ffmpeg'
require 'redis'
require 'aws-sdk-s3'
require 'dotenv'
require 'logger'

file = File.open('/home/devops/transcoder.log', File::WRONLY | File::APPEND | File::CREAT)
file.sync = true
$stdout = file
$stderr = file
LOGGER = Logger.new(file, 'weekly')
LOGGER.level = Logger::DEBUG

Dotenv.load('/home/devops/animeon/.env')
Aws.config.update(
  credentials: Aws::Credentials.new(ENV['ACCESS_KEY_ID'], ENV['SECRET_ACCESS_KEY']),
  region: ENV['S3_REGION'],
  endpoint: ENV['S3_ENDPOINT'],
  force_path_style: true,
  signature_version: 'v4'
)
CLIENT = Aws::S3::Client.new
Aws.config.update(logger: Logger.new($stdout), log_level: :debug)

LOGGER.info('initialize') { "Initializing..." }
CONN = PG::Connection.new(host: ENV['DATABASE_HOST'], user: 'animeon', password: ENV['DATABASE_PASSWORD'], port: 54320, dbname: 'animeon_prod')
REDIS = Redis.new(host: '45.84.1.34', port: ENV['REDIS_PORT'])

REDIS.set("transcoder:status", "active")
REDIS.set("transcoder:iterations", "0")
REDIS.set("transcoder:videos", "0")
REDIS.set("transcoder:stop", "0")
REDIS.set("transcoder:current", "0")
REDIS.set("transcoder:current_time_start", "0")
def main()
  i = 0
  while i != -1
    LOGGER.debug("iteration - #{REDIS.get("transcoder:iterations")}")
    res = CONN.exec('SELECT * FROM videos WHERE status = 0 ORDER BY id LIMIT 1').first

    unless res.nil?
      id = res['id']
      LOGGER.debug("Find video with id = #{id}")
      REDIS.set("transcoder:current", "#{id}")
      format = res['video_file_file_name'].match('(\.mp4|\.avi|\.mkv|\.mov|\.ts)')
      system("sudo -u devops mkdir /transcoding/#{id}")
      LOGGER.debug("Starting to downloading video with id = #{id}")
      CLIENT.get_object(
        bucket: 'video',
        key: "#{id}/video-#{id}#{format}",
        response_target: "/transcoding/#{id}/video-#{id}#{format}"
      )
      movie = FFMPEG::Movie.new("/transcoding/#{id}/video-#{id}#{format}")
      if movie.valid?
        LOGGER.debug("Starting to transcoding video with id = #{id}")
        CONN.exec("UPDATE videos SET status = 1 WHERE id = #{id}")
        REDIS.set("transcoder:status", "transcoding")
        REDIS.set("transcoder:current_time_start", Time.now.to_s)
        time_start = Time.now
        system("sh /home/devops/transcode -i #{id} -f #{format.to_s.gsub('.', '')}")
        time_end = (Time.now - time_start).round(0)
        LOGGER.debug("Successfully transcoded video with id = #{id}")
        LOGGER.debug("Starting to uploading video with id = #{id}")
        system("aws s3 cp --recursive /transcoding/#{id} s3://video/#{id}")
        LOGGER.debug("Successfully uploaded video with id = #{id}")
        REDIS.set("transcoder:video:#{REDIS.get("transcoder:videos_all_time")}:time",
                  time_end.to_s)
        REDIS.set("transcoder:video:#{REDIS.get("transcoder:videos_all_time")}:id",
                  id.to_s)
        REDIS.set("transcoder:videos_all_time_transcoding_time",
                  "#{(REDIS.get("transcoder:videos_all_time_transcoding_time").to_i + time_end)}")
      end
      CONN.exec("UPDATE videos SET status = 2 WHERE id = #{id}")
      REDIS.incr("transcoder:videos")
      REDIS.incr("transcoder:videos_all_time")
      REDIS.set("transcoder:current_time_start", "0")
      REDIS.set("transcoder:current", "0")
      REDIS.set("transcoder:status", "active")
      #system("rm -r /transcoding/#{id}")
    end
    sleep(5)
    REDIS.get("transcoder:stop") == "1" ? i = -1 : i += 1
    REDIS.incr("transcoder:iterations")
  end
rescue => err
  REDIS.set("transcoder:status", "error")
  LOGGER.fatal("Caught exception; exiting")
  LOGGER.fatal(err)
end

main

REDIS.set("transcoder:status", "stopped")
