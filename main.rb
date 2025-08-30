require "yaml"

require "nsq"

module MQ

  class ResponderConfig
    def initialize(mq_config)
      @mq_config = mq_config
    end

    def max_in_flight
      @mq_config["max_in_flight"]
    end
  end

  class Topic
    attr_reader :name, :channel, :config

    def initialize(name, channel, consumer)
      @name = name.to_s
      @channel = "test"
      @consumer = consumer
      @config = {}
    end

    def consumer
      begin
        klass = resolve_consumer_class_name
        ::Object.const_get(klass)
      rescue NameError
        # log error
      end
    end

    def configure=(obj)
      @config = obj
    end


    private
    def resolve_consumer_class_name
      klass = @consumer.to_s.split('_').map(&:capitalize).join
      "#{klass}Responder"
    end

  end

  class RouteSet

    attr_reader :topics

    def initialize
      @topics = []
    end

    def middleware
      # TODO: setup middleware
    end

    def topic(name, **option)
      consumer = option[:to] if option.key?(:to)
      raise ArgumentError, "missing required :to option for topic '#{name}'" if consumer.nil?

      channel = option[:channel] or "test"

      topic = Topic.new(name, channel, consumer)

      @topics << topic
    end
  end

  class WorkerPool
    attr_reader :jobs

    def initialize(size: 3, max_queue_size: 1000)
      @size = size
      @max_queue_size = max_queue_size
      @jobs = []
      @queue = SizedQueue.new(max_queue_size)
    end

    def push(job)
      @queue.push(job)
    end

    def create_job(worker_id)
      Thread.new do
        Thread.current.name = "worker-#{worker_id}"

        loop do
          job = @queue.pop
          break if job == :stop
          job.call
        end
      end
    end

    def start
      @size.times do |id|
        @jobs << create_job(id)
      end
    end

    def stop
      # send signal stop to worker
      @size.times { @queue.push(:stop) }
      # wait for all jobs to finish
      @jobs.each(&:join)
      @jobs.clear
    end
  end

  class Listener

    def initialize(topic)
      @mq_consumer_thread = nil
      @worker = WorkerPool.new

      @nsq_consumer = Nsq::Consumer.new(
        nsqlookupd: "127.0.0.1:4161",
        topic: topic.name,
        channel: topic.channel,
        max_in_flight: topic.config.max_in_flight,
      )

      # start listener
      @worker.start
      subscribe(topic)
    end

    def stop
      if @mq_consumer_thread&.alive?
        @mq_consumer_thread.kill
        @mq_consumer_thread.join
      end
      @mq_consumer_thread = nil

      @worker.stop
      @nsq_consumer.terminate
      @nsq_consumer = nil
    end

    private
    def subscribe(topic)
      @mq_consumer_thread = Thread.new do
        start_mq_consumer(topic)
      end
    end

    def start_mq_consumer(topic)
      begin

        # simulate fake messages
        # messages = []

        # 100.times do |i|
        #   messages << "test message #{i} for topic #{topic.name}"
        # end

        # messages.each do |message|
        #   job = lambda do
        #     klass = topic.consumer
        #     consumer = klass.new
        #     consumer.respond(message)
        #   end

        #   @worker.push(job)
        # end
        #

        loop do
          raise "disconnected nsqd server" if @nsq_consumer.nil?

          msg = @nsq_consumer.pop_without_blocking
          next if msg.nil?

          klass = topic.consumer
          consumer = klass.new

          job = lambda do
            consumer.respond(msg)
          end
          @worker.push(job)
        end
      rescue => e
        # log error
        puts "error consumer #{e.message}"
      end
    end
  end

  class Consumer

    def initialize
      @listeners = []
    end

    def draw(&block)
      @route = RouteSet.new
      @route.instance_eval(&block)
    end

    def start
      mq_config = setup_configure
      @listeners = @route.topics.map do |topic|
        topic.configure = mq_config.find_by_name(topic.name)

        Listener.new(topic)
      end
    end

    def shutdown
      return if @listeners.empty?
      @listeners.each(&:stop)
    end

    private
    def setup_configure(path: "./mq.yaml")
      config = Struct.new do
        def initialize(path)
          @config = YAML.load_file(path)
        end
        def find_by_name(name)
          default_setup unless @config.key?(name)
          ResponderConfig.new(
            @config[name]
          )
        end

        def default_setup
          {}
        end

        def reload!
          @config.reload!
        end
      end

      config.new(path)
    end
  end

  class Server
    class << self
      def listen
        begin
          Application.consumer.start

          stop = false
          trap("INT")  { stop = true }
          trap("TERM") { stop = true }

          until stop
            sleep 1
          end
        rescue => e
          puts "server error: #{e.message}"
          Application.consumer.shutdown
        end
      end
    end

  end


  class Application
    class << self
      def consumer
        @consumer ||= Consumer.new
      end
    end
  end
end


class WelcomeResponder
  def respond(msg)
    puts "HelloWorld: #{msg.body}"

    msg.finish
  end
end

MQ::Application.consumer.draw do
  topic :hello, channel: "test", to: :welcome
end


MQ::Server.listen

# example worker

# worker = MQ::WorkerPool.new

# worker.start

# 10.times do |i|
#   job = lambda do
#     puts "hello world => #{i}"
#   end
#   worker.push(job)
# end

# until worker.jobs.all? { |t| !t.alive? }
# end