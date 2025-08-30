module MQ
  class Topic
    attr_reader :name, :channel

    def initialize(name, consumer)
      @name = name.to_s
      @channel = ""
      @consumer = consumer
    end

    def consumer
      begin
        klass = resolve_consumer_class_name
        ::Object.const_get(klass)
      rescue NameError
        # log error
      end
    end

    private
    def resolve_consumer_class_name
      klass = @consumer.to_s.split('_').map(&:capitalize).join
      "#{klass}Responder"
    end

  end


  class NamedRouteCollection
    def initialize
      @routes = {}
    end

    def add(name, topic)
      @routes[name.to_s] ||= []
      @routes[name.to_s] << topic
    end

    def [](name)
      @routes[name.to_s]
    end

    def all
      @routes
    end
  end

  class RouteSet
    def initialize
      @route = NamedRouteCollection.new
    end

    def middleware
      # TODO: setup middleware
    end

    def topic(name, **option)
      consumer = option[:to] if option.key?(:to)
      raise ArgumentError, "missing required :to option for topic '#{name}'" if consumer.nil?

      topic = Topic.new(name, consumer)

      @route.add(name, topic)
    end

    def collection
      @route.all
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
    def initialize(topic_name, route)
      @topic_name = topic_name
      @route = route
      @mq_consumer_thread = nil
      @worker = WorkerPool.new
    end

    def start
      @worker.start

      subscribe(@topic_name)
    end

    def stop
      if @mq_consumer_thread&.alive?
        @mq_consumer_thread.kill
        @mq_consumer_thread.join
      end
      @mq_consumer_thread = nil

      @worker.stop
    end

    private
    def subscribe(topic_name)
      @route.each do |topic|
        start_mq_consumer(topic_name, topic)
      end
    end

    def start_mq_consumer(topic_name, topic)
      @mq_consumer_thread = Thread.new do
        begin

          # simulate fake messages
          messages = []

          100.times do |i|
            messages << "test message #{i} for topic #{topic_name}"
          end

          messages.each do |message|
            job = lambda do
              klass = topic.consumer
              consumer = klass.new

              klass.define_method(:message) { message }

              consumer.respond
            end

            @worker.push(job)
          end
          # or
          #
          # nsq_consumer = Nsq::Consumer.new(
          #   nsqlookupd: ['127.0.0.1:4161'],
          #   topic: @name,
          #   channel: '...'
          # )
          # nsq_consumer.on_message do |message|
          #   job = lambda do
          #     yield message
          #   end
          #   @worker.push(job)
          # end
        rescue => e
          # log error
          puts "error consumer #{e.message}"
        end
      end
    end
  end

  class Consumer
    def draw(&block)
      @route = RouteSet.new
      @route.instance_eval(&block)
    end

    def start
      @listeners = @route.collection.map do |topic_name, route|
        Listener.new(topic_name, route)
      end

      @listeners.each(&:start)

      stop = false
      trap("INT")  { stop = true }
      trap("TERM") { stop = true }

      until stop
        sleep 1
      end
    end

    def shutdown
      @listeners.each(&:stop)
    end
  end

  class ResponderConfig; end

  class Responder
    module ClassMethods
      def configure(&block)
        @config = ResponderConfig.new
        @config.instance_eval(&block) if block_given?

        @config.to_h
      end
    end

    extend ClassMethods

    def message; end
  end

  class Application
    class << self
      def consumer
        @consumer ||= Consumer.new
      end
    end
  end
end


class WelcomeResponder < MQ::Responder
  def respond
    puts "HelloWorld: #{message}"
  end
end

MQ::Application.consumer.draw do
  topic :hello, to: :welcome
end


begin
  MQ::Application.consumer.start
rescue
  MQ::Application.consumer.shutdown
end


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