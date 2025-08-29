module MQ
  class Topic
    attr_reader :name, :channel

    def initialize(name, consumer)
      @name = name.to_s
      @channel = ""
      @consumer = consumer
      @config = {}
    end

    def config(**options)
      @config.merge!(options)
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
      unless klass.end_with?("Responder")
        raise NameError, "Consumer class name '#{klass}' must end with 'Responder'"
      end

      klass
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
    def initialize(name, route)
      @name = name
      @route = route
      @mq_consumer_thread = nil
      @worker = WorkerPool.new
    end

    def start
      @worker.start
      consumer = create_consumer
      return unless consumer.respond_to?(:subscribe)

      consumer.subscribe(@name) do |message|
        @route.each do |topic|
          klass = topic.consumer.new
          klass.respond(message)
        end
      end
    end

    def stop
      if @mq_consumer_thread&.alive?
        @mq_consumer_thread.join
        @mq_consumer_thread.kill
      end
      @mq_consumer_thread = nil

      @worker.stop
    end

    private
    def create_consumer
      consumer = Struct.new(:worker) do
        def subscribe(name, &block)
          start_mq_consumer(name, &block)
        end

        def start_mq_consumer(name, &block)
          @mq_consumer_thread = Thread.new do
            begin
              # simulate fake messages
              messages = []

              10.times do |i|
                messages << "test message #{i} for topic #{name}"
              end

              messages.each do |message|
                job = lambda do
                  yield message
                end

                worker.push(job)
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
      consumer.new(@worker)
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

  class Application
    class << self
      def consumer
        @consumer ||= Consumer.new
      end
    end
  end
end


class HelloWorld
  def respond(message)
    puts "HelloWorld: #{message}"
  end
end

MQ::Application.consumer.draw do
  topic :hello, to: :hello_world
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