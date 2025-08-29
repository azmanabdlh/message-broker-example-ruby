module MQ

  class Topic
    attr_reader :name, :channel, :consumer

    def initialize(name, consumer)
      @name = name.to_s
      @channel = ""
      @consumer = consumer
      @config = {}
    end

    def config(**options)
      @config.merge!(options)
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

    def topic(name, **option, &block)
      consumer(option[:to]) if option.key?(:to)

      if block_given?
        instance_eval(&block)
      end

      @route.add(name, Topic.new(name, @consumer))
    end

    def consumer(respond)
      respond = respond.to_s if respond.is_a?(Symbol)
      @consumer = ::Object.const_get(respond)
    end

    def collection
      @route.all
    end
  end

  class Listener
    def initialize(name, route)
      @name = name
      @route = route
    end

    def start
      @consumer = create_consumer
      @consumer.subscribe(@name) do | message |
        @route.each do |topic|
          klass = topic.consumer.new
          klass.respond(message)
        end
      end
    end

    private
    def create_consumer
      consumer = Struct.new do
        def subscribe(name, &block)
          # Simulate receiving fake messages
          messages = []
          100.times do |i|
            messages << "test message #{i} for topic #{name}"
          end
          messages.each do |msg|
            block.call(msg)
          end
        end
      end
      consumer.new
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
    end

    def shutdown; end
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
    puts message
  end
end

MQ::Application.consumer.draw do
  # topic "", to: ""
  topic "hello" do
    consumer "HelloWorld"
  end
end

begin
  MQ::Application.consumer.start
rescue
  MQ::Application.consumer.shutdown
end