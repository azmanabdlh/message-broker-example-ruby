# Message Broker Example (Ruby)

This project demonstrates a simple message broker pattern in Ruby, featuring topic-to-consumer mapping, worker pool concurrency, and simulated message delivery.


## Example Usage

### 1. Define a Responder Class
```ruby
class WelcomeResponder
  def respond(message)
    puts "HelloWorld: #{message}"
  end
end
```

### 2. Register Topic and Consumer
```ruby
MQ::Application.consumer.draw do
  topic :hello, channel: :test, to: :welcome
end
```

### 3. Start the Consumer
```ruby
MQ::Server.start
```

### 4. Output
The consumer will print messages like:
```
HelloWorld: test message 0 for topic hello
HelloWorld: test message 1 for topic hello
...
HelloWorld: test message 99 for topic hello
```

## How It Works
- Each topic is mapped to a responder class (consumer).
- The worker pool processes messages concurrently.
- The listener simulates message delivery to the responder's `respond` method.
- Configuration can be loaded from a YAML file for advanced setups.

## Requirements
- Ruby 2.5+
- No external gems required for the basic example

## Running
1. Save your code in `main.rb`.
2. Run:
   ```sh
   make ruby-consumer
   ```

Feel free to modify and extend for your own broker logic!
