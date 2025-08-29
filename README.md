# Message Broker Example (Ruby)

This is a simple example of a message broker pattern in Ruby using a custom route drawing DSL and async simulation.

## Example Usage

### 1. Define a Responder Class
```ruby
class HelloWorld
  def respond(message)
    puts message
  end
end
```

### 2. Register Topic and Consumer
```ruby
NSQ::Application.consumer.draw do
  topic "hello" do
    consumer "HelloWorld"
  end
end
```

### 3. Start the Consumer
```ruby
begin
  NSQ::Application.consumer.start
rescue
  NSQ::Application.consumer.shutdown
end
```

### 4. Output
The consumer will print messages like:
```
test message 0 for topic hello
test message 1 for topic hello
...
test message 99 for topic hello
```

## How It Works
- The `Listener` class simulates receiving 100 messages for each topic.
- Each message is passed to the registered consumer's `respond` method.
- You can add more topics and consumer classes as needed.

## Requirements
- Ruby 2.5+
- No external gems required for the basic example

## Running
1. Save your code in `main.rb`.
2. Run:
   ```sh
   ruby main.rb
   ```

## Customization
- Add more topics and consumer classes to handle different message types.
- Integrate with real async/message queue libraries for production use.

---
Feel free to modify and extend for your own broker logic!
