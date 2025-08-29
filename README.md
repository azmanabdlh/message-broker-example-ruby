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

### 2. Route Topic
```ruby
MQ::Application.consumer.draw do
  topic "hello", to: "HelloWorld"
end
```

### 3. Start the Consumer
```ruby
begin
  MQ::Application.consumer.start
rescue
  MQ::Application.consumer.shutdown
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

## Requirements
- Ruby 2.5+
- No external gems required for the basic example

## Running
1. Save your code in `main.rb`.
2. Run:
   ```sh
   ruby main.rb
   ```

---
Feel free to modify and extend for your own broker logic!
