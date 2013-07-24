-- Code adapted from http://www.lua.org/pil/11.4.html
require "lanes"

Queue = {}

local DEBUG = false

-- Creates new empty queue
function Queue.new()
	return {first = 0, last = -1}
end

-- Enqueues value onto queue
function Queue.enqueue(queue, value)
	if (value == nil) then return nil end  -- Do not enqueue nil values
    	local last = queue.last + 1
	queue.last = queue.last + 1
	queue[last] = value
	if (DEBUG) then print("Inserindo " .. value .. " em " .. queue.last) end
	return value
end

-- Dequeues first element
function Queue.dequeue(queue)
	local first = queue.first
    	if first > queue.last then return nil	end  -- Empty queue
    	local value = queue[first]
    	queue[first] = nil        -- to allow garbage collection
    	queue.first = first + 1

	if (DEBUG) then print("Removendo " .. value .. " de " .. queue.first - 1) end

	if first > queue.last then  -- Queue becomes empty, reset indexes
		queue.first = 0
		queue.last = -1
	end
    return value
end

-- Indicates wether the queue has or hasn't any elements
function Queue.hasElements(queue)
	return queue[queue.first]
end

-- Tests functionalities of this file
function testQueue()
	queue = Queue.new()
	Queue.enqueue(queue, "a")
	Queue.enqueue(queue, "b")
	Queue.enqueue(queue, "c")

	index = queue.first
	value = queue[index]

	while (Queue.hasElements(queue)) do
		Queue.dequeue(queue)
	end
end

testQueue()
