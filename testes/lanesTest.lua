require "lanes"

x = 10

local linda = lanes.linda()
local lock = lanes.genlock(linda, 'lock')

local function loop(max)
  for i = 1, max do
        print("sending: " .. i)
        linda:send("x", i)    -- linda as upvalue
    end
end

local function recebe(str, max)
  for i = 1, max do
        local val = linda:receive(3.0, "x")
    if val ~= nil then print(str .. " received: " .. val) end
    end
end

a = lanes.gen("", loop)(10000)
b = lanes.gen("", recebe)("b", 10000)
c = lanes.gen("", recebe)("c", 10000)

while true do
    local val = linda:receive(3.0, "x")    -- timeout in seconds
    if val == nil then
        print("timed out")
        break
    end
    print("received: " .. val)
end


