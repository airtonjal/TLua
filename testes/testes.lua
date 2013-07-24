require "lanes"
require "queue"

function teste(y)
  if (type(y) == "table") then
    print("y is a table")
  elseif (type(y) == "number") then
    print("y is a number")
  end
end

local tabela = {}

function insert(x)
  tabela[x] = x
end

function write(v)
  v = tabela[v]
  if (tabela[v] == nil) then print("Ficou nil")
  else print("Atribuindo " .. tabela[v] .. " a " .. v)
  end
end

local x1 = 10
local x2 = 15

insert(x1)
insert(x2)

print("Tabela antes")
print(tabela[x1])
print(tabela[x2])

tabela[x1] = 9

print("Tabela depois")
print(tabela[x1])
print(tabela[x2])

write(x1)
write(x2)

print("variáveis")
print(x1)
print(x2)

funcao= function(n, name)
      print(n, name)
    end

f = lanes.gen(funcao)
a = f(1)
b = f(2)

for i=1,1000 do a[i] = f(i, "a") end
for i=1,1000 do b[i] = f(i, "b") end

local linda= lanes.linda()

local function loop(x, max)
  for i = 1,max do
    --print(x .. " sending: " ..i )
    linda:send("x", i)    -- linda as upvalue
  end
end

a = lanes.gen("",loop)("a", 10000)
b = lanes.gen("",loop)("b", 10000)

while true do
  local val= linda:receive( 3.0, "x" )    -- timeout in seconds
    if val==nil then
        --print( "timed out" )
        break
  end
  --print( "received: "..val )
end

