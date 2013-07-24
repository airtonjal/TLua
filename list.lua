local DEBUG = false

List = {}

--
-- Creates new empty list
--
function List.new()
    return {first = 0, last = -1}
end

--
-- Adds value to list's first position
--
function List.addFirst (list, value)
  if (not List.validateAdd(list, value)) then return nil end  -- Validate parameters
  local first = list.first - 1
  list.first = first
  list[first] = value
  if (DEBUG) then print("Inserindo " .. value .. " em " .. list.first) end
  return value
end

--
-- Adds value to list's last position
--
function List.addLast (list, value)
  if (not List.validateAdd(list, value)) then return nil end  -- Validate parameters
  local last = list.last + 1
    list.last = last
    list[last] = value
  if (DEBUG) then print("Inserindo " .. value .. " em " .. list.last) end
  return value
end

--
-- Returns last list element
--
function List.getLast(list)
  if (list == nil) then return nil end
  return list[list.last]
end

--
-- Returns first list element
--
function List.getFirst(list)
  if (list == nil) then return nil end
  return list[list.first]
end

--
-- Validates arguments from add functions
--
function List.validateAdd(list, value)
  if ((value == nil) or (list == nil)) then return nil end  -- Do not add nil values
  if (not type(list) == "table") then return nil end  -- Do not add nil values
  return true
end

--
-- Validates arguments from remove functions
--
function List.validateRemove(list)
  if (list == nil) then return false end
  if (list.last < list.first) then return false end
  return true
end

--
-- Removes list's first element
--
function List.removeFirst (list)
  if (not List.validateRemove(list)) then return nil end
  local first = list.first
    local value = list[first]
    list[first] = nil        -- to allow garbage collection
    list.first = first + 1

  if (DEBUG) then print("Removendo " .. value .. " de " .. list.first - 1) end

  return value
end

--
-- Removes list's last element
--
function List.removeLast (list)
  if (not List.validateRemove(list)) then return nil end
  local last = list.last
    local value = list[last]
    list[last] = nil         -- to allow garbage collection
    list.last = last - 1

  if (DEBUG) then print("Removendo " .. value .. " de " .. list.last + 1) end

    return value
end

--
-- Returns the list's size
--
function List.size(list)
  if list.last < list.first then return 0
  else return list.last - list.first + 1 end
end

function List.show(list)
  for i = list.first, list.last do
    print(list[i])
  end
end

--
-- Function to test functionalities
--
function testList()
  list = List.new()
  List.addLast(list, "a")
  List.addLast(list, "b")
  print("Tamanho " .. List.size(list))
  List.removeLast(list)
  List.removeLast(list)
  List.removeLast(list)
  print("Tamanho " .. List.size(list))
  List.addLast(list, "c")
  List.addLast(list, "d")
  List.addLast(list, "e")
  print("Tamanho " .. List.size(list))
  List.removeFirst(list)
  List.removeLast(list)
  List.removeLast(list)
  print("Tamanho " .. List.size(list))
  List.removeLast(list)
  List.removeLast(list)
  List.removeLast(list)
  print("Tamanho " .. List.size(list))
  List.removeLast(list)
  List.addLast(list, "a")
  List.addLast(list, "b")
  List.addLast(list, "c")
  print("Tamanho " .. List.size(list))
  List.addFirst(list, "d")
  List.addFirst(list, "e")
  List.addFirst(list, "f")
  print("Tamanho " .. List.size(list))
  List.addFirst(list, "g")
  List.removeFirst(list)
  List.removeFirst(list)
  List.removeFirst(list)
  print("Tamanho " .. List.size(list))
  --List.removeFirst(list)
  --List.removeFirst(list)
  print("Tamanho " .. List.size(list))
  List.show(list)
end


testList()
