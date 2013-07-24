require "lanes"
require "queue"
require "transaction_log"

DEBUG = false

ABOUT = {
    author= "Airton Libório <airton052@dcc.ufba.br>",
    description= "Lua Transactional memory implementation",
    copyright= "Copyright (c) 2009, Airton Libório",
    version= _version,
}

--
-- Initializes sets for transactional handling
--
local read_set  = {}
local write_set = {}
-- Conjunto de operações sobre as variáveis
local set       = {}

--
-- Variable versioning table
--
local linda = lanes.linda()
local lock = lanes.genlock(linda, 'lock')
--
-- Versioning constants
--
local INITIAL_VERSION = 1
local VERSION_INCREMENT = 1

--
-- Status de versionamento das variáveis
--
local ABORTED  =  0
local COMMITED =  1
local RUNNING  = -1

--
-- Function used to copy tables. Retired from http://lua-users.org/wiki/CopyTable
--
function deepcopy(object)
  local lookup_table = {}
  local function _copy(object)
    if type(object) ~= "table" then
      return object
    elseif lookup_table[object] then
      return lookup_table[object]
    end
    local new_table = {}
    lookup_table[object] = new_table
    for index, value in pairs(object) do
      new_table[_copy(index)] = _copy(value)
    end
    return setmetatable(new_table, getmetatable(object))
  end
  return _copy(object)
end

--
-- Transactionally reads x
-- Returns x and its version
--
function readTX(x, transaction)
  Transaction.enter(transaction)  -- Indicates that the transaction is trying to enter a critical section
  acquireLock()  -- Acquires global lock
    if (set[x] == nil) then  -- The shared variable doesn't exist on the system
      local stateList = List.new()  -- Value control list
      local newVariable = { value = deepcopy(x), version = INITIAL_VERSION, status = COMMITED }  -- Variable versioning
      List.addLast(stateList, newVariable)
      set[x] = { var = x, stateList = stateList }
    end
    local lastOperation = List.getLast(set[x].stateList)
    local version       = lastOperation.version
    local value         = lastOperation.value
    Transaction.addReadOperation(transaction, x, value, version)  -- Adds a read operation on the transaction log
    List.show(transaction.operation_list)
    --Transaction.exit(transaction)  -- Indicates that the transaction is exiting a critical section
  return value, version, releaseLock()  -- Returns and releases global lock. This is an attempt to make the return more atomic
end

--
-- Transactionally writes x_value to variable x
-- Returns the version of x
--
function writeTX(x, transaction, x_value)
  acquireLock()  -- Acquires global lock
    if (set[x] == nil) then
      local stateList = List.new()  -- List of the variable's states
      local writeOperation = { value = deepcopy(x_value), version = INITIAL_VERSION, status = COMMITED }
      List.addLast(stateList, writeOperation)
      set[x] = { var = x, stateList = stateList }
    else
      local lastOperation = List.getLast(set[x].stateList)
      local lastVersion = lastOperation.version
      local newOperation = { value = x_value, version = lastVersion + VERSION_INCREMENT, status = RUNNING}
      List.addLast(set[x].stateList, newOperation)
    end
    local lastOperation = List.getLast(set[x].stateList)
    local lastVersion = lastOperation.version
    Transaction.addWriteOperation(transaction, x, deepcopy(x), lastVersion)
    --Transaction.exit(transaction)  -- Indicates that the transaction is exiting a critical section
    local op = transaction.operation_list
  releaseLock()  -- Releases global lock
end

--
-- Acquires the global lock
--
function acquireLock()
  lock(1)
end

--
-- Releases the global lock
--
function releaseLock()
  lock(-1)
end

--
-- Commits the set
--
function commit(transaction, ...)
  lock(1)  -- Acquires global lock
  if (validate(transaction)) then
    for v in ipairs(args) do v = write_set[v] end
    return true, lock(-1)  -- Returns and releases global lock
  else return abort(transaction), lock(-1)  -- Returns and releases global lock
  end
end

--
-- Validates the transaction
--
function validate(transaction)
  if (transaction == nil) then return nil end
  print(transaction)
  local operation_list = transaction.operation_list
  for i = operation_list.first, operation_list.last do  -- Searches for a conflict on the transaction
    local operation = operation_list[i]  -- One of the transaction operations
    local variable = operation.variable  -- The operation variable
    local version = operation.version    -- The variable version
    local lastVariableOperation = List.getLast(set[variable].stateList)  -- The current variable state
    local currentVersion = lastVariableOperation.version  -- The current variable version
    if (currentVersion ~= version) then  -- There could be a conflict
      local hasFound = false
      local initial = i + 1
      for j = i + 1, operation_list.last do  -- Searchs for a transaction's write operation on the same variable
        local nextOperation = operation_list[j]
        if (nextOperation.operation_type == 'w' and nextOperation.variable == variable) then  -- Another write occurred, skips the checking
          hasFound = true
          break
        end
      end
      if (not hasFound) then return false end -- The variable was changed by another transaction, invalidates transaction
    end
  end
  return true  -- No conflicts found
end

--
-- Aborts the transaction
--
function abort(transaction)
  if (transaction == nil) then return nil end
  local operation_list = transaction.operation_list
  if (operation_list == nil) then return nil end
  for i = operation_list.first, operation_list.last do
    operation = operation_list[i]
    abortOperation(operation)  -- Aborts all the transaction's operations
  end
  return nil
end

--
-- Aborts an operation
--
function abortOperation(operation)
  if (operation.operation_type == 'w') then  -- Only undoes changes reggarding writes
    local stateList = set[operation.variable].stateList  -- Variable's state list
    local writeValue = operation.value  -- The value that was written
    local writeVersion = operation.version  -- The version that was set to the variable
    local currentState = List.getLast(stateList)
    if (currentState.version == writeVersion) then  -- Last element is the current value for the variable
      repeat  -- Undoes the transaction until a COMMITED or RUNNING value is found, like a cascade
        local currentState = List.getLast(stateList)
        if (currentState.status == RUNNING or currentState.status == COMMITED) then break
        else List.removeLast(stateList) end
      until(false or List(stateList).size() == 0)  -- Unlikely, but the implementation is error prone  :(
    else  -- Just searchs for the operation and sets the transaction changes as aborted
      for i = stateList.last - 1, stateList.first, -1 do
        local operation = stateList[i]
        if (operation.value == writeValue and operation.version == writeVersion) then
          operation.status = ABORTED  -- Sets as aborted
          break
        end
      end
    end
  end
end

--
-- Commits a value to a variable. This function returns the current value for the variable
--
function commit(x)
  if (x == nil) then return nil end
  local stateList = set[x].stateList
  if (stateList == nil) then return nil end
  if (List.size(stateList) > 1) then repeat List.removeFirst(stateList) until (List.size(stateList) == 1) end
  return List.getLast(stateList).value
end

x = 20
y = 10

function T1()
  local T = Transaction.new()
  writeTX(y, T, readTX(x, T) + 10)  -- y = x + 10
  writeTX(x, T, readTX(y, T))       -- x = y
  acquireLock()
    if (validate(T)) then
      x = commit(x)
      y = commit(y)
    end
  releaseLock()
end

function T2()
end

T1()
