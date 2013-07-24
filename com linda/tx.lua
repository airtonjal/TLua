require "lanes"
require "math"

ABOUT = {
    author = "Airton Libório <airton052@dcc.ufba.br>",
    description = "tLua - Lua Transactional memory implementation",
    copyright = "Copyright (c) 2009, Airton Libório",
    version = 0.1,
}

function STM()

  --
  -- Variable versioning table
  --
  local linda = lanes.linda()

  --
  -- Global lock
  --
  local lock  = lanes.genlock(linda, 'lock', 1)

  --
  -- Versioning constants
  --
  local INITIAL_VERSION = 1
  local VERSION_INCREMENT = 1

  --
  -- Variable states
  --
  local ABORTED   =  0
  local COMMITTED =  1
  local RUNNING   = -1

  --
  -- Debug flag variable
  --
  local DEBUG = true

  --
  -- Gets a shared variable
  --
  local get = function(key)
    return linda:get(key)
  end

  --
  -- Checks if the variable is registered on the variable table.
  --
  local registered = function(key)
    if (linda:get(key) == nil) then return false
    else return true end
  end

  --
  -- Acquires the global lock
  --
  local acquireLock = function(transaction)
    lock(1)
    if (DEBUG and transaction ~= nil) then print ("Transaction " .. transaction.transaction_id .. " acquired lock") end
  end

  --
  -- Releases the global lock
  --
  local releaseLock = function(transaction)
    lock(-1)
    if (DEBUG and transaction ~= nil) then print ("Transaction " .. transaction.transaction_id .. " released lock") end
  end


  --
  -- ID limit for transactions identifiers
  --
  local seedlimit = 1000000

  --
  -- Finds a value in a table
  --
  local find = function(t, v, c)
    local FIND_NOCASE = 0
    local FIND_PATTERN = 1
    local FIND_PATTERN_NOCASE = 2
    if type(t) == "table" and v then
      v = (c==0 or c==2) and v:lower() or v
      for k, val in pairs(t) do
        val = (c==0 or c==2) and val:lower() or val
        if (c==1 or c==2) and val:find(v) or v == val then
          return k
        end
      end
    end
    return false
  end


  --
  -- Function used to copy tables. Retired from http://lua-users.org/wiki/CopyTable
  --
  local deepcopy = function(object)
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
  -- Updates a transaction's state.
  --
  local update_transaction_state = function (transaction)
    if (transaction == nil) then return nil end
    local transaction_id = transaction.transaction_id
    linda:set(transaction_id, transaction)
    assert(registered(transaction_id), "Transaction " .. transaction_id .. " was not updated on the table")
  end

  --
  -- Creates a transaction generating a new transaction_id, and adds the transaction to the transacion list
  --
  local new_transaction = function()
    local new_number = false
    local transaction_id = 0
    local transaction = nil
    acquireLock()  -- Critical
      while(not new_number) do
        transaction_id = "t" .. math.random(seedlimit)  -- Concatenates with 't' to identify transaction objects
        if (not registered(transaction_id)) then new_number = true end  --Searches for the number
      end
      transaction = {transaction_id = transaction_id, operation_list = {}}  -- Creates transaction
      update_transaction_state(transaction)  -- Sets as a shared variable
    return transaction, releaseLock()
  end
  --
  -- Adds a write operation for the transaction. The lock must be acquired before this call.
  --
  local add_write = function(transaction, key, value, version)
    local transaction_id = transaction.transaction_id
    if (not registered(transaction_id)) then return nil end
    local operation_list = transaction.operation_list
    assert(operation_list ~= nil, "Transaction " .. transaction_id .. " has a nil operation_list")
    local new_operation = {key = key, value = value, version = version, operation_type = 'w'}
    table.insert(operation_list, new_operation)
    update_transaction_state(transaction)
  end

  --
  -- Adds a read operation for the transaction. The lock must be acquired before this call.
  --
  local add_read = function(transaction, key, value, version)
    local transaction_id = transaction.transaction_id
    if (not registered(transaction_id)) then return nil end
    local operation_list = transaction.operation_list
    assert(operation_list ~= nil, "Transaction " .. transaction_id .. " has a nil operation_list")
    local new_operation = {key = key, value = value, version = version, operation_type = 'r'}
    table.insert(operation_list, new_operation)
    update_transaction_state(transaction)
  end

  --
  -- Registers a linda shared variable
  --
  local register = function(key)
    local state_key = "v" .. key
    if (not registered(state_key)) then  -- The shared variable doesn't exist on the system
      local value = linda:get(key)
      assert(value ~= nil, "Variable with key " .. key .." not yet on linda tables")
      local state_list = {}  -- State control list
      local new_variable = {value = value, version = INITIAL_VERSION, status = COMMITTED}  -- Variable versioning
      table.insert(state_list, new_variable)
      linda:set(state_key, state_list)
      assert(linda:get(state_key) ~= nil, "Variable with key " .. key .. " could not be registered")
      return true
    end
    return false
  end

  --
  -- Finds a variable's state list
  --
  local get_state_list = function (key)
    local state_key = "v" .. key
    local state_list = linda:get(state_key)
    assert(state_list ~= nil, "State list of variable with key " .. key .. " cannot be nil")
    return state_list
  end

  --
  -- Updates a variable's state
  --
  local update_state = function(key, state_list)
    local state_key = "v" .. key
    linda:set(state_key, state_list)
    assert(get_state_list(key) ~= nil, "Variable " .. key .. " could not update state")
    assert(#state_list == #get_state_list(key), "Variable " .. " could not update state table")
  end

  --
  -- Updates a variable's value. Used on commits
  --
  local update = function(key, value)
    linda:set(key, value)
    assert(get(key) ~= nil, "Variable " .. key .. " could not be updated")
    if type(value) ~= "table" then assert(get(key) == value, "Variable " .. key .. " could not be updated") end
  end

  --
  -- Creates a new shared variable.
  --
  local new = function(key, value)
    linda:set(key, value)
    assert(get(key) ~= nil, "Variable " .. key .. " could not be created")
  end

  --
  -- Transactionally reads keyed variable x
  -- Returns x value
  --
  local readTX = function(key, transaction)
    acquireLock(transaction)  -- Acquires global lock
      register(key)  -- Registers variable if it doesn't exist on the system
      local commited_operation = get_state_list(key)[1]  -- Last variable commited value
      local version            = commited_operation.version
      local value              = commited_operation.value
      local operation_list     = transaction.operation_list
      if (DEBUG) then print("Transaction " .. transaction.transaction_id .. " reading variable " .. key .. " value: " .. value .. " version: " ..version) end
      for i = #operation_list, 1, -1 do  -- Searches for a transaction write on the variable
        local operation = operation_list[i]
        if operation.operation_type == 'w' and operation.key == key then  -- The transaction already wrote on the variable, updates values
          value   = operation.value
          version = operation.version
          break
        end
      end
      add_read(transaction, key, value, version)  -- Adds a read operation on the transaction log
    return value, releaseLock(transaction)  -- Returns and releases global lock. This is an attempt to make the return more atomic
  end

  --
  -- Transactionally writes value to keyed variable
  --
  local writeTX = function(key, transaction, value)
    acquireLock(transaction)  -- Acquires global lock
      register(key)  -- Registers variable if it doesn't exist on the system
      local state_list     = get_state_list(key)
      local version        = state_list[#state_list].version + VERSION_INCREMENT
      local this_operation = {value = value, version = version, status = RUNNING, transaction_id = transaction.transaction_id}
      if (DEBUG) then print("Transaction " .. transaction.transaction_id .. " writing variable " .. key .. " value: " .. value .. " version: " ..version) end
      table.insert(state_list, this_operation)
      update_state(key, state_list)
      add_write(transaction, key, value, version)
    releaseLock(transaction)  -- Releases global lock
  end

  --
  -- Validates the transaction
  --
  local validate = function(transaction)
    if (transaction == nil) then return nil end
    local operation_list = transaction.operation_list
    local transaction_variables = {}
    if DEBUG then print("Validating transaction " .. transaction.transaction_id .. " with " .. #operation_list .. " operations") end
    for i, operation in ipairs(operation_list) do  -- Group manipulated variables
      if DEBUG then
        local out = "Checking "
        if (operation.operation_type == "r") then out = out .. "read operation "
        else out = out .. "write operation " end
        out = out .. "on variable " .. operation.key .. " with value " .. operation.value
        print(out)
      end
      if not find(transaction_variables, operation.key) then
        table.insert(transaction_variables, operation.key)  -- Insert the value to control already committed variables
        local state_list = get_state_list(operation.key)
        if (state_list[1].version > operation.version) then
          if (DEBUG) then print("Transaction " .. transaction.transaction_id .. " read a no longer valid value for variable: " .. operation.key) end
          return false
        end
        for j = 2, #state_list do
          state = state_list[j]
          if state.transaction_id ~= transaction.transaction_id and state.status ~= ABORTED then  -- Another transaction wrote on the variable
            if (DEBUG) then print("Transaction " .. transaction.transaction_id .. " aborted because of transaction " .. state.transaction_id .. " and variable " .. operation.key) end
            return false
          end
        end
      end
      if (DEBUG) then print("Operation ok") end
    end
    if (DEBUG) then print ("Transaction " .. transaction.transaction_id .. " validated") end
    return true  -- No conflicts found
--~     for i, operation in ipairs(operation_list) do  -- Searches for a conflict on the transaction
--~       local key             = operation.key  -- The variable key
--~       local version         = operation.version  -- The variable version
--~       local state_list      = get_state_list(key)
--~       local last_operation  = state_list[#state_list]  -- The current variable state
--~       local current_version = last_operation.version   -- The current variable version
--~       if version > current_version then return false
--~       else
--~         for j = 2, #state_list do
--~           state = state_list[j]
--~           if state.transaction_id ~= transaction.transaction_id and state.status ~= ABORTED then  -- Another transaction wrote on the variable
--~             return false
--~           end
--~         end
--~       end
--~     end
  end


--~   local rollback = function(operation)
--~     if (operation.operation_type == 'w') then  -- Only undoes changes reggarding writes
--~       local key           = operation.key
--~       local state_list    = get_state_list(key)  -- Variable's state list
--~       for i, state in ipairs(state_list) do
--~         if state.status ~= COMMITTED and state.transaction_id ==
--~       end

--~       local write_version = operation.version  -- The version that was set to the variable
--~       local current_state = state_list[#state_list]
--~       if (current_state.version == write_version) then  -- Last element is the current value for the variable
--~         repeat  -- Undoes the transaction until a COMMITTED or RUNNING value is found, like a cascade
--~           local current_state = state_list[#state_list]
--~           if (current_state.status == RUNNING or current_state.status == COMMITTED) then break
--~           else table.remove(state_list, #state_list) end
--~         until (#stateList == 1)  -- Unlikely, but the implementation is error prone  :(
--~       else  -- Just searchs for the operation and sets the transaction changes as aborted
--~         for i = #state_list - 1, 1, -1 do
--~           local operation = state_list[i]
--~           if (operation.value == write_value and operation.version == write_version) then
--~             operation.status = ABORTED  -- Sets as aborted
--~             break
--~           end
--~         end
--~       end
--~       update_state(key, state_list)  -- Updates variable state
--~     end
--~   end

  --
  -- Rollbacks an operation
  --
  local rollback = function(transaction)
    assert(transaction ~= nil, "Trying to rollback a null transaction")
    if (DEBUG) then print("Undoing changes of transaction " .. transaction.transaction_id) end
    local operation_list = transaction.operation_list
    local transaction_variables = {}
    for i, operation in ipairs(operation_list) do  -- Group manipulated variables
      if (DEBUG) then print("Operation " .. operation.operation_type .. " variable: " .. operation.key .. " value: " .. operation.value) end
      if not find(transaction_variables, operation.key) then
        if (DEBUG) then print("Undoing changes on variable " .. operation.key) end
        table.insert(transaction_variables, operation.key)  -- Insert the value to control committed variables
        local state_list = get_state_list(operation.key)
        for j = 2, #state_list do
          local state = state_list[j]
          if state.status == RUNNING and state.transaction_id == transaction.transaction_id then
            state.status = ABORTED
          end
        end
        update_state(operation.key, state_list)
      end
    end
  end


  --
  -- Aborts the transaction
  --
  local abort = function(transaction)
    assert(transaction ~= nil, "Trying to abort a null transaction")
    rollback(transaction) -- Rollbacks all the transaction's operations
    transaction.operation_list = {}
    update_transaction_state(transaction)
  end

  --
  -- Commits a variable. This function returns the current value for the variable
  --
  local commit_variable = function(key, transaction_id)
    assert(key ~= nil, "Trying to commit a null keyed variable")
    local state_list = get_state_list(key)
    assert(state_list ~= nil, "Trying to commit a non-changed variable")
    local last_valid_operation = state_list[#state_list]
    i = #state_list - 1
    while last_valid_operation.status ~= RUNNING and i > 0 do
      state = state_list[i]
      if state.transaction_id == transaction_id and state.status == RUNNING then
        last_valid_operation = state
        break
      end
      i = i - 1
    end
    assert(last_valid_operation.status ~= ABORTED, "Trying to commit an aborted value")
    assert(last_valid_operation.status ~= COMMITTED, "Trying to commit an already committed value")
    state_list = {}
    last_valid_operation.status = COMMITTED
    table.insert(state_list, last_valid_operation)
    update_state(key, state_list)  -- Updates variable state
    update(key, last_valid_operation.value)  -- Updates variable value
    if (DEBUG) then print("Committed variable " .. key .. " with value " .. last_valid_operation.value) end
    return state_list[#state_list]
  end

  --
  -- Commits the set
  --
  local commit = function(transaction)
    assert(transaction ~= nil, "Transaction being committed cannot be nil")
    acquireLock(transaction)  -- Acquires global lock
    if (not validate(transaction)) then
      abort(transaction)
      return nil, releaseLock(transaction)
    else
      local operation_list = transaction.operation_list
      local write_keys = {}
      assert(operation_list ~= nil, "Operation list of transaction " .. transaction.transaction_id .. " is nil")
      for i, operation in ipairs(operation_list) do
        local key = operation.key
        if (DEBUG and operation.operation_type == "w") then
          local out = "Committing "
          if (operation.operation_type == "r") then out = out .. "read operation "
          else out = out .. "write operation " end
          out = out .. "on variable " .. operation.key .. " with value " .. operation.value
          print(out)
        end
        if (not find(write_keys, key) and operation.operation_type == "w") then
          table.insert(write_keys, key)  -- Insert the value to control committed variables
          commit_variable(key, transaction.transaction_id)  -- Commits the transaction's manipulated values
        end
      end
      transaction.operation_list = {}
      update_transaction_state(transaction)
      --linda:set("t" + transaction.transaction_id, nil)
      print("releasing")
      releaseLock(transaction)  -- Returns and releases global lock
      return true
    end
  end

  return {
    --
    -- Constants and attributes
    --
    linda = linda,
    lock  = lock,
    INITIAL_VERSION = INITIAL_VERSION,
    VERSION_INCREMENT = VERSION_INCREMENT,
    ABORTED   =  ABORTED,
    COMMITTED =  COMMITTED,
    RUNNING   =  RUNNING,

    --
    -- General purpose functions
    --
    registered          = registered,
    deepcopy          = deepcopy,
    find            = find,

    --
    -- Specific purpose functions
    --
    acquireLock        = acquireLock,
    releaseLock        = releaseLock,
    get            = get,
    update_transaction_state = update_transaction_state,
    new_transaction      = new_transaction,
    add_write         = add_write,
    add_read         = add_read,
    register          = register,
    get_state_list       = get_state_list,
    update_state       = update_state,
    update            = update,
    new            = new,
    readTX            = readTX,
    writeTX          = writeTX,
    validate          = validate,
    rollback          = rollback,
    abort            = abort,
    commit            = commit,
    commit_variable      = commit_variable
  }
end

--
-- Transaction 1
--
function T1(STM, key_x, key_y)
  print(key_x)
  print(key_y)
  assert(STM   ~= nil, "STM is nil")
  assert(key_x ~= nil, "key_x is nil")
  assert(key_y ~= nil, "key_y is nil")
  local T = STM.new_transaction()
  STM.writeTX(key_y, T, STM.readTX(key_x, T) + 10)  -- y = x + 10
  STM.writeTX(key_x, T, STM.readTX(key_y, T))       -- x = y
  return STM.commit(T)                -- Attempts to commit
end

--
-- Transaction 2
--
function T2()
  local T = Transaction.new()
  writeTX(key_x, T, readTX(key_x, T) - 6)    -- x = x - 6
  writeTX(key_y, T, readTX(key_x, T) - 10)  -- y = x - 10
  local committed = commit(T)           -- Attempts to commit
end

function test_STM()
  local STM = STM()

  x = 20
  y = 10

  key_x = STM.deepcopy(x)
  key_y = STM.deepcopy(y)

  STM.new(key_x, x)
  STM.new(key_y, y)

  try = lanes.gen("*", T1)
  local ret = try(STM, key_x, key_y)
  local ret2 = try(STM, key_x, key_y)
  local result = ret1[1]
  local result = ret2[1]
  if result then print("Resultado true")
  else print("Resultado false") end

  print("x " .. STM.get(key_x))
  print("y " .. STM.get(key_y))

  assert(STM.get(key_x) == 40, "Variable with " .. key_x .. "not set")
  assert(STM.get(key_y) == 40, "Variable with " .. key_y .. "not set")
end

function test_abort_STM()
  local STM = STM()

  x = 20
  y = 10

  key_x = STM.deepcopy(x)
  key_y = STM.deepcopy(y)

  STM.new(key_x, x)
  STM.new(key_y, y)

    --
  -- Tests an abort over transaction T2
  --
  local test_abort = function(STM, key_x, key_y)
    --T1 STARTS
    local T1 = STM.new_transaction()
    STM.writeTX(key_y, T1, STM.readTX(key_x, T1) + 10)    -- y = x + 10

    -- T2 INTERRUPTS T1, STARTS AND COMMITS

    local T2 = STM.new_transaction()
    STM.writeTX(key_x, T2, STM.readTX(key_x, T2) - 6)    -- x = x - 6
    STM.writeTX(key_y, T2, STM.readTX(key_x, T2) - 10)    -- y = x - 10
    local committed2 = STM.commit(T2)               -- Attempts to commit

    -- T1 REGAINS CONTROL AND TRIES DO COMMIT. HAS TO FAIL

    STM.writeTX(key_x, T1, STM.readTX(key_y, T1))         -- x = y
    local committed2 = STM.commit(T1)               -- Attempts to commit

    return committed1, committed2
  end

  --test_abort(STM, key_x, key_y)

  try = lanes.gen("*", test_abort)
  local ret = try(STM, key_x, key_y)
  local result1 = ret[1]  -- T1's result
  local result2 = ret[2]  -- T2's result
  assert(not result1, "A primeira transação deveria ter sido efetuada")
  assert(result2, "A segunda transação deveria ter sido efetuada")

  print("x " .. STM.get(key_x))
  print("y " .. STM.get(key_y))

   assert(STM.get(key_x) == 30, "Variable with " .. key_x .. " not set")
  assert(STM.get(key_y) == 30, "Variable with " .. key_y .. " not set")
end

function test_double_abort_STM()
  local STM = STM()

  x = 20
  y = 10

  key_x = STM.deepcopy(x)
  key_y = STM.deepcopy(y)

  STM.new(key_x, x)
  STM.new(key_y, y)

  --
  -- Test two transactions acting and both aborting
  --
  local test_double_abort = function(STM, key_x, key_y)
    --T1 STARTS
    local T1 = STM.new_transaction()
    STM.writeTX(key_x, T1, STM.readTX(key_x, T1) - 6)    -- x = x - 6

    --T2 STARTS
    local T2 = STM.new_transaction()
    STM.writeTX(key_x, T2, STM.readTX(key_y, T2) + 10)    -- x = y + 10

    --T1 CONTINUES AND TRIES TO COMMIT. HAS TO FAIL
    STM.writeTX(key_y, T1, STM.readTX(key_x, T1) - 10)    -- y = x - 10
    local committed1 = STM.commit(T1)               -- Attempts to commit

    --T2 CONTINUES AND TRIES TO COMMIT. HAS TO FAIL
    STM.writeTX(key_x, T2, STM.readTX(key_y, T2))         -- x = y
    local committed2 = STM.commit(T2)               -- Attempts to commit

    return commited1, commited2
  end

  try = lanes.gen("*", test_double_abort)
  local ret = try(STM, key_x, key_y)
  local result1 = ret[1]
  local result2 = ret[2]
  assert(not result1, "A primeira transação deveria ter sido abortada")
  assert(not result2, "A segunda transação deveria ter sido abortada")

  print("x " .. STM.get(key_x))
  print("y " .. STM.get(key_y))

  assert(STM.get(key_x) == 20, "Variable with " .. key_x .. " not set")
  assert(STM.get(key_y) == 10, "Variable with " .. key_y .. " not set")
end

function test_account()
  local STM = STM()

  key_acc1 = 1
  key_acc2 = 2

  acc1 = {nome = "Conta 1", balance = 150}
  acc2 = {nome = "Conta 2", balance = 140}

  STM.new(key_acc1, acc1.balance)
  STM.new(key_acc2, acc2.balance)

  --
  -- Transfers money from one account to another
  --
  local transfer = function(STM, key_from, key_to, amount)
    print("Transfering " .. amount .. " from " .. key_from .. " to " .. key_to ..
    "\nFrom balance " .. STM.get(key_from) ..
    "\nTo balance " .. STM.get(key_to))
    i = 0
    local T = STM.new_transaction()
    repeat
      if (i == 0) then print ("Starting transaction " .. T.transaction_id)
      else print ("Reexecuting transaction " .. amount "\nTimes executed: " .. i) end
      i = i + 1
      if (STM.readTX(key_from, T) - amount >= 0) then  -- if (from.balance - amount >= 0) then
        STM.writeTX(key_from, T, STM.readTX(key_from, T) - amount)  -- from.balance = from.balance - amount
        STM.writeTX(key_to, T, STM.readTX(key_to, T) + amount)  -- to.balance = to.balance + amount
      else print("Not enough money") end
    until (STM.commit(T) == true)
    saida = "Committed " .. STM.get(key_to)
    print("Transaction " .. T.transaction_id .. " committed. Reexecuted " .. (i - 1))
  end

  for i = 1, 20 do
    local amount = math.random(500)
    transfered = lanes.gen("*", transfer)
    local res = nil
    if (i % 2 == 0) then res = transfered(STM, key_acc1, key_acc2, amount)
    else res = transfered(STM, key_acc2, key_acc1, amount) end
    repeat if (amount % 500 == 0) then print("Status: " .. res.status) end until(res.status == "done")
    print("Status: " .. res.status)
    --if (i % 2 == 0) then transfer(STM, key_acc1, key_acc2, amount)
    --else transfer(STM, key_acc2, key_acc1, amount) end
  end
end

function test_increment()
  local STM = STM()

  key_1 = 1
  STM.new(key_1, 1)

  local read = function(STM, key_1)
    i = 0
    local T = STM.new_transaction()
    local wait_time = 0
    repeat
      if (i == 0) then print("Starting transaction " .. T.transaction_id)
      else
        print ("Reexecuting transaction " ..  T.transaction_id .."\nTimes executed: " .. i)
        now = os.clock()
        wait_time = now + math.random() / 10
        print("Current clock: " .. os.clock() .. " wait time for transaction " .. T.transaction_id .. " = " .. wait_time - now)
        repeat until (os.clock() > wait_time + math.random())
      end
      i = i + 1
      STM.writeTX(key_1, T, STM.readTX(key_1, T) + 1)  -- i = i + 1
      local committed = STM.commit(T)
    until (committed == true)
    print("Transaction " .. T.transaction_id .. " committed")
  end

  local res = {}
  leitura = lanes.gen("*", read)
  for i = 1, 25 do
    res[i] = leitura(STM, key_1)
    --repeat if (math.random(500) % 500 == 0) then print("Status: " .. res.status) end until(res.status == "done")
    --print("Status: " .. res.status)
    --read(STM, key_1, key_2, key_3, key_4, key_5)
  end

  local finished = true
  repeat for i, result in ipairs(res) do if (res ~= "done") then finished = false end end until (finished)

  --wait_end(res)

  print(STM.get(key_1))
end

test_increment()


--test_double_abort()
--test_abort()
--test_abort()

--print("x vale: " .. Variable.get(key_x))
--print("y vale: " .. Variable.get(key_y))

