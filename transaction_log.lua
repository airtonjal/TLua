-- This file models the system's transactions
require "queue"
require "list"
require "math"

-- wait_queue is the transaction's waiting queue for variable set manipulating
Transaction = {wait_queue = Queue.new()}

-- ID limit for transactions identifiers
local seedlimit = 1000000

--
-- Indicates that the transaction is trying to enter a critical section
-- Not sure if this is needed, just let it be there
--
function Transaction.enter(transaction)
  if (transaction == nil) then return nil
  else Queue.enqueue(transaction) end
end

--
-- Indicates that the transaction is exiting a critical section
-- Not sure if this is needed, just let it be there
--
function Transaction.exit(transaction)
  if (transaction == nil) then return nil
  else Queue.dequeue(transaction) end
end

--
-- Creates a transaction generating a new transaction_id, and adds the transaction to the transacion list
--
function Transaction.new()
  local new_number = false
  local transaction_id = 0
  while(not new_number) do
    transaction_id = math.random(seedlimit)
    if Transaction[transaction_id] == nil then new_number = true end
  end
  transaction = {transaction_id = transaction_id, operation_list = List.new()}
  Transaction[transaction_id] = transaction
  return transaction
end

--
-- Adds a write operation for the transaction
--
function Transaction.addWriteOperation(transaction, variable, value, version)
  local transaction_id = transaction.transaction_id
  if (not Transaction.isRegistered(transaction_id)) then return nil end
  local operation_list = Transaction[transaction_id].operation_list
  local newOperation = {variable = variable, value = value, version = version, operation_type = 'w'}
  List.addLast(operation_list, newOperation)
end

--
-- Adds a read operation for the transaction
--
function Transaction.addReadOperation(transaction, variable, value, version)
  local transaction_id = transaction.transaction_id
  if (not Transaction.isRegistered(transaction_id)) then return nil end
  local operation_list = Transaction[transaction_id].operation_list
  local newOperation =  {variable = variable, value = value, version = version, operation_type = 'r'}
  List.addLast(operation_list, newOperation)
end

--
-- Checks if the transaction is registered on the transaction table
--
function Transaction.isRegistered(transaction_id)
  if (Transaction[transaction_id] == nil) then return false
  else return true end
end

--
-- Tests this file's functionality
--
function testTransactionsLog()
  T1 = Transaction.new()
  T2 = Transaction.new()

  print(T1.transaction_id)
  print(T2.transaction_id)

  x = 3
  x_version = 1
  y = 4

  Transaction.addReadOperation(T1, x, 3, x_version)
  Transaction.addWriteOperation(T1, x, 3, 4, x_version + 1)
  List.removeLast(T1.operation_list)
  print(List.size(T1.operation_list))
end

testTransactionsLog()
