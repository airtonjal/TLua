require "tx"
require "math"
require "lanes"

Account = {}

local limit = -100

from = Account.new(2000)
to 	 = Account.new(0)

--
-- Creates new Account
--
function Account.new(name, balance)
	return {name = name, balance = balance}
end

--
-- Transfers money from account "from" to account "to"
--
local function transfer(amount)
	repeat
		T = Transaction.new()
		if (readTX(from.balance, T) - amount > -100) then
			writeTX(to.amount, T, readTX(to.amount, T) + amount)
			writeTX(from.amount, T, -1 * amount)
		end
		acquireLock()
			valid = validate(T)
			if (valid) then
				-- Commits transaction
				from.balance = commit(from.balance)
				to.balance = commit(to.balance)
			end
		releaseLock()
	until(valid)
end

for i = 0, 2000 do
	local amount = math.random(50, 100)
	print("Transfering " .. amount)
	lanes.gen("", transfer)(amount)
	print("Balance " .. from.balance)
	from.balance = from.balance + math.random(20, 30)
end


