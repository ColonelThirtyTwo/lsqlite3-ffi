
assert(jit, "lsqlite3_ffi must run on LuaJIT!")
local ffi = require "ffi"

ffi.cdef(assert(io.read((LSQLITE3_FFI_PATH or "").."/sqlite3.ffi")):read("*a"))
local sqlite3 = ffi.load("sqlite3",true)
local new_db_ptr = ffi.typeof("sqlite3*[1]")
local new_stmt_ptr = ffi.typeof("sqlite3_stmt*[1]")
local new_exec_ptr = ffi.typeof("int (*callback)(void*,int,char**,char**)")

local value_handlers = {
	[sqlite3.SQLITE_INTEGER] = function(stmt, n) return sqlite3.sqlite3_column_int(stmt, n) end,
	[sqlite3.SQLITE_FLOAT] = function(stmt, n) return sqlite3.sqlite3_column_double(stmt, n) end,
	[sqlite3.SQLITE_TEXT] = function(stmt, n) return ffi.string(sqlite3.sqlite3_column_text(stmt,n)) end,
	[sqlite3.SQLITE_BLOB] = function(stmt, n) return ffi.string(sqlite3.sqlite_column_blob(stmt,n), sqlite3.sqlite3_column_bytes(stmt,n)) end,
	[sqlite3.SQLITE_NULL] = function() return nil end
}

local lsqlite3 = {}
local sqlite_db = {}
sqlite_db.__index = sqlite_db
local sqlite_stmt = {}
sqlite_stmt.__index = sqlite_stmt

-- -------------------------- Library Methods -------------------------- --

function lsqlite3.open(filename)
	local sdb = new_db_ptr()
	sqlite3.sqlite3_open(filename, sdb)
	return setmetatable({db = sdb[1]},sqlite_db)
end
function lsqlite3.open_memory()
	return lsqlite3.open(":memory:")
end

function lsqlite3.complete(str)
	local r = sqlite3.sqlite3_complete(str)
	if r == sqlite3.SQLITE_NOMEM then error("out of memory (sqlite)",2) end
	return r ~= 0 and true or false
end

function lsqlite3.version()
	return ffi.string(sqlite3.sqlite3_version)
end

-- TODO: lsqlite3.temp_directory

-- -------------------------- Database Methods -------------------------- --

-- TODO: db:busy_handler
-- TODO: db:busy_timeout

function sqlite_db:changes()
	return sqlite3.sqlite3_changes(self.db)
end

function sqlite_db:close()
	local r = sqlite3.sqlite3_close(self.db)
	if r == sqlite3.SQLITE_OK then
		self.db = nil
	end
	return r
end

-- TODO: db:close_vm
-- TODO: db:create_aggregate
-- TODO: db:create_collation
-- TODO: db:create_function

function sqlite_db:errcode()
	return sqlite3.sqlite3_extended_errcode(self.db)
end
sqlite_db.error_code = sqlite_db.errcode

function sqlite_db:errmsg()
	return ffi.string(sqlite3.sqlite3_errmsg(self.db))
end
sqlite_db.error_message = sqlite_db.errmsg

function sqlite_db:exec(sql, func, udata)
	local cb = nil
	if func then
		-- TODO: db:exec callbacks
		error("callback functions not supported yet",2)
	end
	
	return sqlite3.sqlite3_exec(self.db, sql, nil, nil, nil)
end

function sqlite_db:interrupt()
	return sqlite3.sqlite3_interrupt(self.db)
end

function sqlite_db:isopen() return self.db and true or false end

function sqlite_db:last_insert_rowid()
	return sqlite3.sqlite3_last_insert_rowid(self.db)
end

-- TODO: db:nrows

function db:prepare(sql)
	local stmtptr = new_stmt_ptr()
	local r = sqlite3.sqlite3_prepare_v2(self.db, sql, #sql+1, stmtptr, nil)
	if r ~= sqlite3.SQLITE_OK then return nil end
	return setmetatable({stmt=stmtptr[1]},sqlite_stmt)
end

-- TODO: db:progress_handler
-- TODO: db:rows

function sqlite_db:total_changes()
	return sqlite3.sqlite3_total_changes(self.db)
end

-- TODO: db:trace
-- TODO: db:urows

-- -------------------------- Statement Methods -------------------------- --

function sqlite_stmt:bind(n, value)
	local t = type(value)
	if t == "string" then
		return sqlite3.sqlite3_bind_text(self.stmt, n, value, #value+1, sqlite3.SQLITE_TRANSIENT)
	elseif t == "number" then
		return sqlite3.sqlite3_bind_double(self.stmt, n, value)
	elseif t == "boolean" then
		return sqlite3.sqlite3_bind_int(self.stmt, n, value)
	elseif t == "nil" then
		return sqlite3.sqlite3_bind_null(self.stmt, n)
	elseif t == "cdata" then
		return sqlite3.sqlite3_bind_int64(self.stmt, n, ffi.cast("sqlite3_int64",value))
	else error("invalid bind type: "..t,2) end
end

function sqlite_stmt:bind_blob(n,value)
	if not value then
		return sqlite3.sqlite3_bind_zeroblob(self.stmt, n, 0)
	elseif type(value) == "cdata" or type(value) == "string" then
		return sqlite3.sqlite3_bind_blob(self.stmt, n, value, type(value) == "cdata" and ffi.sizeof(value) or #value, sqlite3.SQLITE_TRANSIENT)
	else
		error("invalid bind type: "..type(value))
	end
end

-- TODO: stmt:bind_names
-- TODO: stmt:bind_parameter_count
-- TODO: stmt:bind_parameter_name

function sqlite_stmt:bind_values(...)
	local i = 1
	local v = select(1,...)
	while v do
		self:bind(i,v) -- TODO: error checking?
		i = i + 1
		v = select(i,...)
	end
end

function sqlite_stmt:columns()
	return sqlite3.sqlite3_column_count(self.stmt)
end

function sqlite_stmt:finalize()
	local r = sqlite3.sqlite3_finalize(self.stmt)
	self.stmt = nil
	return r
end

function sqlite_stmt:get_name(n)
	return ffi.string(sqlite3.sqlite3_column_name(self.stmt, n))
end

function sqlite_stmt:get_named_types()
	local tbl = {}
	for i=0,sqlite3.sqlite3_column_count(self.stmt)-1 do
		tbl[ffi.string(sqlite3.sqlite3_column_name(self.stmt, n))] = ffi.string(sqlite3.sqlite3_column_decltype(self.stmt, n))
	end
	return tbl
end

-- TODO: stmt:get_named_values
-- TODO: stmt:get_names
-- TODO: stmt:get_unames
-- TODO: stmt:get_utypes
-- TODO: stmt:get_uvalues

function sqlite_stmt:get_value(n)
	return value_handlers[sqlite3.sqlite3_column_type(self.stmt,n)](self.stmt,n)
end

function sqlite_stmt:get_values()
	local tbl = {}
	for i=0,sqlite3.sqlite3_column_count(self.stmt)-1 do
		tbl[i] = self:get_value(i)
	end
	return tbl
end

function sqlite_stmt:get_values_unpacked(n)
	n = n or 0
	if n < sqlite3.sqlite3_column_count(self.stmt) then
		return self:get_value(n), self:get_values_unpacked(n+1)
	end
end

function sqlite_stmt:isopen() return self.stmt and true or false end

-- TODO: stmt:nrows

function sqlite_stmt:reset()
	sqlite3.sqlite3_reset(self.stmt)
end

function sqlite_stmt:rows()
	return function()
		local r = self:step()
		if r ~= sqlite3.SQLITE_ROW then return end -- TODO: check an error here?
		return self:get_values()
	end
end

function sqlite_stmt:rows_unpacked()
	return function()
		local r = self:step()
		if r ~= sqlite3.SQLITE_ROW then return end -- TODO: check an error here?
		return self:get_values_unpacked()
	end
end

function sqlite_stmt:step()
	return sqlite3.sqlite3_step(self.stmt)
end

-- TODO: stmt:urows

return lsqlite3
