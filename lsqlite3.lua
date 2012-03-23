
assert(jit, "lsqlite3_ffi must run on LuaJIT!")
local ffi = require "ffi"

ffi.cdef(assert(io.open((LSQLITE3_FFI_PATH or ".").."/sqlite3.ffi")):read("*a"))
local sqlite3 = ffi.load("sqlite3",true)
local new_db_ptr = ffi.typeof("sqlite3*[1]")
local new_stmt_ptr = ffi.typeof("sqlite3_stmt*[1]")
local new_exec_ptr = ffi.typeof("int (*)(void*,int,char**,char**)")
local new_blob_ptr = ffi.typeof("sqlite3_blob*[1]")
local new_bytearr = ffi.typeof("uint8_t[?]")
local sqlite3_transient = ffi.cast("void*",-1)

local value_handlers = {
	[sqlite3.SQLITE_INTEGER] = function(stmt, n) return sqlite3.sqlite3_column_int(stmt, n) end,
	[sqlite3.SQLITE_FLOAT] = function(stmt, n) return sqlite3.sqlite3_column_double(stmt, n) end,
	[sqlite3.SQLITE_TEXT] = function(stmt, n) return ffi.string(sqlite3.sqlite3_column_text(stmt,n)) end,
	[sqlite3.SQLITE_BLOB] = function(stmt, n) return ffi.string(sqlite3.sqlite3_column_blob(stmt,n), sqlite3.sqlite3_column_bytes(stmt,n)) end,
	[sqlite3.SQLITE_NULL] = function() return nil end
}

local lsqlite3 = {}
local sqlite_db = {}
sqlite_db.__index = sqlite_db
local sqlite_stmt = {}
sqlite_stmt.__index = sqlite_stmt
local sqlite_blob = {}
sqlite_blob.__index = sqlite_blob

lsqlite3.DEBUG = false

-- -------------------------- Library Methods -------------------------- --

function lsqlite3.open(filename)
	local sdb = new_db_ptr()
	local err = sqlite3.sqlite3_open(filename, sdb)
	local db = sdb[0]
	if err ~= sqlite3.SQLITE_OK then return nil, sqlite3.sqlite3_errmsg(db) end
	return setmetatable({
		db = db,
		stmts = {},
		blobs = {}
	},sqlite_db)
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

lsqlite3.OK = sqlite3.SQLITE_OK
lsqlite3.ERROR = sqlite3.SQLITE_ERROR
lsqlite3.INTERNAL = sqlite3.SQLITE_INTERNAL
lsqlite3.PERM = sqlite3.SQLITE_PERM
lsqlite3.ABORT = sqlite3.SQLITE_ABORT
lsqlite3.BUSY = sqlite3.SQLITE_BUSY
lsqlite3.LOCKED = sqlite3.SQLITE_LOCKED
lsqlite3.NOMEM = sqlite3.SQLITE_NOMEM
lsqlite3.READONLY = sqlite3.SQLITE_READONLY
lsqlite3.INTERRUPT = sqlite3.SQLITE_INTERRUPT
lsqlite3.IOERR = sqlite3.SQLITE_IOERR
lsqlite3.CORRUPT = sqlite3.SQLITE_CORRUPT
lsqlite3.NOTFOUND = sqlite3.SQLITE_NOTFOUND
lsqlite3.FULL = sqlite3.SQLITE_FULL
lsqlite3.CANTOPEN = sqlite3.SQLITE_CANTOPEN
lsqlite3.PROTOCOL = sqlite3.SQLITE_PROTOCOL
lsqlite3.EMPTY = sqlite3.SQLITE_EMPTY
lsqlite3.SCHEMA = sqlite3.SQLITE_SCHEMA
lsqlite3.TOOBIG = sqlite3.SQLITE_TOOBIG
lsqlite3.CONSTRAINT = sqlite3.SQLITE_CONSTRAINT
lsqlite3.MISMATCH = sqlite3.SQLITE_MISMATCH
lsqlite3.MISUSE = sqlite3.SQLITE_MISUSE
lsqlite3.NOLFS = sqlite3.SQLITE_NOLFS
lsqlite3.FORMAT = sqlite3.SQLITE_FORMAT
lsqlite3.NOTADB = sqlite3.SQLITE_NOTADB
lsqlite3.RANGE = sqlite3.SQLITE_RANGE
lsqlite3.ROW = sqlite3.SQLITE_ROW
lsqlite3.DONE = sqlite3.SQLITE_DONE
lsqlite3.INTEGER = sqlite3.SQLITE_INTEGER
lsqlite3.FLOAT = sqlite3.SQLITE_FLOAT
lsqlite3.TEXT = sqlite3.SQLITE_TEXT
lsqlite3.BLOB = sqlite3.SQLITE_BLOB
lsqlite3.NULL = sqlite3.SQLITE_NULL

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
	self.db:check(r)
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
	if func then
		-- TODO: db:exec callbacks
		error("callback functions not supported yet",2)
	end
	
	self:check(sqlite3.sqlite3_exec(self.db, sql, nil, nil, nil))
end

function sqlite_db:interrupt()
	sqlite3.sqlite3_interrupt(self.db)
end

function sqlite_db:isopen() return self.db and true or false end

function sqlite_db:last_insert_rowid()
	return tonumber(sqlite3.sqlite3_last_insert_rowid(self.db))
end

-- TODO: db:nrows

function sqlite_db:prepare(sql)
	local stmtptr = new_stmt_ptr()
	self:check(sqlite3.sqlite3_prepare_v2(self.db, sql, #sql+1, stmtptr, nil))
	local stmt = setmetatable(
	{
		stmt=stmtptr[0],
		db=self,
		trace=lsqlite3.DEBUG and debug.traceback() or nil
	},sqlite_stmt)
	self.stmts[stmt] = stmt
	return stmt
end

-- TODO: db:progress_handler
-- TODO: db:rows

function sqlite_db:total_changes()
	return sqlite3.sqlite3_total_changes(self.db)
end

-- TODO: db:trace
-- TODO: db:urows

function sqlite_db:check(ret)
	if ret ~= sqlite3.SQLITE_OK then
		error(self:errmsg())
	end
	return ret
end

function sqlite_db:checkstep(ret)
	if ret == sqlite3.SQLITE_ROW then
		return true
	elseif ret == sqlite3.SQLITE_DONE then
		return false
	else
		error(self:errmsg())
	end
end

function sqlite_db:open_blob(db, tbl, column, row, write)
	local blobptr = new_blob_ptr()
	self:check(sqlite3.sqlite3_blob_open(self.db, db or "main", tbl, column, row, write, blobptr))
	local blob = setmetatable(
	{
		blob = blobptr[0],
		db = self,
		trace = lsqlite3.DEBUG and debug.traceback() or nil
	},sqlite_blob)
	self.blobs[blob] = blob
	return blob
end

function sqlite_db:dump_unfinalized_statements()
	for _,stmt in pairs(self.stmts) do
		print(tostring(stmt))
		if stmt.trace then
			print("defined at: "..stmt.trace)
		end
	end
end

function sqlite_db:dump_unclosed_blobs()
	for _,blob in pairs(self.blobs) do
		print(tostring(blob))
		if blob.trace then
			print("defined at: "..blob.trace)
		end
	end
end

-- -------------------------- Statement Methods -------------------------- --

function sqlite_stmt:bind(n, value)
	local t = type(value)
	if t == "string" then
		self.db:check(sqlite3.sqlite3_bind_text(self.stmt, n, value, #value+1, sqlite3_transient))
	elseif t == "number" then
		self.db:check(sqlite3.sqlite3_bind_double(self.stmt, n, value))
	elseif t == "boolean" then
		self.db:check(sqlite3.sqlite3_bind_int(self.stmt, n, value))
	elseif t == "nil" then
		self.db:check(sqlite3.sqlite3_bind_null(self.stmt, n))
	elseif t == "cdata" then
		self.db:check(sqlite3.sqlite3_bind_int64(self.stmt, n, ffi.cast("sqlite3_int64",value)))
	else error("invalid bind type: "..t,2) end
end

function sqlite_stmt:bind_blob(n,value,len)
	if not value then
		self.db:check(sqlite3.sqlite3_bind_zeroblob(self.stmt, n, len or 0))
	elseif type(value) == "string" then
		self.db:check(sqlite3.sqlite3_bind_blob(self.stmt, n, value, len or #value, sqlite3_transient))
	elseif type(value) == "cdata" then
		self.db:check(sqlite3.sqlite3_bind_blob(self.stmt, n, value, len, sqlite3_transient))
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
	if r == sqlite3.SQLITE_OK then
		self.stmt = nil
		self.db.stmts[self] = nil
	else
		self.db:check(r)
	end
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
		tbl[i+1] = self:get_value(i)
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
	self.db:check(sqlite3.sqlite3_reset(self.stmt))
end

function sqlite_stmt:rows()
	return function()
		if self:step() then
			return self:get_values()
		else
			return nil
		end
	end
end

function sqlite_stmt:rows_unpacked()
	return function()
		if self:step() then
			return self:get_values_unpacked()
		else
			return nil
		end
	end
end

function sqlite_stmt:step()
	return self.db:checkstep(sqlite3.sqlite3_step(self.stmt))
end

-- TODO: stmt:urows

function sqlite_stmt:clear_bindings()
	self.db:check(sqlite3.sqlite3_clear_bindings(self.stmt))
end

-- -------------------------- Blob Methods -------------------------- --

function sqlite_blob:read(numbytes, offset, buffer)
	numbytes = numbytes or #self
	buffer = buffer or new_bytearr(numbytes)
	self.db:check(sqlite3.sqlite3_blob_read(self.blob, buffer, numbytes, offset or 0))
	return buffer
end

function sqlite_blob:length()
	return sqlite3.sqlite3_blob_bytes(self.blob)
end
sqlite_blob.__len = sqlite_blob.length

function sqlite_blob:write(offset, data, datalen)
	if type(data) == "string" then
		datalen = #data
		data = new_bytearr(datalen, data)
	else assert(datalen and type(datalen) == "number") end
	self.db:check(sqlite3.sqlite3_blob_write(self.blob, data, datalen, offset))
end

function sqlite_blob:close()
	local r = sqlite3.sqlite3_blob_close(self.blob)
	if r == sqlite3.SQLITE_OK then
		self.blob = nil
		self.db.blobs[self] = nil
	else
		self.db:check(r)
	end
end

function sqlite_blob:reopen(row)
	self.db:check(sqlite3.sqlite3_blob_reopen(self.blob, row))
end

return lsqlite3
