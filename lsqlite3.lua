
assert(jit, "lsqlite3_ffi must run on LuaJIT!")
local ffi = require "ffi"

ffi.cdef(assert(io.read((LSQLITE3_FFI_PATH or "").."/sqlite3.ffi")):read("*a"))
local sqlite3 = ffi.load("sqlite3",true)
local new_db_ptr = ffi.typeof("sqlite3*[1]")
local new_stmt_ptr = ffi.typeof("sqlite3_stmt*[1]")
local new_exec_ptr = ffi.typeof("int (*callback)(void*,int,char**,char**)")

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
-- TODO: db:prepare
-- TODO: db:progress_handler
-- TODO: db:rows



return lsqlite3
