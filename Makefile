# TNT_PATH = /Users/blikh/src/work/tarantool/src/tarantool
TNT_PATH := tarantool
COUNT    := 8

clean_lr:
	rm -f var/*.log
	LUA_PATH="`pwd`\?.lua;`pwd`\?\init.lua;;" ./workers.sh restart $(COUNT)
	sleep 0.5
	$(TNT_PATH) common.lua
	LUA_PATH="`pwd`\?.lua;`pwd`\?\init.lua;;" ./workers.sh stop $(COUNT)

clean_all:
	LUA_PATH="`pwd`\?.lua;`pwd`\?\init.lua;;" ./workers.sh stop $(COUNT)
	rm -rf var/
	LUA_PATH="`pwd`\?.lua;`pwd`\?\init.lua;;" ./workers.sh start $(COUNT)
	sleep 0.5
	$(TNT_PATH) common.lua load
	LUA_PATH="`pwd`\?.lua;`pwd`\?\init.lua;;" ./workers.sh stop $(COUNT)

all: clean_lr
