# ???

If you don't know what's that, then you'll need to use it as example of how to
write programs using tarantool-pregel.

## How to start.

You can start workers using ``./workers.sh start <count>``
Configure remote hosts in ``constants.lua`` for now. Parameters:

* ``HOSTS_LIST``
* ``INSTANCE_COUNT``
* ``MASTER_URI``

Stop this workers using ``./workers.sh stop <count>``

> :ledger:
>
> If you can't start/restart workers, (waiting indefinetly on first run and
> `waiting tarantool to start` - you may have problems with Lua config file
> or maybe you need to comment out 39 line of ``workers.sh`` (wait_lauched.py))

When you'll need to start master - simply do:

```
> tarantool wrapper.lua load # for loading phase
# OR
> tarantool wrapper.lua # without loading files inside.
```

## Structure:

* ``avro_loaders.lua`` - loader for Avro file format. Also, it can generate
	data, if you don't have one (in the end you'll get some garbage, if you dont
	have data, but it's no problem, it's just a showcase :) ).
* ``constants.lua`` - every constant, that's needed for this algorithm. For example:

	- ``vertex_type`` - one of ``MASTER``, ``TASK``, ``DATA``
	- ``message_command`` - one of ``NONE``, ``FETCH``, ``PREDICT_CALIBRATION``,
		``PREDICT`` or ``TERMINATE``
	- ``node_status`` - one of ``UNKNOWN``, ``NEW``, ``WORKING`` or ``INACTIVE``
	- ``task_phase`` - ``SELECTION``, ``TRAINING``, ``CALIBRATION``,
		``PREDICTION`` or ``DONE``
	- e.t.c

* ``node_*.lua`` - Node 'class'. Vertice in pregel becomes this Node type in
	``compute`` function.

	[Code part](https://github.com/bigbes/dm-gd/blob/master/common.lua#L314-L333):
	```lua
	local vertex_mt      = nil
	local node_master_mt = require('node_master').mt
	local node_task_mt   = require('node_task').mt
	local node_data_mt   = require('node_data').mt

	local function computeGradientDescent(vertex)
		if vertex_mt == nil then
			vertex_mt = getmetatable(vertex)
		end

		local vtype = vertex:get_value().vtype

		if vtype == vertex_type.MASTER then
			setmetatable(vertex, node_master_mt)
		elseif vtype == vertex_type.TASK then
			if vertex:get_superstep() == 0 then
				return
			end
			setmetatable(vertex, node_task_mt)
		else
			setmetatable(vertex, node_data_mt)
		end
		vertex:compute_new()
		setmetatable(vertex, vertex_mt)
	end
	```

	Basic node structure is:

	* ``key`` - is a key of current node. It consists of one (``vid: <vid_val>``,
		``okid: <okid_val>``, ``vkid: <vkid_val>`` or ``email: <email_val>``) and
		category - integer value.  
		``vid`` may have special value: ``MASTER:`` or ``<task_name>``. We'll see
		it in the future.
	* ``features`` - fixed-width list of floats/doubles to store markers in
	* ``vtype`` - ``vertex_type`` in constants.
	* ``status`` - ``node_status`` in constants.

	- ``node_common.lua`` - common methods for all Nodes:

		~ ``node:get_status()`` - get status of node (``node_status``)
		~ ``node:set_status(status)`` - It'll set new status for ``Node``
		~ ``node:compute_new()`` - start point (check status and choose what to do)
		~ ``node:predictRaw(parameters)`` - make raw prediction based on
			given parameters and local features
		~ ``node:predictCalibrateed(parameters, calibrationBucketPercents)`` -
			make calibrated prediction

	- ``node_master.lua`` - Master Node. It works only once, on first superstep.
		It loads all task nodes.
	- ``node_task.lua`` - main computing section. It has multiple phases:

		* ``SELECTION`` - fetch features from the test sample.
		* ``TRAINING`` - If messages came back, then we need to start training on data,
		  that we've fetched.
		* ``CALIBRATION`` - calibrate results, that we've fetched and trained.
		  Send everyone message using ``aggregator`` and count AUC for given
			parameters.
		* ``PREDICTION`` - wait for an answer from everyone. They'll send calibrated
			data back.
		* ``DONE`` - everything is done, stop and end execution.

	- ``node_data.lua`` - It's the most common type of Node. It stores user data
		and works only on ``TRAINING`` and ``PREDICTION`` phase. It counts raw
		prediction and calibrated prediction on phases respectively.

## TODO:

* Use [torch](http://torch.ch) for features and GD and AUC computing.
* Try not to extract all data from value, if need only one value (Usage of C API)
* Ability to extract reports from workers.
