local ffi = require('ffi')
local fio = require('fio')
local fun = require('fun')
local log = require('log')
local json = require('json')
local yaml = require('yaml')
local clock = require('clock')
local errno = require('errno')
local fiber = require('fiber')
local digest = require('digest')

local pmaster   = require('pregel.master')
local pworker   = require('pregel.worker')
local avro      = require('pregel.avro')

local xpcall_tb = require('pregel.utils').xpcall_tb
local deepcopy  = require('pregel.utils.copy').deep

local GradientDescent   = require('math.gd').GradientDescent
local PercentileCounter = require('math.pc').PercentileCounter

local TaskReport_new    = require('report').new

local avro_loaders = require('avro_loaders')
local constants    = require('constants')
local utils        = require('utils')

local worker, port_offset = arg[0]:match('(%a+)-(%d+)')
port_offset = port_offset or 0

if worker == 'worker' then
    box.cfg{
        wal_mode           = 'none',
        slab_alloc_arena   = 3,
        -- slab_alloc_maximal = 4*1024*1024,
        listen             = '0.0.0.0:' .. tostring(3301 + port_offset),
        background         = true,
        logger_nonblock    = true
    }
else
    box.cfg{
        slab_alloc_arena   = 0.1,
        wal_mode           = 'none',
        listen             = '0.0.0.0:' .. tostring(3301 + port_offset),
        logger_nonblock    = true
    }
end

box.schema.user.grant('guest', 'read,write,execute', 'universe', nil, {
    if_not_exists = true
})

--[[------------------------------------------------------------------------]]--
--[[--------------------------------- Utils --------------------------------]]--
--[[------------------------------------------------------------------------]]--

local math_round   = utils.math_round
local log_features = utils.log_features

local NULL = json.NULL

--[[------------------------------------------------------------------------]]--
--[[--------------------------- Job configuration --------------------------]]--
--[[------------------------------------------------------------------------]]--

local vertex_type            = constants.vertex_type
local message_command        = constants.message_command
local node_status            = constants.node_status
local task_phase             = constants.task_phase
-- keys of dataSet
local dataSetKeys            = constants.dataSetKeys
-- config keys
local FEATURES_LIST          = constants.FEATURES_LIST
local TASKS_CONFIG_HDFS_PATH = constants.TASKS_CONFIG_HDFS_PATH
local DATASET_PATH           = constants.DATASET_PATH

-- other
local SUFFIX_TRAIN           = constants.SUFFIX_TRAIN
local SUFFIX_TEST            = constants.SUFFIX_TEST
local DISTRIBUTED_GD_GROUP   = constants.DISTRIBUTED_GD_GROUP
local MISSING_USERS_COUNT    = constants.MISSING_USERS_COUNT
local MASTER_VERTEX_TYPE     = constants.MASTER_VERTEX_TYPE
local TASK_VERTEX_TYPE       = constants.TASK_VERTEX_TYPE
-- Parameters of gradient descend / algorithm
local GDParams               = constants.GDParams

--[[------------------------------------------------------------------------]]--
--[[----------------------------- Worker Context ---------------------------]]--
--[[------------------------------------------------------------------------]]--

local wc = nil
do
    local featureList           = {}
    local featureMap            = {}
    local randomVertexIds       = {} -- List of vertices
    local taskPhases            = {} -- taskName -> phase
    local taskDeploymentConfigs = {} -- taskName:uid_type -> config
    local taskDataSet           = {} -- taskName -> data_set_path
    local taskReport            = {} -- taskName -> report

    local predictionReportSamplingProb = GDParams['p.report.prediction']
    local calibrationBucketPercents    = GDParams['calibration.bucket.percents']

    -- Open/Parse file with features
    local fd = io.open(fio.pathjoin(DATASET_PATH, 'features.txt'))
    assert(fd, "Can't open file for reading")
    local line, input = fd:read('*a'), nil
    assert(line, "Bad input")
    local n = 1
    while true do
        if line:match('\n') ~= nil then
            input, line = line:match('([^\n]+)\n(.*)')
        elseif #line > 0 then
            input = line
            line = ''
        else
            break
        end
        table.insert(featureList, input)
        featureMap[input] = n
        n = n + 1
    end
    log.info("<worker_context> Found %d features", #featureList)
    fd:close()

    local fd = io.open(fio.pathjoin(DATASET_PATH, 'prediction_config.json'))
    assert(fd, "Can't open file for reading")
    local input = json.decode(fd:read('*a'))
    assert(input, "Bad input")
    fd:close()

    local function check_file(fname)
        local file = fio.open(fname, {'O_RDONLY'})
        if file == nil then
            local errstr = "Can't open file '%s' for reading: %s [errno %d]"
            errstr = string.format(errstr, fname, errno.strerror(), errno())
            error(errstr)
        end
        file:close()
        return fname
    end

    for _, task_config in ipairs(input) do
        local name  = task_config['name']
        local input = task_config['input']
        for _, deployment in ipairs(task_config['deployment']) do
            local key = ('%s:%s'):format(name, deployment['user_id_type'])
            taskDeploymentConfigs[key] = deployment
        end
        taskPhases[name] = task_phase.SELECTION
        taskReport[name] = TaskReport_new(name)
        log.info("<worker_context> Added config for '%s'", name)
    end

    local dataSetKeysTypes = {'email', 'okid', 'vkid'}

    wc = setmetatable({
        featureList                  = featureList,
        featureMap                   = featureMap,
        randomVertexIds              = randomVertexIds,
        taskReport                   = taskReport,
        taskPhases                   = taskPhases,
        taskDeploymentConfigs        = taskDeploymentConfigs,
        taskDataSet                  = taskDataSet,
        calibrationBucketPercents    = calibrationBucketPercents,
        predictionReportSamplingProb = predictionReportSamplingProb,
    }, {
        __index = {
            addRandomVertex = function(self, vertexId)
                table.insert(self.randomVertexIds, vertexId)
            end,
            iterateRandomVertexIds = function(self)
                return ipairs(self.randomVertexIds)
            end,
            setTaskPhase = function(self, name, phase)
                self.taskPhases[name] = phase
            end,
            getTaskPhase = function(self, name)
                return self.taskPhases[name] or task_phase.SELECTION
            end,
            iterateDataSet = function(self, name)
                local function processDataSet(input)
                    local category = input.category and input.category.int
                    local output = fun.iter(dataSetKeysTypes):map(function(fname)
                        if input[fname] ~= nil then
                            return {fname, input[fname].string, category}
                        end
                    end):totable()
                    local vid = input['vid']
                    if vid and #vid > 0 then
                        table.insert(output, {'vid', vid, category})
                    end
                    return output
                end

                local file  = fio.open(self.taskDataSet[name][1], {'O_RDONLY'})
                local line  = ''
                local errstr = "Can't open file '%s' for reading: %s [errno %d]"
                errstr = string.format(errstr, self.taskDataSet[name][1],
                                       errno.strerror(), errno())
                assert(file ~= nil, errstr)
                local function iterator()
                    while true do
                        local input
                        if line:find('\n') == nil then
                            local rv = file:read(65536)
                            if #rv == 0 then
                                file:close()
                                return nil
                            else
                                line = line .. rv
                            end
                        else
                            input, line = line:match('([^\n]*)\n(.*)')
                            input = json.decode(input)
                            return processDataSet(input)
                        end
                    end
                end
                return iterator, nil
            end,
            iterateDataSetWrap = function(self, name)
                local iter_func = self:iterateDataSet(name)
                local last_item = {}
                local category  = 1
                local iterator = function()
                    if last_item[1] == nil then
                        last_item = iter_func()
                        if last_item == nil then
                            return
                        end
                    end
                    return table.remove(last_item)
                end
                return iterator, nil
            end,
            storeDataSet = function(self, name)
                for tuple in self:iterateDataSetWrap(name) do
                    self.taskDataSet[name][2]:replace(tuple)
                end
            end,
            addAggregators = function(self, instance)
                log.info("<worker_context> Adding aggregators")
                for taskName, _ in pairs(self.taskPhases) do
                    instance:add_aggregator(taskName, {
                        default = {
                            name     = nil,
                            command  = message_command.NONE,
                            target   = 0.0,
                            features = {}
                        },
                        merge   = function(old, new)
                            if new ~= nil and
                               (old == nil or new.command > old.command) then
                                return deepcopy(new)
                            end
                            return old
                        end
                    })
                end
                return instance
            end,
            getTaskReport = function(self, name)
                return self.taskReport[name]
            end
        }
    })

    if worker == 'worker' then
        for _, task_config in ipairs(input) do
            local name   = task_config['name']
            local input  = task_config['input']
            local fname  = check_file(fio.pathjoin(DATASET_PATH, input))
            local sname  = ('wc_%s_ds'):format(name)
            local fspace = box.space[sname]
            taskDataSet[name] = {fname, fspace}
            if box.space[sname] == nil then
                local space = box.schema.create_space(sname, {
                    format = {
                        [1] = {name = 'id_type',  type = 'str'},
                        [2] = {name = 'id',       type = 'str'},
                        [3] = {name = 'category', type = 'num'}
                    }
                })
                space:create_index('primary', {
                    type  = 'TREE',
                    parts = {1, 'STR', 2, 'STR'}
                })
                taskDataSet[name][2] = space
                log.info("<worker_context> Begin preloading data for '%s'", name)
                wc:storeDataSet(name)
                log.info("<worker_context> Data stored for '%s'", name)
            end
            log.info("<worker_context> Done loading dataSet for '%s'", name)
        end
    end

    log.info('<worker_context> Initialized:')
    log.info('<worker_context> taskDeploymentConfigs:')
    fun.iter(taskDeploymentConfigs):each(function(name, config)
        log.info('<worker_context> %s -> %s', name, json.encode(config))
    end)
end

--[[------------------------------------------------------------------------]]--
--[[------------------------ Configuration of Runner -----------------------]]--
--[[------------------------------------------------------------------------]]--

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

local function generate_worker_uri(cnt)
    cnt = cnt or 8
    local sh6servers = fun.range(cnt):map(function(k)
        return 'sh6.tarantool.org:' .. tostring(3301 + k)
    end)
    local sh7servers = fun.range(cnt):map(function(k)
        return 'sh7.tarantool.org:' .. tostring(3301 + k)
    end)
    local sh8servers = fun.range(cnt):map(function(k)
        return 'sh8.tarantool.org:' .. tostring(3301 + k)
    end)
    return sh6servers:chain(sh7servers):chain(sh8servers):totable()
end

--[[--
local function generate_worker_uri(cnt)
    return fun.range(cnt or 4):map(function(k)
        return 'localhost:' .. tostring(3301 + k)
    end):totable()
end
--]]--

local common_cfg = {
    master         = 'sh7.tarantool.org:3301',
    workers        = generate_worker_uri(23),
    compute        = computeGradientDescent,
    combiner       = nil,
    master_preload = avro_loaders.master,
    worker_preload = avro_loaders.worker_additional,
    preload_args   = {
        path          = DATASET_PATH,
        feature_count = 300,
        -- vertex_count  = 17600000,
    },
    squash_only    = false,
    pool_size      = 250,
    delayed_push   = false,
    obtain_name    = utils.obtain_name,
    worker_context = wc
}

if worker == 'worker' then
    worker = pworker.new('test', common_cfg)
    wc:addAggregators(worker)
else
    xpcall_tb(function()
        local master = pmaster.new('test', common_cfg)
        wc:addAggregators(master)
        master:wait_up()
        if arg[1] == 'load' then
            -- master:preload()
            master:preload_on_workers()
            master:save_snapshot()
        end
        master.mpool:by_id('MASTER:'):put('vertex.store', {
            key      = {vid = 'MASTER', category = 0},
            features = fun.duplicate(0.0):take(#wc.featureList):totable(),
            vtype    = constants.vertex_type.MASTER,
            status   = constants.node_status.NEW
        })
        master.mpool:flush()
        master:start()
    end)
    os.exit(0)
end
