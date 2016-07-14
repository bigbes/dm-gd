local ffi = require('ffi')

-- common constants
ffi.cdef[[
    struct gd_vertex_type {
        static const int MASTER = 0x00;
        static const int TASK   = 0x01;
        static const int DATA   = 0x02;
    };
    struct gd_message_command {
        static const int NONE                = 0x00;
        static const int FETCH               = 0x01;
        static const int PREDICT_CALIBRATION = 0x02;
        static const int PREDICT             = 0x03;
        static const int TERMINATE           = 0x04;
    };
    struct gd_node_status {
        static const int UNKNOWN  = 0x00;
        static const int NEW      = 0x01;
        static const int WORKING  = 0x02;
        static const int INACTIVE = 0x03;
    };
    struct gd_task_phase {
        static const int SELECTION   = 0x00;
        static const int TRAINING    = 0x01;
        static const int CALIBRATION = 0x02;
        static const int PREDICTION  = 0x03;
        static const int DONE        = 0x04;
    };
]]
local vertex_type     = ffi.new('struct gd_vertex_type')
local message_command = ffi.new('struct gd_message_command')
local node_status     = ffi.new('struct gd_node_status')
local task_phase      = ffi.new('struct gd_task_phase')
-- keys of dataSet
local dataSetKeys            = {'vid', 'email', 'okid', 'vkid'}
-- config keys
local FEATURES_LIST          = "features.list"
local TASKS_CONFIG_HDFS_PATH = "tasks.config.hdfs.path"
-- local DATASET_PATH        = '/Users/blikh/src/work/pregel-data/tarantool-test'
-- local DATASET_PATH           = '/home/taransible/bigbes-work/pregel-avro-test'
local DATASET_PATH           = '/home/taransible/bigbes-work/pregel-avro-test-big'

-- other
local SUFFIX_TRAIN           = "train"
local SUFFIX_TEST            = "test"
local DISTRIBUTED_GD_GROUP   = "Distributed GD"
local MISSING_USERS_COUNT    = "Missing users"
local MASTER_VERTEX_TYPE     = "MASTER"
local TASK_VERTEX_TYPE       = "TASK"
-- Parameters of gradient descend / algorithm
local GDParams = nil
do
    local __params = {
        ['max.dataset.size']               = 30000,
        ['max.gd.iter']                    = 300,
        ['gd.loss.averaging.factor']       = 0.2,
        ['gd.loss.convergence.factor']     = 1e-4,
        ['train.batch.size']               = 500,
        ['test.batch.size']                = 300,
        ['negative.vertices.fraction']     = 0.2,
        ['test.vertices.fraction']         = 0.05,
        ['n.calibration.vertices']         = 10000,
        ['p.report.prediction']            = 1e-3,
        -- this should be a divisor of 0
        ['calibration.bucket.percents']    = 5.0,
        ['max.predicted.calibrated.value'] = 1000
    }
    GDParams = setmetatable({}, {
        __index = function(self, key)
            local value = __params[key]
            if value == nil then
                error(string.format('undefined constant "%s"', tostring(key)))
            end
            return value
        end,
        __newindex = function(self, key, val)
            error('trying to modify read-only table')
        end
    })
end

return {
    vertex_type            = vertex_type,
    message_command        = message_command,
    node_status            = node_status,
    task_phase             = task_phase,
    dataSetKeys            = dataSetKeys,
    FEATURES_LIST          = FEATURES_LIST,
    TASKS_CONFIG_HDFS_PATH = TASKS_CONFIG_HDFS_PATH,
    DATASET_PATH           = DATASET_PATH,
    SUFFIX_TRAIN           = SUFFIX_TRAIN,
    SUFFIX_TEST            = SUFFIX_TEST,
    MASTER_VERTEX_TYPE     = MASTER_VERTEX_TYPE,
    DISTRIBUTED_GD_GROUP   = DISTRIBUTED_GD_GROUP,
    MISSING_USERS_COUNT    = MISSING_USERS_COUNT,
    TASK_VERTEX_TYPE       = TASK_VERTEX_TYPE,
    GDParams               = GDParams
}
