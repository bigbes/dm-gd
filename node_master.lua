local log = require('log')

local vertex_methods = require('pregel.vertex').vertex_methods

local constants   = require('constants')
local node_status = constants.node_status
local vertex_type = constants.vertex_type

local node_common_methods = require('node_common').methods

local node_master_methods = {
    __init = function(self)
        self:set_status(node_status.WORKING)
        log.info('<MASTER> Initializing')
    end,
    work = function(self)
        local value = self:get_value()
        local wc = self:get_worker_context()

        if self:get_superstep() == 1 then
            for name, _ in pairs(wc.taskPhases) do
                log.info('<MASTER> Adding task vertex %s', name)
                self:add_vertex({
                    name     = name,
                    vtype    = vertex_type.TASK,
                    features = value.features,
                    status   = node_status.NEW
                })
            end
        end
        self:set_status(node_status.INACTIVE)
        self:vote_halt()
    end,
}

local node_master_mt = {
    __index = {}
}

for k, v in pairs(vertex_methods) do
    node_master_mt.__index[k] = v
end

for k, v in pairs(node_common_methods) do
    node_master_mt.__index[k] = v
end

for k, v in pairs(node_master_methods) do
    node_master_mt.__index[k] = v
end

return {
    mt = node_master_mt
}
