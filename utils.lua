local fun  = require('fun')
local log  = require('log')
local json = require('json')

local constants = require('constants')

local vertex_type = constants.vertex_type
local dataSetKeys = constants.dataSetKeys

local MASTER_VERTEX_TYPE = constants.MASTER_VERTEX_TYPE
local TASK_VERTEX_TYPE   = constants.TASK_VERTEX_TYPE

local function obtain_type(name)
    return name:match('(%a+):(%w*)')
end

local function obtain_name(value)
    if value.vtype == vertex_type.MASTER then
        return ('%s:%s'):format(MASTER_VERTEX_TYPE, nil)
    elseif value.vtype == vertex_type.TASK then
        return ('%s:%s'):format(TASK_VERTEX_TYPE, value.name)
    end
    for _, name in ipairs(dataSetKeys) do
        local key_value = value.key[name]
        if key_value ~= nil and
           type(key_value) == 'string' and
           #key_value > 0 then
            if key_value == 'vid' and name == MASTER_VERTEX_TYPE then
                return 'MASTER:'
            end
            return ('%s:%s'):format(name, key_value)
        end
    end
    assert(false)
end

local function math_round(fnum)
    return (fnum % 1 >= 0.5) and math.ceil(fnum) or math.floor(fnum)
end

local function log_features(prefix, features, in_one_line)
    in_one_line = in_one_line or 7
    for i = 1, math.ceil(#features/in_one_line) do
        local ln = fun.iter(features):drop((i - 1) * in_one_line):take(in_one_line):totable()
        log.info('<%s> %d: %s', prefix, i, json.encode(ln))
    end
end

return {
    obtain_type  = obtain_type,
    obtain_name  = obtain_name,
    math_round   = math_round,
    log_features = log_features
}
