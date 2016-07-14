local fio = require('fio')
local fun = require('fun')
local log = require('log')
local json = require('json')
local uuid = require('uuid')
local fiber = require('fiber')
local clock = require('clock')

local pavro = require('pregel.avro')
local ploader = require('pregel.loader')

local utils = require('utils')
local constants = require('constants')

local FEATURE_COUNT = 300
local VERTEX_COUNT  = 16000000

--[[--
-- Avro schema (in JSON representation) is:
-- {
--     'type': 'record',
--     'name': 'KeyValuePair',
--     'namespace': 'org.apache.avro.mapreduce',
--     'fields': [{
--         'name': 'key',
--         'type': {
--             'type': 'record',
--             'name': 'User',
--             'namespace': 'ru.mail.avro',
--             'fields': [{
--                 'name': 'vid',
--                 'type': {'type': 'string'}
--             }, {
--                 'name': 'okid',
--                 'type': [
--                     {'type': 'null'},
--                     {'type': 'string'}
--                 ]
--             }, {
--                 'name': 'email',
--                 'type': [
--                     {'type': 'null'},
--                     {'type': 'string'}
--                 ]
--             }, {
--                 'name': 'vkid',
--                 'type': [
--                     {'type': 'null'},
--                     {'type': 'string'}
--                 ]
--             }, {
--                 'name': 'category',
--                 'type': [
--                     {'type': 'null'},
--                     {'type': 'int'}
--                 ]
--             }, {
--                 'name': 'start',
--                 'type': [
--                     {'type': 'null'},
--                     {'type': 'long'}
--                 ]
--             }, {
--                 'name': 'end',
--                 'type': [
--                     {'type': 'null'},
--                     {'type': 'long'}
--                 ]
--             }],
--         }
--     }, {
--         'name': 'value',
--         'type': {
--             'type': 'record',
--             'name': 'SparseFeatureVector',
--             'namespace': 'ru.mail.avro',
--             'fields': [{
--                 'name': 'features',
--                 'type': {
--                     'type': 'array',
--                     'items': {
--                         'type': 'record',
--                         'name': 'Feature',
--                         'fields': [{
--                             'name': 'feature_id',
--                             'type': {'type': 'string'}
--                         }, {
--                             'name': 'value',
--                             'type': [
--                                 {'type': 'double'},
--                                 {'type': 'null'}
--                             ]
--                         }, {
--                             'name': 'timestamps',
--                             'type': [{
--                                 'type': 'array',
--                                 'items': {'type': 'int'}
--                             }, {
--                                 'type': 'null'
--                             }]
--                         }]
--                     }
--                 }
--             }],
--         }
--     }]
-- }
--]]--

local function append_feature_vector(feature_count, features)
    if #features < feature_count then
        for k = #features + 1, feature_count do
            table.insert(features, math.random() * math.random(-30, 30))
        end
    end
end

local function process_avro_file(self, filename, cnt_cur, cnt_all, feature_count)
    log.info('%03d/%03d processing %s', cnt_cur, cnt_all, filename)
    local avro_file = pavro.open(filename)
    local count = 0
    local begin_time = clock.time()
    while true do
        local line = avro_file:read_raw()
        if line == nil then break end
        assert(line:type() == pavro.RECORD and
                line:schema_name() == 'KeyValuePair')
        local key_object = {}
        local fea_object = {}
        -- parse key
        do
            local key = line:get('key')
            assert(key ~= nil and
                    key:type() == pavro.RECORD and
                    key:schema_name() == 'User')
            for _, v in ipairs{'okid', 'email', 'vkid'} do
                local obj = key:get(v)
                assert(obj:type() == pavro.UNION)
                local obj_value = obj:get():get()
                key_object[v] = obj_value
            end
            -- set category
            local category = key:get('category')
            assert(category ~= nil and
                    category:type() == pavro.UNION)
            local category_value = category:get()
            -- local category_value = category:get('int')
            key_object.category = category_value:get()
            -- set vid
            local vid = key:get('vid')
            assert(vid ~= nil and
                    vid:type() == pavro.STRING)
            local vid_value = vid:get()
            key_object.vid = vid_value
        end
        -- parse value
        do
            local val = line:get('value')
            assert(val:type() == pavro.RECORD and
                    val:schema_name() == 'SparseFeatureVector')
            local features = val:get('features')
            assert(features ~= nil and
                    features:type() == pavro.ARRAY)
            for index, feature in features:iterate() do
                assert(feature ~= nil and
                        feature:type() == pavro.RECORD and
                        feature:schema_name() == 'Feature')
                local fid  = feature:get('feature_id'):get()
                fid = tonumber(fid:match('SVD_(%d+)')) + 1
                local fval = feature:get('value'):get():get()
                local tst  = feature:get('timestamp')
                assert(tst == nil, 'timestamp is not nil')
                fea_object[fid] = fval
            end
            if feature_count ~= nil then
                append_feature_vector(feature_count, fea_object)
            end
        end
        local vtype = constants.vertex_type.DATA
        if type(key_object.vid) == 'string' and
           #key_object.vid > 0 and
           key_object.vid == constants.MASTER_VERTEX_TYPE then
            vtype = constants.vertex_type.MASTER
        end
        local vertex = {
            key      = key_object,
            features = fea_object,
            vtype    = vtype,
            status   = constants.node_status.NEW,
        }
        self:store_vertex(vertex)
        line:release()
        count = count + 1
        fiber.yield()
    end
    log.info('done processing %d values in %.3f seconds',
                count, clock.time() - begin_time)
    avro_file:close()
    fiber.yield()
    return count
end

local function master_avro_loader(master, path)
    local function loader(self)
        local avro_path  = fio.pathjoin(path, '*.avro')
        local avro_files = fio.glob(avro_path);
        table.sort(avro_files)
        log.info('%d found files found in path %s', #avro_files, avro_path)
        for idx, filename in ipairs(avro_files) do
            process_avro_file(self, filename, idx, #avro_files)
        end
    end
    return ploader.new(master, loader)
end

local function worker_avro_loader(worker, path)
    local function loader(self, current_idx, worker_count)
        local avro_path  = fio.pathjoin(path, '*.avro')
        local avro_files = fun.iter(fio.glob(avro_path)):filter(function(filename)
            local avrofile_no = tonumber(filename:match('part%-m%-(%d+).avro'))
            if avrofile_no % worker_count == current_idx - 1 then
                return true
            end
            return false
        end):totable()
        table.sort(avro_files)
        log.info('%d found files found in path %s', #avro_files, avro_path)
        for idx, filename in ipairs(avro_files) do
            process_avro_file(self, filename, idx, #avro_files)
        end
    end
    return ploader.new(worker, loader)
end

local function generate_random_features(feature_count)
    return fun.range(feature_count):map(function()
        return math.random() * math.random(-30, 30)
    end):totable()
end

local function generate_random_name()
    local name = {
        vid   = '',
        email = uuid.str(),
    }
    local b = math.random(0, 1000000)
    if b % 739 == 0 then
        name['vkid'] = math.random(200000, 10000000)
    elseif b % 839 == 0 then
        name['okid'] = math.random(200000, 10000000)
    end
    return name
end

local function generate_random_vertex(feature_count)
    return {
        key      = generate_random_name(),
        features = generate_random_features(feature_count),
        vtype    = constants.vertex_type.DATA,
        status   = constants.node_status.NEW
    }
end

local function worker_additional_avro_loader(worker, opts)
    assert(type(opts) == 'table')
    assert(type(opts.path) == 'string')
    local path = opts.path
    local feature_count = opts.feature_count or FEATURE_COUNT
    local vertex_count  = opts.vertex_count  or nil
    local function loader(self, current_idx, worker_count)
        local avro_path  = fio.pathjoin(path, 'tokens', '*.avro')
        local avro_files = fun.iter(fio.glob(avro_path)):filter(function(filename)
            local avrofile_no = tonumber(filename:match('part%-m%-(%d+).avro'))
            if avrofile_no % worker_count == current_idx - 1 then
                return true
            end
            return false
        end):totable()
        table.sort(avro_files)
        log.info('%d found files found in path %s', #avro_files, avro_path)
        local vertex_processed = 0
        for idx, filename in ipairs(avro_files) do
            vertex_processed = vertex_processed + process_avro_file(self, filename, idx,
                                                                    #avro_files, feature_count)
        end
        if vertex_count and vertex_processed < vertex_count then
            vertex_count = vertex_count - vertex_processed
            vertex_count = math.floor(vertex_count / worker_count)
            fun.range(vertex_count):each(function(id)
                if id % 100 == 0 then
                    fiber.yield()
                end
                if id % 100000 == 0 then
                    log.info('<preload> generated %d/%d vertices', id, vertex_count)
                end
                local vertex = generate_random_vertex(feature_count)
                self:store_vertex(vertex)
            end)
        end
    end
    return ploader.new(worker, loader)
end

return {
    master = master_avro_loader,
    worker = worker_avro_loader,
    worker_additional = worker_additional_avro_loader
}
