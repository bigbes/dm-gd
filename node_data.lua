local log = require('log')
local fun = require('fun')
local json = require('json')
local digest = require('digest')

local vertex_methods = require('pregel.vertex').vertex_methods

local NULL = json.NULL

local constants       = require('constants')
local message_command = constants.message_command
local node_status     = constants.node_status
local GDParams        = constants.GDParams

local node_common_methods = require('node_common').methods

local node_data_methods = {
    __init = function(self)
        local wc = self:get_worker_context()
        local status = self:get_status()
        local negativeVerticesFraction = GDParams['negative.vertices.fraction']
        if status == node_status.NEW then
            local hash = digest.crc32(self:get_name()) % 1000
            if hash <= 1000 * negativeVerticesFraction then
                wc:addRandomVertex(self:get_name())
            end
            self:set_status(node_status.WORKING)
        elseif status == node_status.UNKNOWN then
            self:set_status(node_status.INACTIVE)
        end
    end,
    work = function(self)
        local wc = self:get_worker_context()
        for _, msg in self:pairs_messages() do
            -- return feature vector to master
            if msg.command == message_command.FETCH then
                -- log.info("<data node, '%s'->'%s'> processing command FETCH",
                --          self:get_name(), msg.sender)
                self:send_message(msg.sender, {
                    sender   = self:get_name(),
                    command  = message_command.NONE,
                    target   = msg.target,
                    features = self:get_value().features
                })
            -- compute raw prediction and return to master
            elseif msg.command == message_command.PREDICT_CALIBRATION then
                -- log.info("<data node, '%s'->'%s'> processing command PREDICT_CALIBRATION",
                --          self:get_name(), msg.sender)
                local prediction = self:predictRaw(msg.features)
                -- log.info('<data node, "%s"> prediction is %f', self:get_name(), prediction)
                self:send_message(msg.sender, {
                    sender   = self:get_name(),
                    command  = message_command.NONE,
                    target   = prediction,
                    features = NULL
                })
            else
                assert(false)
            end
        end

        local isPredictionPhase = false
        local predictions = {}

        -- do calibrated prediction for each task
        for taskName, _ in pairs(wc.taskDataSet) do
            local msg = self:get_aggregation(taskName)

            -- predict and save
            if msg ~= nil and msg.command == message_command.PREDICT then
                -- log.info('<data node, %s> processing command PREDICT from aggregator', self:get_name())
                isPredictionPhase = true
                local calibratedPrediction = self:predictCalibrated(
                    msg.features, wc.calibrationBucketPercents
                )
                -- log.info('<data node, %s> calibratedPrediction %f', self:get_name(), calibratedPrediction)

                -- return to master for report, maybe
                if math.random() < wc.predictionReportSamplingProb then
                    self:send_message(msg.sender, {
                        sender   = self:get_name(),
                        command  = message_command.NONE,
                        target   = calibratedPrediction,
                        features = NULL
                    })
                end

                -- write (audience, score) pair if threshold exceeded
                local dcName = ('%s:%s'):format(taskName, self.idType)
                local dc = wc.taskDeploymentConfigs[dcName]
                local maxPredictedCalibratedValue = GDParams['max.predicted.calibrated.value']
                if (dc ~= nil and
                    calibratedPrediction > maxPredictedCalibratedValue *
                                           (1 - dc.threshold)) then
                    table.insert(predictions, {dc.targeting, calibratedPrediction})
                end
            end
        end

        if isPredictionPhase == true then
            if #predictions > 0 then
                log.info('<data node, %s> Prediction phase done', self:get_name())
                log.info('<data node, %s> %s', self:get_name(), json.encode(predictions))
            end
            -- Update value
            local value = self:get_value()
            value.features = predictions
            self:set_value(value)
            -- Make it inactive
            self:set_status(node_status.INACTIVE)
            self:vote_halt()
        end
    end,
}

local node_data_mt = {
    __index = {}
}

for k, v in pairs(vertex_methods) do
    node_data_mt.__index[k] = v
end

for k, v in pairs(node_common_methods) do
    node_data_mt.__index[k] = v
end

for k, v in pairs(node_data_methods) do
    node_data_mt.__index[k] = v
end

return {
    mt = node_data_mt
}
