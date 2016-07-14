local fun = require('fun')
local log = require('log')

local constants       = require('constants')
local node_status     = constants.node_status
local GDParams        = constants.GDParams

local node_common_methods = {
    get_status = function(self)
        return self:get_value().status or node_status.UNKNOWN
    end,
    set_status = function(self, status)
        local v = self:get_value()
        v.status = status
        self:set_value(v)
    end,
    compute_new = function(self)
        self:__init()
        local status = self:get_status()
        if status == node_status.WORKING then
            self:work()
        elseif status == node_status.INACTIVE then
            log.debug('Vertex %s is inactive: doing nothing', self:get_name())
            self:vote_halt()
        else
            assert(false, string.format(
                'Unexpected data vertex status: %s - %d',
                self:get_name(), status
            ))
        end
    end,
    predictRaw = function(self, parameters)
        local features = self:get_value().features
        local prediction = fun.iter(features):zip(parameters)
                              :map(function(feature, parameter)
            return feature * parameter
        end):sum()
        return prediction
    end,
    predictCalibrated = function(self, param, calibrationBucketPercents)
        local features = self:get_value().features
        local dim = #features
        local nPercentiles = math.floor(100 / calibrationBucketPercents) - 1
        local percentileStep = GDParams['max.predicted.calibrated.value'] / 100
              percentileStep = math.floor(percentileStep * calibrationBucketPercents)
        local prediction = self:predictRaw(param)

        local calibratedPrediction = nil
        -- log.info('param_len %d', #param)
        -- log.info('featu_len %d', dim)
        for i = 1, nPercentiles do
            if prediction < param[dim + i] then
                calibratedPrediction = math.random(0, percentileStep) + i * percentileStep
                break
            end
        end
        if calibratedPrediction == nil then
            calibratedPrediction = math.random(0, percentileStep)
            calibratedPrediction = calibratedPrediction + nPercentiles * percentileStep
        end
        return calibratedPrediction
    end
}

return {
    methods = node_common_methods
}
