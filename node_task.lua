local fun = require('fun')
local log = require('log')
local json = require('json')

local defaultdict = require('pregel.utils.collections').defaultdict

local vertex_methods = require('pregel.vertex').vertex_methods

local constants       = require('constants')
local message_command = constants.message_command
local node_status     = constants.node_status
local task_phase      = constants.task_phase
local GDParams        = constants.GDParams
local SUFFIX_TRAIN    = constants.SUFFIX_TRAIN
local SUFFIX_TEST     = constants.SUFFIX_TEST

local GradientDescent   = require('math.gd').GradientDescent
local PercentileCounter = require('math.pc').PercentileCounter
local MeasureAUC        = require('math.auc').MeasureAUC

local utils        = require('utils')
local math_round   = utils.math_round
local log_features = utils.log_features

local NULL = json.NULL

local node_common_methods = require('node_common').methods

local node_task_methods = {
    __init = function(self)
        local taskName = self:get_value().name
        log.info('<task node, %s> Initializing', taskName)
        if self:get_status() == node_status.NEW then
            self:set_status(node_status.WORKING)
        end
        local space_name = ('task_node_%s_ds'):format(taskName)
        if box.space[space_name] == nil then
            local space = box.schema.create_space(space_name, {
                format = {
                    [1] = {name = 'id',           type = 'num'  },
                    [2] = {name = 'task_name',    type = 'str'  },
                    [3] = {name = 'suffix',       type = 'str'  },
                    [4] = {name = 'target_round', type = 'num'  },
                    [5] = {name = 'target',       type = 'num'  },
                    [6] = {name = 'features',     type = 'array'},
                }
            })
            space:create_index('primary', {
                type  = 'TREE',
                parts = {1, 'NUM'}
            })
            space:create_index('name', {
                type   = 'TREE',
                parts  = {2, 'STR', 3, 'STR', 4, 'NUM'},
                unique = false
            })
        end
        self.dataSetSpace = box.space[space_name]
    end,
    work = function(self)
        local wc       = self:get_worker_context()
        local value    = self:get_value()
        local taskName = self:get_value().name
        local phase    = wc:getTaskPhase(taskName)
        local report   = wc:getTaskReport(taskName)

        if phase == task_phase.SELECTION then

            log.info('<task node, %s> SELECTION phase', taskName)
            for _, task in wc.taskDataSet[taskName][2]:pairs() do
                local ktype, kname, value = task:unpack()
                local name = ('%s:%s'):format(ktype, kname)
                -- log.info('<iterateDataSet, %s> send_message to <%s>', taskName, name)
                self:send_message(name, {
                    sender   = self:get_name(),
                    command  = message_command.FETCH,
                    target   = value,
                    features = NULL
                })
            end
            log.info('<task node, %s> SELECTION phase done', taskName)

            wc:setTaskPhase(taskName, task_phase.TRAINING)
        elseif phase == task_phase.TRAINING then

            log.info('<task node, %s> TRAINING phase', taskName)

            local testVerticesFraction = GDParams['test.vertices.fraction']
            log.info('Test vertices fraction %f', testVerticesFraction)

            local n, recordCounts = self:saveDataSetToLocalSpace(
                taskName, testVerticesFraction
            )

            if n == 0 then
                log.warn('Master didn\'t receive any messages, so no training occured')
                log.warn('Waiting one superstep')
                return
            end

            log.info("TRAINING: %s", json.encode(recordCounts))
            local trainCount, testCount = self:computeTrainTestRecordCounts(recordCounts);
            report.ensemble_report.data.train_size = trainCount
            report.ensemble_report.data.test_size  = testCount
            report.ensemble_report.data.n_features = #value.features

            local d = #value.features
            local trainBatchSize = GDParams['train.batch.size']
            local testBatchSize  = GDParams['test.batch.size']
            local maxIter        = GDParams['max.gd.iter']
            local alpha          = GDParams['gd.loss.averaging.factor']
            local epsilon        = GDParams['gd.loss.convergence.factor']

            local params = self:train(taskName, recordCounts, d, maxIter,
                                      trainBatchSize, testBatchSize, alpha,
                                      epsilon)
            value.features = params
            self:set_value(value)
            report.ensemble_report.ensemble.model = 'express-SGD'

            local nCalibrationMessages = GDParams['n.calibration.vertices']
            log.info('Number of calibration messages: %d', nCalibrationMessages)

            local calibrationProb = math.min(
                1.0 * nCalibrationMessages / #wc.randomVertexIds, 1.0
            )
            log.info('Probability to take message for calibration: %f',
                     calibrationProb)

            local n = 0
            for idx, randomID in wc:iterateRandomVertexIds() do
                if math.random() < calibrationProb then
                    self:send_message(randomID, {
                        sender   = self:get_name(),
                        command  = message_command.PREDICT_CALIBRATION,
                        target   = 0.0,
                        features = value.features,
                    })
                    n = n + 1
                end
            end
            log.info('sent %d calibration messages. Waiting for response', n)

            wc:setTaskPhase(taskName, task_phase.CALIBRATION)
        elseif phase == task_phase.CALIBRATION then

            log.info('<task node, %s> CALIBRATION phase', taskName)
            local ds = PercentileCounter()
            for _, msg, _ in self:pairs_messages() do
                ds:addValue(msg.target)
            end
            if ds:getN() == 0 then
                log.warn('Master didn\'t receive any messages, so no calibration occured')
                log.warn('Waiting one superstep')
                return
            end
            local cbp = GDParams['calibration.bucket.percents']
            log.info('Calibration bucket percents %f', cbp)
            local parametersAndCalibration = self:calibrate(value.features, ds,
                                                            #value.features, cbp)
            log.info('parametersAndCalibration %d', #parametersAndCalibration)
            local broadcast = {
                sender   = self:get_name(),
                command  = message_command.PREDICT,
                target   = 0.0,
                features = parametersAndCalibration
            }
            log.info('<task node, %s> Set aggregator to broadcast model ' ..
                     'across all vertices to:', taskName)
            log.info('<task node, %s> - command - PREDICT', taskName)
            log.info("<task node, %s> - sender: %s", taskName, broadcast.sender)
            log.info('<task node, %s> - features:', taskName)
            log_features(("task node, %s"):format(taskName), broadcast.features)
            self:set_aggregation(taskName, broadcast)

            local category, parameters = self:applyModelToTestDataSets(taskName, parametersAndCalibration)
            if #category == 0 then
                log.warn('No prediction for test data set found')
            end

            local auc = MeasureAUC(category, parameters)
            report.ensemble_report.area_under_roc_weighted_transformed = auc

            log.info('<task node, %s> TRUE:', taskName)
            log_features(("task node, %s"):format(taskName), category)
            log.info('<task node, %s> PRED:', taskName)
            log_features(("task node, %s"):format(taskName), parameters)
            log.info('<task node, %s> AUC: %f', taskName, auc)

            wc:setTaskPhase(taskName, task_phase.PREDICTION)
        elseif phase == task_phase.PREDICTION then

            log.info('<task node, %s> PREDICTION phase', taskName)
            log.info("Calibrated data:")
            local n = 1
            for _, msg in self:pairs_messages() do
                log.info('%d> %f', n, msg.target)
                n = n + 1
            end
            if n == 0 then
                log.info('Master didn\'t receive any messaged, so no prediction occured')
                log.info('Waiting one superstep')
                return
            end

            -- we may cleanup temporary files
            wc:setTaskPhase(taskName, task_phase.DONE)
        elseif phase == task_phase.DONE then

            log.info('<task node, %s> DONE phase', taskName)
            self:vote_halt()

            self:set_status(node_status.INACTIVE)
        else
            assert(false)
        end
    end,
    calibrate = function(self, param, ds, dim, calibrationBucketPercents)
        local nPercentiles = math.floor(100 / calibrationBucketPercents) - 1;
        log.info('Model dimensionality: %d, calibration bucket percents: %f',
                 dim, calibrationBucketPercents)
        log.info('Number of calibration percentiles: %d', nPercentiles)
        local parametersWithCalibration = fun.iter(param):chain(
            fun.range(1, nPercentiles):map(function(p)
                return ds:getPercentile((p + 1) * calibrationBucketPercents)
            end)
        ):totable()
        return parametersWithCalibration
    end,
    train = function(self, taskName, recordounts, dim, maxIter,
                     trainBatchSize, testBatchSize, alpha, epsilon)
        log.info('<task node, %s> Initializing Gradient descent',    taskName)
        log.info('<task node, %s> - train batch size %d',            taskName, trainBatchSize)
        log.info('<task node, %s> - test batch size %d',             taskName, testBatchSize)
        log.info('<task node, %s> - maximum number of iteration %d', taskName, maxIter)
        log.info('<task node, %s> - loss averaging factor %f',       taskName, alpha)
        log.info('<task node, %s> - loss convergence factor %f',     taskName, epsilon)

        local gd = GradientDescent('hinge', 'l2')
        local param = gd:initialize(dim)
        log.info('<task node, %s> initialized model parameters to:', taskName)
        log_features(('task node, %s'):format(taskName), param)

        local trainAverageLoss = nil
        local testAverageLoss  = nil

        local function get_rtargets(self)
            local acc = {n = 0}
            self.dataSetSpace.index.name:pairs{taskName, 'test'}
                                        :each(function(tuple)
                local target_round = tuple[4]
                if acc[acc.n] ~= target_round then
                    table.insert(acc, target_round)
                    acc.n = acc.n + 1
                end
            end)
            return acc
        end

        local rtargets = get_rtargets(self)

        for nIter = 1, maxIter do
            local trainBatchLoss = 0.0
            local testBatchLoss  = 0.0
            local trainBatchGradient = fun.duplicate(0.0):take(dim):totable()
            for _, rtarget in ipairs(rtargets) do
                do
                    local batchSize = testBatchSize
                    while batchSize > 0 do
                        self.dataSetSpace.index.name:pairs{taskName, 'test', rtarget}
                                                    :take(batchSize)
                                                    :all(function(tuple)
                            local target, features = tuple:unpack(5, 6)
                            -- log.info('- msgid: %d', tuple[1])
                            -- log.info('- target: %f', target)
                            -- log.info('- features')
                            -- log_features(('task node, %s'):format(taskName), features, 500)
                            local lg = gd:lossAndGradient(target, features, param)
                            testBatchLoss = testBatchLoss + lg[1] / testBatchSize
                            -- if fun.iter(lg):all(function(val)
                            --      return val == 0
                            -- end) == true then
                            --     log.info('<task node, %s> lossAnd' ..
                            --              'Gradient has zeroed result',
                            --              taskName)
                            -- end
                            batchSize = batchSize - 1
                            -- log.info('"test" message processed, %d left', batchSize)
                            return batchSize > 0
                        end)
                    end
                end
                do
                    local batchSize = trainBatchSize
                    while batchSize > 0 do
                        self.dataSetSpace.index.name:pairs{taskName, 'train', rtarget}
                                                    :take(batchSize)
                                                    :all(function(tuple)
                            local target, features = tuple:unpack(5, 6)
                            -- log.info('- target: %f', target)
                            -- log.info('- features')
                            -- log_features(('task node, %s'):format(taskName), features, 500)
                            local lg = gd:lossAndGradient(target, features, param)
                            trainBatchLoss = trainBatchLoss + lg[1] / trainBatchSize
                            for i = 2, #lg do
                                trainBatchGradient[i - 1] = trainBatchGradient[i - 1] + lg[i]
                            end
                            -- if fun.iter(lg):all(function(val)
                            --      return val == 0
                            -- end) == true then
                            --     log.info('<task node, %s> lossAnd' ..
                            --              'Gradient has zeroed result',
                            --              taskName)
                            -- end
                            batchSize = batchSize - 1
                            -- log.info('"train" message processed, %d left', batchSize)
                            return batchSize > 0
                        end)
                    end
                end
            end
            if trainAverageLoss == nil then
                trainAverageLoss = trainBatchLoss
            else
                trainAverageLoss = (1 - alpha) * trainAverageLoss +
                                         alpha * trainBatchLoss
            end

            if testAverageLoss == nil then
                testAverageLoss = testBatchLoss
            else
                local tal = (1 - alpha) * testAverageLoss +
                                  alpha * testBatchLoss
                if math.abs(testAverageLoss - tal) < epsilon then
                    log.info('<task node, %s> GD converged on iteration %d',
                             taskName, nIter)
                    break
                end
                testAverageLoss = tal
            end

            param = gd:update(nIter, param, trainBatchGradient)
            log.info('<task node, %s> GD OUTPUT on iteration %d:', taskName, nIter)
            log.info('<task node, %s> trainBatchLoss - %f, testBatchLoss - %f',
                     taskName, trainBatchLoss, testBatchLoss)
            log.info('<task node, %s> trainAverageLoss - %f, testAverageLoss - %f',
                     taskName, trainAverageLoss, testAverageLoss)
        end

        log.info('<task node, %s> Finished GD, new parameters', taskName)
        log_features(('task node, %s'):format(taskName), param)

        self.dataSetSpace:truncate()
        return param
    end,
    saveDataSetToLocalSpace = function(self, taskName, testVerticesFraction)
        log.info('<task node, %s> Saving data to space %s',
                 taskName, self.dataSetSpace.name)

        local cnt = 0
        local recordCounts = defaultdict(function() return {
            [SUFFIX_TEST] = 0,
            [SUFFIX_TRAIN]= 0,
        } end)

        for _, msg in self:pairs_messages() do
            local rtarget = math_round(msg.target)
            local suffix = math.random() < testVerticesFraction and SUFFIX_TEST or SUFFIX_TRAIN

            self.dataSetSpace:auto_increment{taskName, suffix, rtarget,
                                             msg.target, msg.features}
            recordCounts[rtarget][suffix] = recordCounts[rtarget][suffix] + 1
            cnt = cnt + 1
        end

        local last = self.dataSetSpace.index.name:select(taskName, {
            limit = 1
        })[1]

        while true do
            if last == nil or last[2] ~= taskName then
                break
            end
            local suffix, target = last:unpack(3, 4)
            local cnt = self.dataSetSpace.index.name:count{taskName, suffix, target}
            log.info('<task node, %s> Written data set: %s_%d_%s -> %d',
                     taskName, taskName, target, suffix, cnt)
            last = self.dataSetSpace.index.name:select(
                {taskName, suffix, target}, {limit = 1, iterator = 'GT'}
            )[1]
        end

        return cnt, recordCounts
    end,
    removeDataSetLocalSpace = function(self, taskName, suffix, target)
        log.info('<task node, %s> Removing all records for %s_%d_%s',
                 taskName, taskName, suffix, target)
        local to_remove = self.dataSetSpace.index.name
                                           :pairs{taskName, suffix, target}
                                           :map(function(tuple)
            return tuple[1]
        end):totable()
        fun.iter(to_remove):each(function(id)
            self.dataSetSpace:delete(id)
        end)
        log.info('<task node, %s> Removed %d records', #to_remove)
    end,
    applyModelToTestDataSets = function(self, taskName, parameters)
        local wc = self:get_worker_context()

        local categories  = {}
        local predictions = {}
        local last = self.dataSetSpace.index.name:select(taskName, {
            limit = 1
        })[1]
        while true do
            if last == nil or last[2] ~= taskName then
                break
            end
            local suffix, target = last:unpack(3, 4)
            if suffix == SUFFIX_TRAIN then
                self.dataSetSpace.index.name:pairs{taskName, suffix, target}:map(function(tuple)
                    local target, features = tuple:unpack(5, 6)
                    table.insert(categories, target == -1 and 0 or 1)
                    table.insert(predictions,
                            self:predictCalibrated(parameters, features,
                                                   wc.calibrationBucketPercents)
                    )
                end)
            end
            last = self.dataSetSpace.index.name:select(
                {taskName, suffix, target}, {limit = 1, iterator = 'GT'}
            )[1]
        end
        return categories, predictions
    end,
    saveReport = function(self, taskName, report, paths)
    end,
    computeTrainTestRecordCounts = function(self, recordCounts)
        local trainCount, testCount = 0, 0
        for k, v in pairs(recordCounts) do
            log.info("k - %s, v - %s", json.encode(k), json.encode(v))
            if v[SUFFIX_TEST] > 0 then
                testCount = testCount + 1
            end
            if v[SUFFIX_TRAIN] > 0 then
                trainCount = trainCount + 1
            end
        end
        return trainCount, testCount
    end,
}

local node_task_mt = {
    __index = {}
}

for k, v in pairs(vertex_methods) do
    node_task_mt.__index[k] = v
end

for k, v in pairs(node_common_methods) do
    node_task_mt.__index[k] = v
end

for k, v in pairs(node_task_methods) do
    node_task_mt.__index[k] = v
end

return {
    mt = node_task_mt
}
