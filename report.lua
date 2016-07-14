local json = require('json')

local NULL = json.NULL

local REPORT_TYPE_EXPRESS = "express"
local NO_DATA  = "NO_DATA"
local NO_VALUE = -1

-- Long date format pattern for startupTime and finishTime serialization
local LONG_DATE_FORMAT = "%Y-%m-%dT%I:%M:%S"

-- Short date format pattern for reportDate serialization
local SHORT_DATE_FORMAT = "%Y-%m-%d"

local EnsembleReportData_mt = nil
local EnsembleReportEnsemble_mt = nil
local EnsembleReport_mt = nil

local function EnsembleReportData_new()
    local self = setmetatable({
        train_size = NO_VALUE,
        test_size  = NO_VALUE,
        n_features = NO_VALUE
    }, EnsembleReportData_mt)
    return self
end

local function EnsembleReportEnsemble_new()
    local self = setmetatable({
        model = NO_DATA
    }, EnsembleReportEnsemble_mt)
    return self
end

local function EnsembleReport_new()
    local self = setmetatable({
        area_under_roc_weighted_transformed = NULL,
        area_under_roc_weighted_raw         = NULL,
        data                                = EnsembleReportData_new(),
        ensemble                            = EnsembleReportEnsemble_new(),
    }, EnsembleReport_mt)
    return self
end

local TaskReport_mt = {
    __index = {
        update_finish_time = function(self)
            self.finish_time = os.date(LONG_DATE_FORMAT)
        end,
        toJson = function(self)
            return json.encode(self)
        end,
        fromJson = function(self, string)
            local obj = json.decode(string)
            -- TaskReport
            self.startup_time    = obj.startup_time
            self.finish_time     = obj.finish_time
            self.ensemble_report = obj.ensemble_report
            self.date            = obj.date
            self.type            = obj.type
            self.app_name        = obj.app_name
            -- EnsembleReport
            self.ensemble_report.area_under_roc_weighted_transformed =
                        obj.ensemble_report.area_under_roc_weighted_transformed
            self.ensemble_report.area_under_roc_weighted_raw         =
                        obj.ensemble_report.area_under_roc_weighted_raw
            -- EnsembleReportData
            self.ensemble_report.data.train_size = obj.ensemble_report.data.train_size
            self.ensemble_report.data.test_size  = obj.ensemble_report.data.test_size
            self.ensemble_report.data.n_features = obj.ensemble_report.data.n_features
            -- EnsembleReportEnsemble
            self.ensemble_report.ensemble.model = obj.ensemble_report.ensemble.model
        end
    }
}

local function TaskReport_new(taskName)
    local self = setmetatable({
        startup_time    = os.date(LONG_DATE_FORMAT),
        finish_time     = NULL,
        ensemble_report = EnsembleReport_new(),
        date            = os.date(SHORT_DATE_FORMAT),
        type            = REPORT_TYPE_EXPRESS,
        app_name        = taskName,
    }, TaskReport_mt)
    return self
end

return {
    new = TaskReport_new
}
