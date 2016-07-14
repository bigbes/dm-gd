local fun  = require('fun')

local dup       = fun.duplicate
local duplicate = fun.duplicate

local function scalar_product(x, y)
    return fun.iter(x):zip(y):map(function(l, r) return l * r end):sum()
end

local function ConstantLearningRate_new(c)
    c = c or 0.01
    return setmetatable({
        c = c,
    }, {
        __index = {
            rate = function(self, iteration, parameters)
                return dup(self.c):take(#parameters):totable()
            end,
        }
    })
end

local function HingeLoss_new()
    return setmetatable({}, {
        __index = {
            valueAndGradient = function(self, t, x, parameters)
                local result = nil
                local y = scalar_product(x, parameters)
                if t * y < 1 then
                    result = fun.range(2, #x + 1):zip(x, dup(t))
                                :map(function(idx, x_i, t)
                                    return idx, -t * x_i
                                end):tomap()
                    result[1] = 1 - t * y
                else
                    result = fun.range(1, #parameters + 1):zip(dup(0)):tomap()
                end
                return result
            end,
        }
    })
end

local function L2_new()
    return setmetatable({}, {
        __index = {
            valueAndGradient = function(self, t, x, parameters)
                local result = nil
                local y = scalar_product(x, parameters)
                result = fun.range(2, #x + 1):zip(fun.iter(parameters):drop_n(1))
                            :map(function(idx, p_i)
                                if idx == 2 then
                                    return 2, 0
                                end
                                return idx, p_i * 2
                            end):tomap()
                result[1] = scalar_product(parameters, parameters)
                result[1] = result[1] - (parameters[1] * parameters[1])
                return result
            end,
        }
    })
end

local function RegularizedLoss_new(loss, regularizer, lambda)
    return setmetatable({
        loss        = loss,
        regularizer = regularizer,
        lambda      = lambda
    }, {
        __index = {
            valueAndGradient = function(self, t, x, parameters)
                local lossResult         = self.loss:valueAndGradient(t, x, parameters)
                local regularizerResult  = self.regularizer:valueAndGradient(t, x, parameters)

                local result = nil
                result = fun.iter(lossResult):zip(regularizerResult)
                            :map(function(l, r) return l + self.lambda * r end)
                            :totable()

                return result
            end,
        }
    })
end

local function GradientDescent_new(loss, lr)
    return setmetatable({
        -- Optimized Function
        loss         = RegularizedLoss_new(HingeLoss_new(), L2_new(), 0.0),
        -- Learning Rate
        learningRate = ConstantLearningRate_new(0.00005),
    }, {
        __index = {
            lossAndGradient = function(self, t, x, parameters)
                return self.loss:valueAndGradient(t, x, parameters)
            end,
            update = function(self, iteration, parameters, gradient)
                local rate = self.learningRate:rate(iteration, parameters)
                local updated = fun.iter(parameters):zip(rate, gradient)
                                   :map(function(p, r, g) return p - r * g end)
                                   :totable()
                return updated
            end,
            initialize = function(self, dim)
                local parameters = fun.range(dim):map(
                    function() return 2 * math.random() - 1 end
                ):totable()
                return parameters
            end,
        }
    })
end

return {
    GradientDescent   = GradientDescent_new,
}
