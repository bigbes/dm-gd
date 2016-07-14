local fun = require('fun')

local table_new = require('table.new')

local qsort2a = require('math.qsort').qsort2a

-- truth:       is array of integers
-- probability: is array of doubles
local function MeasureAUC(truth, probability)
    local truth_len = #truth
    assert(truth_len == #probability)

    local pos = fun.sum(truth)
    local neg = truth_len - pos

    local prediction, label = qsort2a(truth, probability)

    local rank = table_new(truth_len, 0)
    for i = 1, truth_len do
        if i == truth_len or prediction[i] ~= prediction[i + 1] then
            rank[i] = i + 1
        else
            local j = i + 1
            while true do
                if j > truth_len or prediction[j] == prediction[i] then
                    break
                end
                j = j + 1
            end
            local r = (1 + i + j) / 2.0;
            for k = i, j do
                rank[k] = r
            end
            i = j - 1
        end
    end

    local auc = fun.iter(label):zip(rank):filter(function(l_i, r_i)
        if l_i == 1 then
            return true
        end
    end):reduce(function(acc, l_i, r_i)
        return acc + r_i
    end, 0.0)

    auc = (auc - (pos * (pos + 1) / 2.0)) / (pos * neg)

    return auc
end

return {
    MeasureAUC = MeasureAUC
}
