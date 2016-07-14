local function math_round(fnum)
    return (fnum % 1 >= 0.5) and math.ceil(fnum) or math.floor(fnum)
end

local function PercentileCounter_new(window_size)
    if window_size == nil then
        window_size = math.pow(2, 30)
    end
    return setmetatable({
        window_size = window_size,
        values = {},
        n = 0,
    }, {
        __index = {
            addValue = function(self, new)
                -- pop random element from table
                local isInserted = false
                if self.n == self.window_size then
                    table.remove(self.values, math.random(self.n))
                    self.n = self.n - 1
                end
                -- insert element in the right position
                for idx, val in ipairs(self.values) do
                    if val > new then
                        table.insert(self.values, new, idx)
                        isInserted = true
                        break
                    end
                end
                if not isInserted then
                    table.insert(self.values, new)
                end
                self.n = self.n + 1
            end,
            getPercentile = function(self, p)
                local p = math_round(p * self.n / 100)
                return self.values[p]
            end,
            getN = function(self)
                return self.n
            end
        }
    })
end

return {
    PercentileCounter = PercentileCounter_new
}
