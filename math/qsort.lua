local fun = require('fun')
local log = require('log')

local table_new = require('table.new')

local MAX_DEPTH_1 = 1000
local MAX_DEPTH_2 = 300

local function default_comparator(left, right)
    return left < right
end

local function qsort_inplace(input, cmp)
    cmp = cmp or default_comparator
    local input_len = #input
    local pivot, L, R, i = nil, nil, nil, 1
    local _beg = table_new(MAX_DEPTH_1, 0); _beg[1] = 1
    local _end = table_new(MAX_DEPTH_1, 0); _end[1] = input_len
    while (i > 0) do
        L, R = _beg[i], _end[i]
        if (L < R) then
            pivot = input[L]
            if (i == MAX_DEPTH_1) then
                error('max depth is reached')
            end
            while (L < R) do
                while (not cmp(input[R], pivot) and L < R) do
                    R = R - 1
                end
                if (L < R) then
                    input[L] = input[R]
                    L = L + 1
                end
                while (cmp(input[L], pivot) and L < R) do
                    L = L + 1
                end
                if (L < R) then
                    input[R] = input[L]
                    R = R - 1
                end
            end
            input[L] = pivot
            _beg[i + 1] = L + 1
            _end[i + 1] = _end[i]
            _end[i] = L
            i = i + 1
        else
            i = i - 1
        end
    end
    return input
end

local function qsort(input, cmp)
    return qsort_inplace(fun.totable(input), cmp)
end

local function qsorta_inplace(input, adjacent, cmp)
    assert(#input == #adjacent, "bad lengths")
    cmp = cmp or default_comparator
    local input_len = #input
    local pivot, pivota, L, R, i = nil, nil, nil, nil, 1
    local _beg = table_new(MAX_DEPTH_1, 0); _beg[1] = 1
    local _end = table_new(MAX_DEPTH_1, 0); _end[1] = input_len
    while (i > 0) do
        L, R = _beg[i], _end[i]
        if (L < R) then
            pivot  = input[L]
            pivota = adjacent[L]
            if (i == MAX_DEPTH_1) then
                error('max depth is reached')
            end
            while (L < R) do
                while (not cmp(input[R], pivot) and L < R) do
                    R = R - 1
                end
                if (L < R) then
                    adjacent[L] = adjacent[R]
                    input[L]    = input[R]
                    L = L + 1
                end
                while (cmp(input[L], pivot) and L < R) do
                    L = L + 1
                end
                if (L < R) then
                    adjacent[R] = adjacent[L]
                    input[R]    = input[L]
                    R = R - 1
                end
            end
            adjacent[L] = pivota
            input[L]    = pivot
            _beg[i + 1] = L + 1
            _end[i + 1] = _end[i]
            _end[i] = L
            i = i + 1
        else
            i = i - 1
        end
    end
    return input, adjacent
end

local function qsorta(input, adjacent, cmp)
    return qsorta_inplace(fun.totable(input), fun.totable(adjacent), cmp)
end

local function qsort2_inplace(input, cmp)
    cmp = cmp or default_comparator
    local input_len = #input
    local pivot, L, R, i = nil, nil, nil, 1
    local _beg = table_new(MAX_DEPTH_2, 0); _beg[1] = 1
    local _end = table_new(MAX_DEPTH_2, 0); _end[1] = input_len
    while (i > 0) do
        L, R = _beg[i], _end[i]
        if (L < R) then
            pivot = input[L]
            if (i == MAX_DEPTH_2) then
                error('max depth is reached')
            end
            while (L < R) do
                while (not cmp(input[R], pivot) and L < R) do
                    R = R - 1
                end
                if (L < R) then
                    input[L] = input[R]
                    L = L + 1
                end
                while (cmp(input[L], pivot) and L < R) do
                    L = L + 1
                end
                if (L < R) then
                    input[R] = input[L]
                    R = R - 1
                end
            end
            input[L] = pivot
            _beg[i + 1] = L + 1
            _end[i + 1] = _end[i]
            _end[i] = L
            i = i + 1
            if _end[i] - _beg[i] > _end[i - 1] - _beg[i - 1] then
                _beg[i], _beg[i - 1] = _beg[i - 1], _beg[i]
                _end[i], _end[i - 1] = _end[i - 1], _end[i]
            end
        else
            i = i - 1
        end
    end
    return input
end

local function qsort2(input, cmp)
    return qsort2_inplace(fun.totable(input), cmp)
end

local function qsort2a_inplace(input, adjacent, cmp)
    cmp = cmp or default_comparator
    local input_len = #input
    local pivot, pivota, L, R, i = nil, nil, nil, nil, 1
    local _beg = table_new(MAX_DEPTH_2, 0); _beg[1] = 1
    local _end = table_new(MAX_DEPTH_2, 0); _end[1] = input_len
    while (i > 0) do
        L, R = _beg[i], _end[i]
        if (L < R) then
            pivot  = input[L]
            pivota = adjacent[L]
            if (i == MAX_DEPTH_2) then
                error('max depth is reached')
            end
            while (L < R) do
                while (not cmp(input[R], pivot) and L < R) do
                    R = R - 1
                end
                if (L < R) then
                    adjacent[L] = adjacent[R]
                    input[L]    = input[R]
                    L = L + 1
                end
                while (cmp(input[L], pivot) and L < R) do
                    L = L + 1
                end
                if (L < R) then
                    adjacent[R] = adjacent[L]
                    input[R]    = input[L]
                    R = R - 1
                end
            end
            adjacent[L] = pivota
            input[L]    = pivot
            _beg[i + 1] = L + 1
            _end[i + 1] = _end[i]
            _end[i] = L
            i = i + 1
            if _end[i] - _beg[i] > _end[i - 1] - _beg[i - 1] then
                _beg[i], _beg[i - 1] = _beg[i - 1], _beg[i]
                _end[i], _end[i - 1] = _end[i - 1], _end[i]
            end
        else
            i = i - 1
        end
    end
    return input, adjacent
end

local function qsort2a(input, adjacent, cmp)
    return qsort2a_inplace(fun.totable(input), fun.totable(adjacent), cmp)
end

-- fun.each(print, qsort{4, 5, 3, 6, 2, 8, 1, 7})
-- fun.each(print, qsort2{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
-- fun.each(print, ({qsort2a({10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})})[1])

return {
    qsort = qsort,
    qsorta = qsorta,
    qsort2 = qsort2,
    qsort2a = qsort2a,
    qsort_inplace = qsort_inplace,
    qsorta_inplace = qsorta_inplace,
    qsort2_inplace = qsort2_inplace,
    qsort2a_inplace = qsort2a_inplace,
}
