local key = KEYS[1]
-- local num_args = table.getn(ARGV)

local success, error = pcall(function(key, idx_arr)
    for _, v in ipairs(idx_arr) do
        redis.call("LPUSH", key, v)
    end

    return "Keys and values have been set successfully."
end, key, ARGV)

if success then
    return 'ok'
else
    return error
end
