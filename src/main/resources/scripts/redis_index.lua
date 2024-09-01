local function set_diff(a, b)
    local diff, set = {}, {}
    for _, v in ipairs(b) do
        set[v] = true
    end

    for _, v in ipairs(a) do
        if not set[v] then
            table.insert(diff, v)
        end
    end
    return diff
end

local function get_idx_track_key(key_space, data_key)
    return key_space .. "::_idx_::" .. data_key
end

local function get_idx_key(key_space, idx_name, idx_val)
    return key_space .. "::" .. idx_name .. "::" .. idx_val
end

local function put_index(key_space, key, idx_tab)
    -- validation
    if idx_tab == nil then
        return
    end

    if key_space == nil or type(key_space) ~= "string" then
        return redis.error_reply("Error: key_space is required.")
    end

    if key == nil or type(key) ~= "string" then
        return redis.error_reply("Error: key is required.")
    end

    if type(idx_tab) ~= 'table' then
        return redis.error_reply("Error: Argument idx_tab must be a key-value table.")
    end

    -- check exists indexes
    local idx_track_key = get_idx_track_key(key_space, key)
    redis.pcall('SMEMBERS', idx_track_key)
    

end

-- test key builders
print(get_idx_track_key("opx:position", "DEFAULT-ACC1-11BBG12345"))
print(get_idx_key("opx:position", "index1", "ACC1-11BBG12345"))

-- test set_diff
--[[ local set_a = { 1, 2, 3, 4, 5 }
local set_b = { 4, 5, 6, 7, 8 }
local res1 = set_diff(set_a, set_b)
local res2 = set_diff(set_b, set_a)

for _, v in ipairs(res1) do
    print(v)
end
print("---")
for _, v in ipairs(res2) do
    print(v)
end ]]
