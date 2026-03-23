const std = @import("std");

pub const ProviderModelRef = struct {
    provider: ?[]const u8,
    model: []const u8,
};

fn splitAtSlash(model_ref: []const u8, slash_idx: usize) ?ProviderModelRef {
    if (slash_idx == 0 or slash_idx + 1 >= model_ref.len) return null;
    return .{
        .provider = model_ref[0..slash_idx],
        .model = model_ref[slash_idx + 1 ..],
    };
}

const preserved_url_provider_suffixes = [_][]const u8{
    "/chat/completions/",
    "/responses/",
};

fn splitKnownEndpointUrlProviderModel(model_ref: []const u8, url_start: usize) ?ProviderModelRef {
    var best_split: ?ProviderModelRef = null;
    var best_provider_len: usize = 0;

    inline for (preserved_url_provider_suffixes) |suffix| {
        var search_from = url_start;
        while (std.mem.indexOfPos(u8, model_ref, search_from, suffix)) |idx| {
            const split = splitAtSlash(model_ref, idx + suffix.len - 1) orelse return null;
            const provider_len = split.provider.?.len;
            if (provider_len > best_provider_len) {
                best_provider_len = provider_len;
                best_split = split;
            }
            search_from = idx + 1;
        }
    }

    return best_split;
}

fn splitVersionedUrlProviderModel(model_ref: []const u8, url_start: usize) ?ProviderModelRef {
    var last_split: ?ProviderModelRef = null;
    var i: usize = url_start;
    while (i + 3 < model_ref.len) : (i += 1) {
        if (model_ref[i] != '/' or model_ref[i + 1] != 'v') continue;
        var j = i + 2;
        var has_digit = false;
        while (j < model_ref.len and std.ascii.isDigit(model_ref[j])) : (j += 1) {
            has_digit = true;
        }
        if (!has_digit) continue;
        if (j >= model_ref.len or model_ref[j] != '/') continue;
        last_split = splitAtSlash(model_ref, j) orelse return null;
    }
    return last_split;
}

fn splitLastUrlPathSegment(model_ref: []const u8, url_start: usize) ?ProviderModelRef {
    var i = model_ref.len;
    while (i > url_start) : (i -= 1) {
        const slash_idx = i - 1;
        if (model_ref[slash_idx] != '/') continue;
        return splitAtSlash(model_ref, slash_idx);
    }
    return null;
}

pub fn splitProviderModel(model_ref: []const u8) ?ProviderModelRef {
    if (model_ref.len == 0) return null;
    if (std.mem.indexOf(u8, model_ref, "://")) |proto_start| {
        const url_start = proto_start + 3;
        if (splitKnownEndpointUrlProviderModel(model_ref, url_start)) |split| return split;
        if (splitVersionedUrlProviderModel(model_ref, url_start)) |split| return split;
        return splitLastUrlPathSegment(model_ref, url_start);
    }

    const slash = std.mem.indexOfScalar(u8, model_ref, '/') orelse return null;
    return splitAtSlash(model_ref, slash);
}

test "splitProviderModel handles regular refs" {
    const split = splitProviderModel("openrouter/anthropic/claude-sonnet-4") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("openrouter", split.provider.?);
    try std.testing.expectEqualStrings("anthropic/claude-sonnet-4", split.model);
}

test "splitProviderModel handles custom url refs" {
    const split = splitProviderModel("custom:https://api.example.com/openai/v2/qianfan/custom-model") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("custom:https://api.example.com/openai/v2", split.provider.?);
    try std.testing.expectEqualStrings("qianfan/custom-model", split.model);
}

test "splitProviderModel uses last versioned segment for nested gateways" {
    const split = splitProviderModel("custom:https://gateway.example.com/proxy/v1/openai/v2/qianfan/custom-model") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("custom:https://gateway.example.com/proxy/v1/openai/v2", split.provider.?);
    try std.testing.expectEqualStrings("qianfan/custom-model", split.model);
}

test "splitProviderModel handles versionless custom url refs" {
    const split = splitProviderModel("custom:https://example.com/gpt-4o") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("custom:https://example.com", split.provider.?);
    try std.testing.expectEqualStrings("gpt-4o", split.model);
}

test "splitProviderModel preserves explicit responses endpoint suffix" {
    const split = splitProviderModel("custom:https://my-api.example.com/api/v2/responses/my-model") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("custom:https://my-api.example.com/api/v2/responses", split.provider.?);
    try std.testing.expectEqualStrings("my-model", split.model);
}

test "splitProviderModel handles versionless anthropic custom refs" {
    const split = splitProviderModel("anthropic-custom:https://my-api.example.com/claude-sonnet-4") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("anthropic-custom:https://my-api.example.com", split.provider.?);
    try std.testing.expectEqualStrings("claude-sonnet-4", split.model);
}

test "splitProviderModel rejects empty model tail" {
    try std.testing.expect(splitProviderModel("custom:https://api.example.com/v1/") == null);
}
