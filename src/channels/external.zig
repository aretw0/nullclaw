//! External channel host.
//!
//! This adapter keeps nullclaw clean by hosting community channel extensions
//! as separate processes that speak line-delimited JSON-RPC over stdio.

const std = @import("std");
const root = @import("root.zig");
const config_types = @import("../config_types.zig");
const bus_mod = @import("../bus.zig");
const json_util = @import("../json_util.zig");

const log = std.log.scoped(.external_channel);

const MAX_JSONRPC_LINE_BYTES: usize = 256 * 1024;
const PLUGIN_PROTOCOL_VERSION: i64 = 1;
const HEALTH_CHECK_TIMEOUT_MS: u32 = 2_000;
const HEALTH_CHECK_CACHE_TTL_NS: i64 = 5 * std.time.ns_per_s;

pub const ExternalChannel = struct {
    allocator: std.mem.Allocator,
    config: config_types.ExternalChannelConfig,
    event_bus: ?*bus_mod.Bus = null,
    child: ?std.process.Child = null,
    reader_thread: ?std.Thread = null,
    lifecycle_mutex: std.Thread.Mutex = .{},
    request_state: RequestState = .{},
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    reader_alive: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    health_rpc_mode: std.atomic.Value(i8) = std.atomic.Value(i8).init(HEALTH_RPC_UNKNOWN),
    last_health_probe_ns: i64 = 0,
    last_health_result: bool = false,

    const Self = @This();
    const HEALTH_RPC_UNKNOWN: i8 = 0;
    const HEALTH_RPC_SUPPORTED: i8 = 1;
    const HEALTH_RPC_UNSUPPORTED: i8 = -1;

    const RequestState = struct {
        mutex: std.Thread.Mutex = .{},
        cond: std.Thread.Condition = .{},
        next_id: u32 = 1,
        pending_id: ?u32 = null,
        response_line: ?[]u8 = null,
        closed: bool = false,
    };

    pub const Error = error{
        InvalidConfiguration,
        ExternalChannelNotRunning,
        ExternalChannelClosed,
        InvalidPluginManifest,
        PluginManifestMismatch,
        PluginRequestFailed,
        InvalidPluginResponse,
        MissingPluginStdout,
        MissingPluginStdin,
        PluginRequestTimeout,
        HealthMethodNotSupported,
        UnsupportedPluginProtocolVersion,
        ResponseTooLarge,
    };

    pub fn initFromConfig(allocator: std.mem.Allocator, cfg: config_types.ExternalChannelConfig) Self {
        return .{
            .allocator = allocator,
            .config = cfg,
        };
    }

    pub fn setBus(self: *Self, event_bus: *bus_mod.Bus) void {
        self.event_bus = event_bus;
    }

    pub fn channel(self: *Self) root.Channel {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    fn start(self: *Self) !void {
        self.lifecycle_mutex.lock();
        defer self.lifecycle_mutex.unlock();
        try self.startLocked();
    }

    fn stop(self: *Self) void {
        self.lifecycle_mutex.lock();
        defer self.lifecycle_mutex.unlock();
        self.stopLocked(true);
    }

    fn send(self: *Self, target: []const u8, message: []const u8, media: []const []const u8) !void {
        self.lifecycle_mutex.lock();
        defer self.lifecycle_mutex.unlock();
        try self.sendLocked(target, message, media, .final);
    }

    fn sendEvent(self: *Self, target: []const u8, message: []const u8, media: []const []const u8, stage: root.Channel.OutboundStage) !void {
        self.lifecycle_mutex.lock();
        defer self.lifecycle_mutex.unlock();
        try self.sendLocked(target, message, media, stage);
    }

    fn healthCheck(self: *Self) bool {
        self.lifecycle_mutex.lock();
        defer self.lifecycle_mutex.unlock();
        return self.healthCheckLocked();
    }

    fn healthCheckLocked(self: *Self) bool {
        if (!self.running.load(.acquire) or !self.reader_alive.load(.acquire) or self.child == null) {
            self.last_health_probe_ns = 0;
            self.last_health_result = false;
            return false;
        }

        if (self.health_rpc_mode.load(.acquire) == HEALTH_RPC_UNSUPPORTED) {
            return true;
        }

        const now_ns: i64 = @intCast(std.time.nanoTimestamp());
        if (self.last_health_probe_ns != 0 and (now_ns - self.last_health_probe_ns) < HEALTH_CHECK_CACHE_TTL_NS) {
            return self.last_health_result;
        }

        const response = self.sendRequestLockedWithTimeout("health", "{}", @min(self.config.timeout_ms, HEALTH_CHECK_TIMEOUT_MS)) catch |err| switch (err) {
            Error.HealthMethodNotSupported => {
                self.health_rpc_mode.store(HEALTH_RPC_UNSUPPORTED, .release);
                self.last_health_probe_ns = now_ns;
                self.last_health_result = true;
                return true;
            },
            else => {
                self.last_health_probe_ns = now_ns;
                self.last_health_result = false;
                return false;
            },
        };
        defer self.allocator.free(response);

        const healthy = parseHealthResponse(self.allocator, response) catch |err| switch (err) {
            Error.HealthMethodNotSupported => {
                self.health_rpc_mode.store(HEALTH_RPC_UNSUPPORTED, .release);
                self.last_health_probe_ns = now_ns;
                self.last_health_result = true;
                return true;
            },
            else => {
                self.last_health_probe_ns = now_ns;
                self.last_health_result = false;
                return false;
            },
        };
        self.health_rpc_mode.store(HEALTH_RPC_SUPPORTED, .release);
        self.last_health_probe_ns = now_ns;
        self.last_health_result = healthy;
        return healthy;
    }

    fn startLocked(self: *Self) !void {
        if (self.running.load(.acquire)) return;
        if (!config_types.ExternalChannelConfig.isValidChannelName(self.config.channel_name)) {
            return Error.InvalidConfiguration;
        }
        if (!config_types.ExternalChannelConfig.hasCommand(self.config.command)) {
            return Error.InvalidConfiguration;
        }
        if (!config_types.ExternalChannelConfig.isValidTimeoutMs(self.config.timeout_ms)) {
            return Error.InvalidConfiguration;
        }

        var argv: std.ArrayListUnmanaged([]const u8) = .empty;
        defer argv.deinit(self.allocator);
        try argv.append(self.allocator, self.config.command);
        for (self.config.args) |arg| {
            try argv.append(self.allocator, arg);
        }

        var child = std.process.Child.init(argv.items, self.allocator);
        child.stdin_behavior = .Pipe;
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Inherit;

        if (self.config.env.len > 0) {
            var env = std.process.EnvMap.init(self.allocator);
            defer env.deinit();

            const inherit_vars = [_][]const u8{
                "PATH",        "HOME",    "TERM",         "LANG",   "LC_ALL",
                "LC_CTYPE",    "USER",    "SHELL",        "TMPDIR", "NODE_PATH",
                "USERPROFILE", "APPDATA", "LOCALAPPDATA", "TEMP",   "TMP",
                "SYSTEMROOT",  "COMSPEC", "PROGRAMFILES", "WINDIR",
            };
            for (&inherit_vars) |key| {
                if (std.process.getEnvVarOwned(self.allocator, key)) |value| {
                    defer self.allocator.free(value);
                    try env.put(key, value);
                } else |_| {}
            }
            for (self.config.env) |entry| {
                try env.put(entry.key, entry.value);
            }
            child.env_map = &env;
            try child.spawn();
        } else {
            try child.spawn();
        }

        const stdout_file = child.stdout orelse {
            _ = child.kill() catch {};
            _ = child.wait() catch {};
            return Error.MissingPluginStdout;
        };

        self.child = child;
        self.resetRequestState();
        self.health_rpc_mode.store(HEALTH_RPC_UNKNOWN, .release);
        self.last_health_probe_ns = 0;
        self.last_health_result = false;
        self.reader_thread = std.Thread.spawn(.{}, readerThreadMain, .{ self, stdout_file }) catch |err| {
            self.stopLocked(false);
            return err;
        };

        errdefer self.stopLocked(false);

        const manifest_response = try self.sendRequestLocked("get_manifest", "{}");
        defer self.allocator.free(manifest_response);
        const manifest = try self.parseManifestResponse(manifest_response);
        if (manifest.health_supported) |supported| {
            self.health_rpc_mode.store(if (supported) HEALTH_RPC_SUPPORTED else HEALTH_RPC_UNSUPPORTED, .release);
        }

        const start_params = try self.buildStartParams();
        defer self.allocator.free(start_params);
        const start_response = try self.sendRequestLocked("start", start_params);
        defer self.allocator.free(start_response);
        try validateRpcSuccess(self.allocator, start_response);

        self.running.store(true, .release);
    }

    fn stopLocked(self: *Self, notify_plugin: bool) void {
        if (notify_plugin and self.child != null and !self.request_state.closed) {
            const stop_response = self.sendRequestLocked("stop", "{}") catch null;
            if (stop_response) |response| self.allocator.free(response);
        }

        self.running.store(false, .release);
        self.signalClosed();

        if (self.child) |*child| {
            if (child.stdin) |stdin_file| {
                stdin_file.close();
                child.stdin = null;
            }
            _ = child.kill() catch {};
        }

        if (self.reader_thread) |thread| {
            thread.join();
            self.reader_thread = null;
        }

        if (self.child) |*child| {
            _ = child.wait() catch {};
        }
        self.child = null;
        self.reader_alive.store(false, .release);
        self.health_rpc_mode.store(HEALTH_RPC_UNKNOWN, .release);
        self.last_health_probe_ns = 0;
        self.last_health_result = false;
        self.clearPendingResponse();
    }

    fn sendLocked(self: *Self, target: []const u8, message: []const u8, media: []const []const u8, stage: root.Channel.OutboundStage) !void {
        if (!self.running.load(.acquire) or self.child == null) {
            return Error.ExternalChannelNotRunning;
        }

        const params = try buildSendParams(self.allocator, self.config, target, message, media, stage);
        defer self.allocator.free(params);

        const response = try self.sendRequestLocked("send", params);
        defer self.allocator.free(response);
        try validateRpcSuccess(self.allocator, response);
    }

    fn resetRequestState(self: *Self) void {
        self.request_state.mutex.lock();
        defer self.request_state.mutex.unlock();
        self.request_state.next_id = 1;
        self.request_state.pending_id = null;
        if (self.request_state.response_line) |line| {
            self.allocator.free(line);
            self.request_state.response_line = null;
        }
        self.request_state.closed = false;
    }

    fn clearPendingResponse(self: *Self) void {
        self.request_state.mutex.lock();
        defer self.request_state.mutex.unlock();
        if (self.request_state.response_line) |line| {
            self.allocator.free(line);
            self.request_state.response_line = null;
        }
        self.request_state.pending_id = null;
    }

    fn signalClosed(self: *Self) void {
        self.request_state.mutex.lock();
        defer self.request_state.mutex.unlock();
        self.request_state.closed = true;
        self.request_state.pending_id = null;
        self.request_state.cond.broadcast();
    }

    fn sendRequestLocked(self: *Self, method: []const u8, params_json: []const u8) ![]u8 {
        return self.sendRequestLockedWithTimeout(method, params_json, self.config.timeout_ms);
    }

    fn sendRequestLockedWithTimeout(self: *Self, method: []const u8, params_json: []const u8, timeout_ms: u32) ![]u8 {
        const child = if (self.child) |*child| child else return Error.ExternalChannelNotRunning;
        const stdin_file = child.stdin orelse return Error.MissingPluginStdin;

        const request_id = blk: {
            self.request_state.mutex.lock();
            defer self.request_state.mutex.unlock();
            if (self.request_state.closed) return Error.ExternalChannelClosed;
            if (self.request_state.response_line) |line| {
                self.allocator.free(line);
                self.request_state.response_line = null;
            }
            const id = self.request_state.next_id;
            self.request_state.next_id += 1;
            self.request_state.pending_id = id;
            break :blk id;
        };

        const request_line = try buildJsonRpcRequest(self.allocator, request_id, method, params_json);
        defer self.allocator.free(request_line);

        stdin_file.writeAll(request_line) catch |err| {
            self.signalClosed();
            return err;
        };
        stdin_file.writeAll("\n") catch |err| {
            self.signalClosed();
            return err;
        };

        const deadline_ns: i128 = std.time.nanoTimestamp() +
            (@as(i128, @intCast(timeout_ms)) * std.time.ns_per_ms);
        self.request_state.mutex.lock();
        defer self.request_state.mutex.unlock();
        while (!self.request_state.closed and self.request_state.pending_id != null and self.request_state.response_line == null) {
            const remaining_ns = remainingRequestTimeoutNs(deadline_ns);
            if (remaining_ns == 0) {
                self.request_state.pending_id = null;
                return Error.PluginRequestTimeout;
            }
            self.request_state.cond.timedWait(&self.request_state.mutex, remaining_ns) catch |err| switch (err) {
                error.Timeout => {
                    if (!self.request_state.closed and self.request_state.pending_id != null and self.request_state.response_line == null) {
                        self.request_state.pending_id = null;
                        return Error.PluginRequestTimeout;
                    }
                },
            };
        }
        if (self.request_state.response_line) |line| {
            self.request_state.response_line = null;
            return line;
        }
        return Error.ExternalChannelClosed;
    }

    const Manifest = struct {
        health_supported: ?bool = null,
    };

    fn parseManifestResponse(self: *Self, response_line: []const u8) !Manifest {
        var parsed = std.json.parseFromSlice(std.json.Value, self.allocator, response_line, .{}) catch
            return Error.InvalidPluginManifest;
        defer parsed.deinit();

        if (parsed.value != .object) return Error.InvalidPluginManifest;
        const obj = parsed.value.object;
        if (obj.get("error")) |_| {
            return Error.PluginRequestFailed;
        }
        const result = obj.get("result") orelse return Error.InvalidPluginManifest;
        if (result != .object) return Error.InvalidPluginManifest;
        const channel_name_value = result.object.get("channel_name") orelse return Error.InvalidPluginManifest;
        if (channel_name_value != .string) return Error.InvalidPluginManifest;
        if (!std.mem.eql(u8, channel_name_value.string, self.config.channel_name)) {
            return Error.PluginManifestMismatch;
        }

        if (result.object.get("protocol_version")) |protocol_version_value| {
            if (protocol_version_value != .integer) return Error.InvalidPluginManifest;
            if (protocol_version_value.integer != PLUGIN_PROTOCOL_VERSION) {
                return Error.UnsupportedPluginProtocolVersion;
            }
        }

        var manifest = Manifest{};
        if (result.object.get("capabilities")) |capabilities_value| {
            if (capabilities_value != .object) return Error.InvalidPluginManifest;
            if (capabilities_value.object.get("health")) |health_value| {
                if (health_value != .bool) return Error.InvalidPluginManifest;
                manifest.health_supported = health_value.bool;
            }
        }
        return manifest;
    }

    fn buildStartParams(self: *Self) ![]u8 {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        errdefer buf.deinit(self.allocator);

        try buf.appendSlice(self.allocator, "{\"account_id\":");
        try json_util.appendJsonString(&buf, self.allocator, self.config.account_id);
        try buf.appendSlice(self.allocator, ",\"channel_name\":");
        try json_util.appendJsonString(&buf, self.allocator, self.config.channel_name);
        try buf.appendSlice(self.allocator, ",\"config\":");
        try buf.appendSlice(self.allocator, self.config.config_json);
        try buf.append(self.allocator, '}');

        return buf.toOwnedSlice(self.allocator);
    }

    fn handleIncomingLine(self: *Self, line: []u8) !void {
        var parsed = std.json.parseFromSlice(std.json.Value, self.allocator, line, .{}) catch {
            self.allocator.free(line);
            return;
        };
        defer parsed.deinit();

        if (parsed.value != .object) {
            self.allocator.free(line);
            return;
        }
        const obj = parsed.value.object;

        if (obj.get("id")) |id_value| {
            if (id_value == .integer) {
                const response_id: u32 = std.math.cast(u32, id_value.integer) orelse {
                    self.allocator.free(line);
                    return;
                };
                self.requestStateHandleResponse(response_id, line);
                return;
            }
        }

        const method_value = obj.get("method") orelse {
            self.allocator.free(line);
            return;
        };
        if (method_value != .string) {
            self.allocator.free(line);
            return;
        }
        if (!std.mem.eql(u8, method_value.string, "inbound_message")) {
            self.allocator.free(line);
            return;
        }

        const params_value = obj.get("params") orelse {
            self.allocator.free(line);
            return;
        };
        if (params_value != .object) {
            self.allocator.free(line);
            return;
        }

        try self.handleInboundMessage(params_value.object);
        self.allocator.free(line);
    }

    fn requestStateHandleResponse(self: *Self, response_id: u32, line: []u8) void {
        self.request_state.mutex.lock();
        defer self.request_state.mutex.unlock();

        if (self.request_state.pending_id == null or self.request_state.pending_id.? != response_id) {
            self.allocator.free(line);
            return;
        }
        if (self.request_state.response_line) |old_line| {
            self.allocator.free(old_line);
        }
        self.request_state.response_line = line;
        self.request_state.pending_id = null;
        self.request_state.cond.signal();
    }

    fn handleInboundMessage(self: *Self, params: std.json.ObjectMap) !void {
        const event_bus = self.event_bus orelse {
            log.warn("external channel '{s}' dropped inbound message because no bus is attached", .{self.config.channel_name});
            return;
        };

        const sender_id = requiredString(params, "sender_id") orelse return Error.InvalidPluginResponse;
        const chat_id = requiredString(params, "chat_id") orelse return Error.InvalidPluginResponse;
        const content = requiredString(params, "content") orelse return Error.InvalidPluginResponse;

        const media = try parseMediaSlice(self.allocator, params);
        defer if (media.len > 0) self.allocator.free(media);

        var derived_session_key: ?[]u8 = null;
        defer if (derived_session_key) |owned| self.allocator.free(owned);
        const session_key = if (stringValue(params, "session_key")) |provided|
            provided
        else blk: {
            derived_session_key = try std.fmt.allocPrint(self.allocator, "{s}:{s}", .{ self.config.channel_name, chat_id });
            break :blk derived_session_key.?;
        };

        const metadata_json = try self.buildInboundMetadataJson(if (params.get("metadata")) |metadata| metadata else null);
        defer self.allocator.free(metadata_json);

        const msg = try bus_mod.makeInboundFull(
            self.allocator,
            self.config.channel_name,
            sender_id,
            chat_id,
            content,
            session_key,
            media,
            metadata_json,
        );

        event_bus.publishInbound(msg) catch |err| {
            var owned_msg = msg;
            owned_msg.deinit(self.allocator);
            if (err != error.Closed) return err;
        };
    }

    fn buildInboundMetadataJson(self: *Self, metadata_value: ?std.json.Value) ![]u8 {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        errdefer buf.deinit(self.allocator);

        try buf.appendSlice(self.allocator, "{\"account_id\":");
        try json_util.appendJsonString(&buf, self.allocator, self.config.account_id);

        if (metadata_value) |metadata| {
            if (metadata == .object) {
                var it = metadata.object.iterator();
                while (it.next()) |entry| {
                    if (std.mem.eql(u8, entry.key_ptr.*, "account_id")) continue;
                    const value_json = try std.json.Stringify.valueAlloc(self.allocator, entry.value_ptr.*, .{});
                    defer self.allocator.free(value_json);

                    try buf.append(self.allocator, ',');
                    try json_util.appendJsonString(&buf, self.allocator, entry.key_ptr.*);
                    try buf.append(self.allocator, ':');
                    try buf.appendSlice(self.allocator, value_json);
                }
            }
        }

        try buf.append(self.allocator, '}');
        return try buf.toOwnedSlice(self.allocator);
    }

    fn channelName(self: *Self) []const u8 {
        return self.config.channel_name;
    }

    fn readerThreadMain(self: *Self, stdout_file: std.fs.File) void {
        var stdout = stdout_file;
        self.reader_alive.store(true, .release);
        defer {
            self.reader_alive.store(false, .release);
            self.running.store(false, .release);
            self.signalClosed();
        }

        while (true) {
            const line = readJsonRpcLine(self.allocator, &stdout) catch |err| switch (err) {
                error.EndOfStream => return,
                else => {
                    log.warn("external channel '{s}' reader failed: {}", .{ self.config.channel_name, err });
                    return;
                },
            };
            if (line.len == 0) {
                self.allocator.free(line);
                continue;
            }
            self.handleIncomingLine(line) catch |err| {
                log.warn("external channel '{s}' failed to handle plugin message: {}", .{ self.config.channel_name, err });
            };
        }
    }

    fn vtableStart(ptr: *anyopaque) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.start();
    }

    fn vtableStop(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.stop();
    }

    fn vtableSend(ptr: *anyopaque, target: []const u8, message: []const u8, media: []const []const u8) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.send(target, message, media);
    }

    fn vtableSendEvent(ptr: *anyopaque, target: []const u8, message: []const u8, media: []const []const u8, stage: root.Channel.OutboundStage) anyerror!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.sendEvent(target, message, media, stage);
    }

    fn vtableName(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.channelName();
    }

    fn vtableHealthCheck(ptr: *anyopaque) bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.healthCheck();
    }

    pub const vtable = root.Channel.VTable{
        .start = &vtableStart,
        .stop = &vtableStop,
        .send = &vtableSend,
        .name = &vtableName,
        .healthCheck = &vtableHealthCheck,
        .sendEvent = &vtableSendEvent,
    };
};

fn requiredString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const value = obj.get(key) orelse return null;
    return if (value == .string) value.string else null;
}

fn stringValue(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const value = obj.get(key) orelse return null;
    return if (value == .string and value.string.len > 0) value.string else null;
}

fn readJsonRpcLine(allocator: std.mem.Allocator, file: *std.fs.File) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);

    var buf: [1]u8 = undefined;
    while (true) {
        const n = try file.read(&buf);
        if (n == 0) {
            if (out.items.len == 0) return error.EndOfStream;
            break;
        }
        if (buf[0] == '\n') break;
        if (buf[0] == '\r') continue;
        if (out.items.len >= MAX_JSONRPC_LINE_BYTES) return ExternalChannel.Error.ResponseTooLarge;
        try out.append(allocator, buf[0]);
    }

    return out.toOwnedSlice(allocator);
}

fn buildJsonRpcRequest(allocator: std.mem.Allocator, request_id: u32, method: []const u8, params_json: []const u8) ![]u8 {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(allocator);

    const writer = buf.writer(allocator);
    try writer.writeAll("{\"jsonrpc\":\"2.0\",\"id\":");
    try writer.print("{d}", .{request_id});
    try writer.writeAll(",\"method\":");
    try json_util.appendJsonString(&buf, allocator, method);
    try writer.writeAll(",\"params\":");
    try writer.writeAll(params_json);
    try writer.writeAll("}");

    return buf.toOwnedSlice(allocator);
}

fn buildSendParams(
    allocator: std.mem.Allocator,
    config: config_types.ExternalChannelConfig,
    target: []const u8,
    message: []const u8,
    media: []const []const u8,
    stage: root.Channel.OutboundStage,
) ![]u8 {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"account_id\":");
    try json_util.appendJsonString(&buf, allocator, config.account_id);
    try buf.appendSlice(allocator, ",\"channel_name\":");
    try json_util.appendJsonString(&buf, allocator, config.channel_name);
    try buf.appendSlice(allocator, ",\"target\":");
    try json_util.appendJsonString(&buf, allocator, target);
    try buf.appendSlice(allocator, ",\"message\":");
    try json_util.appendJsonString(&buf, allocator, message);
    try buf.appendSlice(allocator, ",\"stage\":");
    try json_util.appendJsonString(&buf, allocator, stageToSlice(stage));
    try buf.appendSlice(allocator, ",\"media\":[");
    for (media, 0..) |item, index| {
        if (index > 0) try buf.append(allocator, ',');
        try json_util.appendJsonString(&buf, allocator, item);
    }
    try buf.appendSlice(allocator, "]}");

    return buf.toOwnedSlice(allocator);
}

fn stageToSlice(stage: root.Channel.OutboundStage) []const u8 {
    return switch (stage) {
        .chunk => "chunk",
        .final => "final",
    };
}

fn remainingRequestTimeoutNs(deadline_ns: i128) u64 {
    const remaining_ns = deadline_ns - std.time.nanoTimestamp();
    if (remaining_ns <= 0) return 0;
    return @intCast(remaining_ns);
}

fn isMethodNotFoundError(err_value: std.json.Value) bool {
    if (err_value != .object) return false;
    if (err_value.object.get("code")) |code_value| {
        if (code_value == .integer and code_value.integer == -32601) {
            return true;
        }
    }
    if (err_value.object.get("message")) |message_value| {
        if (message_value == .string) {
            const message = message_value.string;
            return std.ascii.indexOfIgnoreCase(message, "method not found") != null or
                std.ascii.indexOfIgnoreCase(message, "not implemented") != null or
                std.ascii.indexOfIgnoreCase(message, "unknown method") != null;
        }
    }
    return false;
}

fn parseHealthResponse(allocator: std.mem.Allocator, response_line: []const u8) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, response_line, .{}) catch
        return ExternalChannel.Error.InvalidPluginResponse;
    defer parsed.deinit();

    if (parsed.value != .object) return ExternalChannel.Error.InvalidPluginResponse;
    const obj = parsed.value.object;
    if (obj.get("error")) |err_value| {
        if (isMethodNotFoundError(err_value)) return ExternalChannel.Error.HealthMethodNotSupported;
        return ExternalChannel.Error.PluginRequestFailed;
    }

    const result = obj.get("result") orelse return ExternalChannel.Error.InvalidPluginResponse;
    if (result != .object) return ExternalChannel.Error.InvalidPluginResponse;

    const healthy_val = result.object.get("healthy");
    const ok_val = result.object.get("ok");
    const connected_val = result.object.get("connected");
    const logged_in_val = result.object.get("logged_in");

    if (healthy_val) |v| {
        if (v == .bool) return v.bool;
        return ExternalChannel.Error.InvalidPluginResponse;
    }

    var healthy = true;
    var seen_signal = false;

    if (ok_val) |v| {
        if (v != .bool) return ExternalChannel.Error.InvalidPluginResponse;
        healthy = healthy and v.bool;
        seen_signal = true;
    }
    if (connected_val) |v| {
        if (v != .bool) return ExternalChannel.Error.InvalidPluginResponse;
        healthy = healthy and v.bool;
        seen_signal = true;
    }
    if (logged_in_val) |v| {
        if (v != .bool) return ExternalChannel.Error.InvalidPluginResponse;
        healthy = healthy and v.bool;
        seen_signal = true;
    }

    return if (seen_signal) healthy else true;
}

fn validateRpcSuccess(allocator: std.mem.Allocator, response_line: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, response_line, .{}) catch
        return ExternalChannel.Error.InvalidPluginResponse;
    defer parsed.deinit();

    if (parsed.value != .object) return ExternalChannel.Error.InvalidPluginResponse;
    const obj = parsed.value.object;
    if (obj.get("error")) |err_value| {
        if (err_value == .object) {
            if (err_value.object.get("message")) |message_value| {
                if (message_value == .string) {
                    log.warn("external channel plugin error: {s}", .{message_value.string});
                }
            }
        }
        return ExternalChannel.Error.PluginRequestFailed;
    }
}

fn parseMediaSlice(allocator: std.mem.Allocator, params: std.json.ObjectMap) ![]const []const u8 {
    const media_value = params.get("media") orelse return &.{};
    if (media_value != .array) return &.{};

    var count: usize = 0;
    for (media_value.array.items) |item| {
        if (item == .string) count += 1;
    }
    if (count == 0) return &.{};

    const media = try allocator.alloc([]const u8, count);
    var idx: usize = 0;
    for (media_value.array.items) |item| {
        if (item != .string) continue;
        media[idx] = item.string;
        idx += 1;
    }
    return media;
}

test "buildSendParams includes stage and media" {
    const allocator = std.testing.allocator;
    const params = try buildSendParams(allocator, .{
        .account_id = "main",
        .channel_name = "whatsapp_web",
        .command = "plugin",
    }, "chat-1", "hello", &.{ "a.png", "b.jpg" }, .chunk);
    defer allocator.free(params);

    try std.testing.expect(std.mem.indexOf(u8, params, "\"stage\":\"chunk\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, params, "\"media\":[\"a.png\",\"b.jpg\"]") != null);
}

test "parseHealthResponse honors healthy and bridge connectivity flags" {
    try std.testing.expect(try parseHealthResponse(
        std.testing.allocator,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"healthy\":true}}",
    ));
    try std.testing.expect(!(try parseHealthResponse(
        std.testing.allocator,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true,\"connected\":true,\"logged_in\":false}}",
    )));
}

test "parseHealthResponse detects missing health method" {
    try std.testing.expectError(
        ExternalChannel.Error.HealthMethodNotSupported,
        parseHealthResponse(
            std.testing.allocator,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32601,\"message\":\"Method not found\"}}",
        ),
    );
}

test "parseManifestResponse accepts protocol version and health capability" {
    const allocator = std.testing.allocator;
    var channel = ExternalChannel.initFromConfig(allocator, .{
        .account_id = "main",
        .channel_name = "whatsapp_web",
        .command = "plugin",
    });

    const manifest = try channel.parseManifestResponse(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"channel_name\":\"whatsapp_web\",\"protocol_version\":1,\"capabilities\":{\"health\":true}}}",
    );
    try std.testing.expectEqual(@as(?bool, true), manifest.health_supported);
}

test "parseManifestResponse rejects unsupported protocol version" {
    const allocator = std.testing.allocator;
    var channel = ExternalChannel.initFromConfig(allocator, .{
        .account_id = "main",
        .channel_name = "whatsapp_web",
        .command = "plugin",
    });

    try std.testing.expectError(
        ExternalChannel.Error.UnsupportedPluginProtocolVersion,
        channel.parseManifestResponse(
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"channel_name\":\"whatsapp_web\",\"protocol_version\":2}}",
        ),
    );
}

test "buildInboundMetadataJson injects account_id and preserves metadata fields" {
    const allocator = std.testing.allocator;
    var channel = ExternalChannel.initFromConfig(allocator, .{
        .account_id = "backup",
        .channel_name = "plugin-chat",
        .command = "plugin",
    });

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, "{\"peer_kind\":\"group\",\"peer_id\":\"room-1\"}", .{});
    defer parsed.deinit();

    const metadata_json = try channel.buildInboundMetadataJson(parsed.value);
    defer allocator.free(metadata_json);

    try std.testing.expect(std.mem.indexOf(u8, metadata_json, "\"account_id\":\"backup\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, metadata_json, "\"peer_kind\":\"group\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, metadata_json, "\"peer_id\":\"room-1\"") != null);
}

test "handleInboundMessage publishes to bus with injected account id" {
    const allocator = std.testing.allocator;

    var event_bus = bus_mod.Bus.init();
    defer event_bus.close();

    var channel = ExternalChannel.initFromConfig(allocator, .{
        .account_id = "main",
        .channel_name = "whatsapp_web",
        .command = "plugin",
    });
    channel.setBus(&event_bus);

    const line = try allocator.dupe(
        u8,
        "{\"jsonrpc\":\"2.0\",\"method\":\"inbound_message\",\"params\":{\"sender_id\":\"5511\",\"chat_id\":\"room-1\",\"content\":\"hello\",\"metadata\":{\"peer_kind\":\"group\",\"peer_id\":\"room-1\"}}}",
    );
    try channel.handleIncomingLine(line);

    const msg = event_bus.consumeInbound() orelse return error.TestUnexpectedResult;
    defer {
        var owned = msg;
        owned.deinit(allocator);
    }

    try std.testing.expectEqualStrings("whatsapp_web", msg.channel);
    try std.testing.expectEqualStrings("5511", msg.sender_id);
    try std.testing.expectEqualStrings("room-1", msg.chat_id);
    try std.testing.expectEqualStrings("hello", msg.content);
    try std.testing.expectEqualStrings("whatsapp_web:room-1", msg.session_key);
    try std.testing.expect(msg.metadata_json != null);
    try std.testing.expect(std.mem.indexOf(u8, msg.metadata_json.?, "\"account_id\":\"main\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg.metadata_json.?, "\"peer_kind\":\"group\"") != null);
}
