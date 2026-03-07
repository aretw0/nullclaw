const std = @import("std");
const AtomicBool = std.atomic.Value(bool);

threadlocal var thread_interrupt_flag: ?*const AtomicBool = null;

pub fn setThreadInterruptFlag(flag: ?*const AtomicBool) void {
    thread_interrupt_flag = flag;
}

/// Result of a child process execution.
pub const RunResult = struct {
    stdout: []u8,
    stderr: []u8,
    success: bool,
    exit_code: ?u32 = null,
    interrupted: bool = false,

    /// Free both stdout and stderr buffers.
    pub fn deinit(self: *const RunResult, allocator: std.mem.Allocator) void {
        if (self.stdout.len > 0) allocator.free(self.stdout);
        if (self.stderr.len > 0) allocator.free(self.stderr);
    }
};

/// Options for running a child process.
pub const RunOptions = struct {
    cwd: ?[]const u8 = null,
    env_map: ?*std.process.EnvMap = null,
    max_output_bytes: usize = 1_048_576,
    cancel_flag: ?*const AtomicBool = null,
};

const CancelWatcherCtx = struct {
    child: *std.process.Child,
    cancel_flag: *const AtomicBool,
    done: *AtomicBool,
};

fn cancelWatcherMain(ctx: *CancelWatcherCtx) void {
    while (!ctx.done.load(.acquire)) {
        if (ctx.cancel_flag.load(.acquire)) {
            if (comptime @import("builtin").os.tag == .windows) {
                std.os.windows.TerminateProcess(ctx.child.id, 1) catch {};
            } else {
                std.posix.kill(ctx.child.id, std.posix.SIG.TERM) catch {};
            }
            break;
        }
        std.Thread.sleep(20 * std.time.ns_per_ms);
    }
}

/// Run a child process, capture stdout and stderr, and return the result.
///
/// The caller owns the returned stdout and stderr buffers.
/// Use `result.deinit(allocator)` to free them.
pub fn run(
    allocator: std.mem.Allocator,
    argv: []const []const u8,
    opts: RunOptions,
) !RunResult {
    var child = std.process.Child.init(argv, allocator);
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;
    if (opts.cwd) |cwd| child.cwd = cwd;
    if (opts.env_map) |env| child.env_map = env;

    try child.spawn();

    const effective_cancel_flag = opts.cancel_flag orelse thread_interrupt_flag;
    var cancel_done = AtomicBool.init(false);
    var cancel_watcher: ?std.Thread = null;
    var watcher_ctx: CancelWatcherCtx = undefined;
    if (effective_cancel_flag) |flag| {
        watcher_ctx = .{
            .child = &child,
            .cancel_flag = flag,
            .done = &cancel_done,
        };
        cancel_watcher = std.Thread.spawn(.{}, cancelWatcherMain, .{&watcher_ctx}) catch null;
    }
    defer {
        cancel_done.store(true, .release);
        if (cancel_watcher) |t| t.join();
    }

    const stdout = if (child.stdout) |stdout_file| blk: {
        break :blk stdout_file.readToEndAlloc(allocator, opts.max_output_bytes) catch |err| {
            if (effective_cancel_flag != null and effective_cancel_flag.?.load(.acquire)) {
                break :blk try allocator.dupe(u8, "");
            }
            return err;
        };
    } else try allocator.dupe(u8, "");
    errdefer allocator.free(stdout);

    const stderr = if (child.stderr) |stderr_file| blk: {
        break :blk stderr_file.readToEndAlloc(allocator, opts.max_output_bytes) catch |err| {
            if (effective_cancel_flag != null and effective_cancel_flag.?.load(.acquire)) {
                break :blk try allocator.dupe(u8, "");
            }
            return err;
        };
    } else try allocator.dupe(u8, "");
    errdefer allocator.free(stderr);

    const term = try child.wait();
    const interrupted = if (effective_cancel_flag) |flag| flag.load(.acquire) else false;

    return switch (term) {
        .Exited => |code| .{
            .stdout = stdout,
            .stderr = stderr,
            .success = code == 0,
            .exit_code = code,
            .interrupted = interrupted,
        },
        else => .{
            .stdout = stdout,
            .stderr = stderr,
            .success = false,
            .exit_code = null,
            .interrupted = interrupted,
        },
    };
}

// ── Tests ───────────────────────────────────────────────────────────

const builtin = @import("builtin");

test "run echo returns stdout" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const result = try run(allocator, &.{ "echo", "hello" }, .{});
    defer result.deinit(allocator);

    try std.testing.expect(result.success);
    try std.testing.expectEqual(@as(u32, 0), result.exit_code.?);
    try std.testing.expect(std.mem.indexOf(u8, result.stdout, "hello") != null);
}

test "run failing command returns exit code" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const result = try run(allocator, &.{ "ls", "/nonexistent_dir_xyz_42" }, .{});
    defer result.deinit(allocator);

    try std.testing.expect(!result.success);
    try std.testing.expect(result.exit_code.? != 0);
    try std.testing.expect(result.stderr.len > 0);
}

test "run with cwd" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const result = try run(allocator, &.{"pwd"}, .{ .cwd = "/tmp" });
    defer result.deinit(allocator);

    try std.testing.expect(result.success);
    // /tmp may resolve to /private/tmp on macOS
    try std.testing.expect(result.stdout.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, result.stdout, "tmp") != null);
}

test "run honors cancel flag and interrupts child" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    const allocator = std.testing.allocator;

    var cancel = AtomicBool.init(false);

    const ThreadResult = struct {
        res: ?RunResult = null,
        err: ?anyerror = null,
    };
    var thread_result = ThreadResult{};

    const Runner = struct {
        fn runThread(
            allocator_inner: std.mem.Allocator,
            cancel_flag: *const AtomicBool,
            out: *ThreadResult,
        ) void {
            out.res = run(allocator_inner, &.{ "sh", "-c", "sleep 5; echo done" }, .{
                .cancel_flag = cancel_flag,
            }) catch |err| {
                out.err = err;
                return;
            };
        }
    };

    const t = try std.Thread.spawn(.{}, Runner.runThread, .{ allocator, &cancel, &thread_result });
    std.Thread.sleep(100 * std.time.ns_per_ms);
    cancel.store(true, .release);
    t.join();

    try std.testing.expect(thread_result.err == null);
    const result = thread_result.res orelse return error.TestUnexpectedResult;
    defer result.deinit(allocator);
    try std.testing.expect(!result.success);
    try std.testing.expect(result.interrupted);
}

test "RunResult deinit frees buffers" {
    const allocator = std.testing.allocator;
    const stdout = try allocator.dupe(u8, "output");
    const stderr = try allocator.dupe(u8, "error");
    const result = RunResult{
        .stdout = stdout,
        .stderr = stderr,
        .success = true,
        .exit_code = 0,
    };
    result.deinit(allocator);
}

test "RunResult deinit with empty buffers" {
    const allocator = std.testing.allocator;
    const result = RunResult{
        .stdout = "",
        .stderr = "",
        .success = true,
        .exit_code = 0,
    };
    result.deinit(allocator); // should not crash or attempt to free ""
}
