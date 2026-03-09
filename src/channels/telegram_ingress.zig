const std = @import("std");
const root = @import("root.zig");

pub const TEXT_MESSAGE_DEBOUNCE_SECS: u64 = 3;
pub const LARGE_TEXT_CHAIN_DEBOUNCE_SECS: u64 = 8;
pub const LARGE_TEXT_CHAIN_MIN_PARTS: usize = 4;
pub const LARGE_TEXT_CHAIN_MIN_BYTES: usize = 16 * 1024;
pub const TEXT_SPLIT_LIKELY_MIN_LEN: usize = 500;

pub const PendingTextChainStats = struct {
    latest: u64,
    parts: usize,
    total_bytes: usize,
};

fn sameSenderAndChat(a: root.ChannelMessage, b: root.ChannelMessage) bool {
    return std.mem.eql(u8, a.sender, b.sender) and std.mem.eql(u8, a.id, b.id);
}

fn sameChat(a: root.ChannelMessage, b: root.ChannelMessage) bool {
    return std.mem.eql(u8, a.sender, b.sender);
}

fn matchesPendingTextKey(msg: root.ChannelMessage, id: []const u8, sender: []const u8) bool {
    return std.mem.eql(u8, msg.id, id) and std.mem.eql(u8, msg.sender, sender);
}

fn hasMessageId(msg: root.ChannelMessage) bool {
    return msg.message_id != null;
}

fn isLikelySplitTextChunk(msg: root.ChannelMessage) bool {
    return msg.content.len >= TEXT_SPLIT_LIKELY_MIN_LEN;
}

pub fn isSlashCommandMessage(content: []const u8) bool {
    const trimmed = std.mem.trim(u8, content, " \t\r\n");
    return std.mem.startsWith(u8, trimmed, "/");
}

pub fn pendingTextChainStatsForKey(
    id: []const u8,
    sender: []const u8,
    pending_messages: []const root.ChannelMessage,
    received_at: []const u64,
) ?PendingTextChainStats {
    const n = @min(pending_messages.len, received_at.len);
    var seen = false;
    var latest: u64 = 0;
    var parts: usize = 0;
    var total_bytes: usize = 0;
    for (0..n) |i| {
        const msg = pending_messages[i];
        if (!matchesPendingTextKey(msg, id, sender)) continue;
        if (!seen or received_at[i] > latest) latest = received_at[i];
        seen = true;
        parts += 1;
        total_bytes += msg.content.len;
    }
    if (!seen) return null;
    return .{ .latest = latest, .parts = parts, .total_bytes = total_bytes };
}

pub fn textDebounceSecsForChain(parts: usize, total_bytes: usize) u64 {
    if (parts >= LARGE_TEXT_CHAIN_MIN_PARTS or total_bytes >= LARGE_TEXT_CHAIN_MIN_BYTES) {
        return LARGE_TEXT_CHAIN_DEBOUNCE_SECS;
    }
    return TEXT_MESSAGE_DEBOUNCE_SECS;
}

fn chainStillWarm(now: u64, stats: PendingTextChainStats) bool {
    return now <= stats.latest + textDebounceSecsForChain(stats.parts, stats.total_bytes);
}

fn chainIsMature(now: u64, stats: PendingTextChainStats) bool {
    return !chainStillWarm(now, stats);
}

pub fn pendingTextBuffersInSync(
    pending_messages: []const root.ChannelMessage,
    received_at: []const u64,
) bool {
    return pending_messages.len == received_at.len;
}

pub fn nextPendingTextDeadline(
    pending_messages: []const root.ChannelMessage,
    received_at: []const u64,
) ?u64 {
    const n = @min(pending_messages.len, received_at.len);
    var seen = false;
    var next_deadline: u64 = 0;
    for (0..n) |i| {
        const stats = pendingTextChainStatsForKey(
            pending_messages[i].id,
            pending_messages[i].sender,
            pending_messages,
            received_at,
        ) orelse continue;
        const deadline = stats.latest + textDebounceSecsForChain(stats.parts, stats.total_bytes);
        if (!seen or deadline < next_deadline) next_deadline = deadline;
        seen = true;
    }
    return if (seen) next_deadline else null;
}

pub fn shouldDebounceTextMessage(
    now: u64,
    pending_messages: []const root.ChannelMessage,
    received_at: []const u64,
    msg: root.ChannelMessage,
) bool {
    if (!hasMessageId(msg)) return false;
    if (isSlashCommandMessage(msg.content)) return false;
    if (isLikelySplitTextChunk(msg)) return true;

    const stats = pendingTextChainStatsForKey(
        msg.id,
        msg.sender,
        pending_messages,
        received_at,
    ) orelse return false;
    return chainStillWarm(now, stats);
}

pub fn pendingTextChainMatureAtIndex(
    now: u64,
    pending_messages: []const root.ChannelMessage,
    received_at: []const u64,
    index: usize,
) bool {
    if (index >= pending_messages.len or index >= received_at.len) return false;

    const msg = pending_messages[index];
    const stats = pendingTextChainStatsForKey(
        msg.id,
        msg.sender,
        pending_messages,
        received_at,
    ) orelse return false;
    return chainIsMature(now, stats);
}

pub fn cancelPendingTextChainForKey(
    allocator: std.mem.Allocator,
    pending_messages: *std.ArrayListUnmanaged(root.ChannelMessage),
    received_at: *std.ArrayListUnmanaged(u64),
    id: []const u8,
    sender: []const u8,
) void {
    var i: usize = 0;
    while (i < pending_messages.items.len and i < received_at.items.len) {
        const pending = pending_messages.items[i];
        if (!matchesPendingTextKey(pending, id, sender)) {
            i += 1;
            continue;
        }

        const removed = pending_messages.orderedRemove(i);
        _ = received_at.orderedRemove(i);
        removed.deinit(allocator);
    }
}

fn findNextMergeCandidateIndex(messages: []const root.ChannelMessage, start: usize) ?usize {
    const current = messages[start];
    for (start + 1..messages.len) |idx| {
        const next = messages[idx];
        if (!sameChat(current, next)) continue;
        if (!sameSenderAndChat(current, next)) break;
        if (!hasMessageId(next)) break;
        if (isSlashCommandMessage(next.content)) break;
        return idx;
    }
    return null;
}

fn buildMergedContent(
    allocator: std.mem.Allocator,
    first: []const u8,
    second: []const u8,
) ?[]u8 {
    var merged: std.ArrayListUnmanaged(u8) = .empty;
    defer merged.deinit(allocator);

    merged.appendSlice(allocator, first) catch return null;
    merged.appendSlice(allocator, "\n") catch return null;
    merged.appendSlice(allocator, second) catch return null;
    return merged.toOwnedSlice(allocator) catch null;
}

fn replaceMergedContent(
    allocator: std.mem.Allocator,
    dst: *root.ChannelMessage,
    src: root.ChannelMessage,
) bool {
    const new_content = buildMergedContent(allocator, dst.content, src.content) orelse return false;
    allocator.free(dst.content);
    dst.content = new_content;
    dst.message_id = src.message_id;
    return true;
}

pub fn mergeConsecutiveMessages(
    allocator: std.mem.Allocator,
    messages: *std.ArrayListUnmanaged(root.ChannelMessage),
) void {
    if (messages.items.len <= 1) return;

    var i: usize = 0;
    while (i < messages.items.len) {
        if (!hasMessageId(messages.items[i])) {
            i += 1;
            continue;
        }
        if (isSlashCommandMessage(messages.items[i].content)) {
            i += 1;
            continue;
        }

        const found_idx = findNextMergeCandidateIndex(messages.items, i) orelse {
            i += 1;
            continue;
        };

        if (!replaceMergedContent(allocator, &messages.items[i], messages.items[found_idx])) {
            i += 1;
            continue;
        }

        var extra = messages.orderedRemove(found_idx);
        extra.deinit(allocator);
    }
}
