const providers = @import("providers/root.zig");

pub const OutboundStage = enum {
    chunk,
    final,
};

pub const Event = struct {
    stage: OutboundStage,
    text: []const u8 = "",
};

pub const Sink = struct {
    callback: *const fn (ctx: *anyopaque, event: Event) void,
    ctx: *anyopaque,

    pub fn emit(self: Sink, event: Event) void {
        self.callback(self.ctx, event);
    }

    pub fn emitChunk(self: Sink, text: []const u8) void {
        if (text.len == 0) return;
        self.emit(.{
            .stage = .chunk,
            .text = text,
        });
    }

    pub fn emitFinal(self: Sink) void {
        self.emit(.{ .stage = .final });
    }
};

pub fn eventFromProviderChunk(chunk: providers.StreamChunk) ?Event {
    if (chunk.is_final) return .{ .stage = .final };
    if (chunk.delta.len == 0) return null;
    return .{
        .stage = .chunk,
        .text = chunk.delta,
    };
}

pub fn forwardProviderChunk(sink: Sink, chunk: providers.StreamChunk) void {
    if (eventFromProviderChunk(chunk)) |event| {
        sink.emit(event);
    }
}

// ---------------------------------------------------------------------------
// TagFilter – state-machine that strips <tool_call>…</tool_call> (and bracket
// variants) from a stream of chunks before forwarding to an inner Sink.
// ---------------------------------------------------------------------------

pub const TagFilter = struct {
    inner: Sink,
    state: State = .passthrough,
    buf: [max_tag_len]u8 = undefined,
    buf_len: u8 = 0,

    const State = enum {
        passthrough,
        maybe_open, // buffering after '<', checking if prefix matches
        skip_to_angle_close, // prefix matched, eating until '>'
        inside_tag, // inside tag body, suppressing output
        maybe_close, // buffering after '<', checking if close tag matches
    };

    // Opening tag prefixes. After matching, skip until '>'.
    // Handles both `<tool_call>` and `<tool_result name="x" status="ok">`.
    const open_prefixes = [_][]const u8{
        "<tool_call",
        "<tool_result",
    };

    // Closing tags (fixed match).
    const close_tags = [_][]const u8{
        "</tool_call>",
        "</tool_result>",
    };

    const max_prefix_len = 12; // "<tool_result".len
    const max_tag_len = 14; // "</tool_result>".len

    pub fn init(inner: Sink) TagFilter {
        return .{ .inner = inner };
    }

    /// Return a Sink whose callback routes through this filter.
    pub fn sink(self: *TagFilter) Sink {
        return .{
            .callback = filterCallback,
            .ctx = @ptrCast(self),
        };
    }

    fn filterCallback(ctx: *anyopaque, event: Event) void {
        const self: *TagFilter = @ptrCast(@alignCast(ctx));
        if (event.stage == .final) {
            // Flush any pending buffer as-is (incomplete tag at end of stream).
            self.flushBuf();
            self.inner.emit(event);
            return;
        }
        self.process(event.text);
    }

    fn process(self: *TagFilter, text: []const u8) void {
        var clean_start: usize = 0;
        for (text, 0..) |b, i| {
            switch (self.state) {
                .passthrough => {
                    if (b == '<') {
                        // Flush clean text accumulated so far.
                        if (i > clean_start)
                            self.inner.emitChunk(text[clean_start..i]);
                        self.buf[0] = b;
                        self.buf_len = 1;
                        self.state = .maybe_open;
                    }
                },
                .maybe_open => {
                    self.buf[self.buf_len] = b;
                    self.buf_len += 1;
                    const prefix = self.buf[0..self.buf_len];
                    // Check if the bytes before this one match a full open prefix
                    // and this byte is a delimiter ('>' closes the tag, ' ' starts attrs).
                    if (self.buf_len > 1 and (b == '>' or b == ' ') and
                        matchesAnyPrefix(prefix[0 .. self.buf_len - 1], &open_prefixes))
                    {
                        self.buf_len = 0;
                        if (b == '>') {
                            self.state = .inside_tag;
                        } else {
                            self.state = .skip_to_angle_close;
                        }
                        clean_start = i + 1;
                        continue;
                    }
                    // Still a valid prefix of some open tag — keep buffering.
                    if (prefixOfAny(prefix, &open_prefixes)) {
                        clean_start = i + 1;
                        continue;
                    }
                    // Not a prefix of any tag — flush buffer and resume passthrough.
                    self.inner.emitChunk(self.buf[0..self.buf_len]);
                    self.buf_len = 0;
                    self.state = .passthrough;
                    clean_start = i + 1;
                },
                .skip_to_angle_close => {
                    clean_start = i + 1;
                    if (b == '>') {
                        self.state = .inside_tag;
                    }
                },
                .inside_tag => {
                    clean_start = i + 1;
                    if (b == '<') {
                        self.buf[0] = b;
                        self.buf_len = 1;
                        self.state = .maybe_close;
                    }
                },
                .maybe_close => {
                    clean_start = i + 1;
                    self.buf[self.buf_len] = b;
                    self.buf_len += 1;
                    const prefix = self.buf[0..self.buf_len];
                    if (matchesAny(prefix, &close_tags)) |_| {
                        // Complete close tag matched — back to passthrough.
                        self.buf_len = 0;
                        self.state = .passthrough;
                        clean_start = i + 1;
                        continue;
                    }
                    if (!prefixOfAny(prefix, &close_tags) or self.buf_len >= max_tag_len) {
                        // Not a close tag — stay inside, discard buffer.
                        self.buf_len = 0;
                        self.state = .inside_tag;
                        continue;
                    }
                    // Still a valid prefix of a close tag — keep buffering.
                },
            }
        }
        // Flush remaining clean text in passthrough mode.
        if (self.state == .passthrough and clean_start < text.len)
            self.inner.emitChunk(text[clean_start..]);
    }

    fn flushBuf(self: *TagFilter) void {
        if (self.buf_len > 0 and self.state == .maybe_open) {
            // Incomplete open tag at end of stream — not a real tag, flush it.
            self.inner.emitChunk(self.buf[0..self.buf_len]);
        }
        self.buf_len = 0;
        self.state = .passthrough;
    }

    /// Returns the index if `text` exactly matches any entry in `tags`.
    fn matchesAny(text: []const u8, tags: []const []const u8) ?usize {
        for (tags, 0..) |tag, i| {
            if (std.mem.eql(u8, text, tag)) return i;
        }
        return null;
    }

    /// Returns true if `text` exactly matches any entry in `prefixes`.
    fn matchesAnyPrefix(text: []const u8, prefixes: []const []const u8) bool {
        for (prefixes) |p| {
            if (std.mem.eql(u8, text, p)) return true;
        }
        return false;
    }

    /// Returns true if `text` is a valid prefix of at least one entry in `tags`.
    fn prefixOfAny(text: []const u8, tags: []const []const u8) bool {
        for (tags) |tag| {
            if (text.len <= tag.len and std.mem.eql(u8, text, tag[0..text.len]))
                return true;
        }
        return false;
    }
};

const std = @import("std");

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn collectChunks(comptime max: usize) type {
    return struct {
        chunks: [max][]const u8 = undefined,
        count: usize = 0,
        got_final: bool = false,

        fn callback(ctx: *anyopaque, event: Event) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (event.stage == .final) {
                self.got_final = true;
                return;
            }
            if (self.count < max) {
                self.chunks[self.count] = event.text;
                self.count += 1;
            }
        }

        fn joined(self: *const @This(), buf: []u8) []const u8 {
            var pos: usize = 0;
            for (self.chunks[0..self.count]) |c| {
                @memcpy(buf[pos..][0..c.len], c);
                pos += c.len;
            }
            return buf[0..pos];
        }

        fn sink(self: *@This()) Sink {
            return .{ .callback = callback, .ctx = @ptrCast(self) };
        }
    };
}

test "TagFilter passthrough without tags" {
    var col = collectChunks(16){};
    var filter = TagFilter.init(col.sink());
    const s = filter.sink();
    s.emitChunk("Hello ");
    s.emitChunk("world!");
    s.emitFinal();
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("Hello world!", col.joined(&buf));
    try std.testing.expect(col.got_final);
}

test "TagFilter strips complete tool_call in single chunk" {
    var col = collectChunks(16){};
    var filter = TagFilter.init(col.sink());
    const s = filter.sink();
    s.emitChunk("Hi <tool_call>{\"name\":\"x\",\"arguments\":{}}</tool_call> bye");
    s.emitFinal();
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("Hi  bye", col.joined(&buf));
}

test "TagFilter strips tool_result with attributes" {
    var col = collectChunks(16){};
    var filter = TagFilter.init(col.sink());
    const s = filter.sink();
    s.emitChunk("A<tool_result name=\"shell\" status=\"success\">output</tool_result>B");
    s.emitFinal();
    var buf: [128]u8 = undefined;
    try std.testing.expectEqualStrings("AB", col.joined(&buf));
}

test "TagFilter strips tool_result without attributes" {
    var col = collectChunks(16){};
    var filter = TagFilter.init(col.sink());
    const s = filter.sink();
    s.emitChunk("A<tool_result>output</tool_result>B");
    s.emitFinal();
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("AB", col.joined(&buf));
}

test "TagFilter tag split across chunks" {
    var col = collectChunks(16){};
    var filter = TagFilter.init(col.sink());
    const s = filter.sink();
    s.emitChunk("Hello <tool_c");
    s.emitChunk("all>{\"name\":\"x\"}</tool_call> world");
    s.emitFinal();
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("Hello  world", col.joined(&buf));
}

test "TagFilter close tag split across chunks" {
    var col = collectChunks(16){};
    var filter = TagFilter.init(col.sink());
    const s = filter.sink();
    s.emitChunk("<tool_call>body</tool_");
    s.emitChunk("call>after");
    s.emitFinal();
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("after", col.joined(&buf));
}

test "TagFilter false positive angle bracket" {
    var col = collectChunks(16){};
    var filter = TagFilter.init(col.sink());
    const s = filter.sink();
    s.emitChunk("a < b > c");
    s.emitFinal();
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("a < b > c", col.joined(&buf));
}

test "TagFilter multiple tool calls" {
    var col = collectChunks(16){};
    var filter = TagFilter.init(col.sink());
    const s = filter.sink();
    s.emitChunk("A<tool_call>1</tool_call>B<tool_call>2</tool_call>C");
    s.emitFinal();
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("ABC", col.joined(&buf));
}

test "TagFilter incomplete open tag at end flushes on final" {
    var col = collectChunks(16){};
    var filter = TagFilter.init(col.sink());
    const s = filter.sink();
    s.emitChunk("end<tool_c");
    s.emitFinal();
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("end<tool_c", col.joined(&buf));
    try std.testing.expect(col.got_final);
}
