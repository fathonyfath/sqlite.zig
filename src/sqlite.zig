const std = @import("std");
const c = @cImport({
    @cInclude("sqlite3.h");
});

pub const Sqlite = struct {
    sqlite_ref: *c.sqlite3,

    pub fn open(filename: []const u8) !Sqlite {
        var db: ?*c.sqlite3 = null;
        const result = c.sqlite3_open(filename.ptr, &db);
        if (result == c.SQLITE_OK) {
            return .{ .sqlite_ref = db.? };
        }
        return error.OpenDatabaseError;
    }

    pub fn close(db: *Sqlite) void {
        _ = c.sqlite3_close(db.sqlite_ref);
        db.sqlite_ref = undefined;
    }

    pub fn prepare(db: Sqlite, query: []const u8, extras: ?ExtraResults) !Statement {
        var stmt: ?*c.sqlite3_stmt = null;
        var leftover: ?[*:0]const u8 = null;

        const result = c.sqlite3_prepare_v2(
            db.sqlite_ref,
            query.ptr,
            -1,
            &stmt,
            &leftover,
        );

        if (result == c.SQLITE_OK) {
            if (leftover) |leftover_qry| {
                const leftover_slice = std.mem.span(leftover_qry);
                if (extras) |e| {
                    if (e.leftover_query) |leftover_ptr| {
                        leftover_ptr.* = leftover_slice;
                    }
                }
            }

            if (stmt) |s| {
                return .{
                    .sqlite_ref = db.sqlite_ref,
                    .stmt_ref = s,
                };
            } else {
                if (extras) |e| {
                    if (e.error_msg) |msg_ctr| {
                        msg_ctr.* = "query is empty";
                    }
                }

                return error.EmptyQueryError;
            }
        }

        if (extras) |e| {
            if (e.error_msg) |msg_ctr| {
                const msg: []const u8 = std.mem.sliceTo(
                    c.sqlite3_errmsg(db.sqlite_ref),
                    0,
                );
                msg_ctr.* = msg;
            }
        }

        return error.PrepareStatementError;
    }
};

pub const Statement = struct {
    sqlite_ref: *c.sqlite3,
    stmt_ref: *c.sqlite3_stmt,

    pub fn finalize(stmt: Statement) void {
        _ = c.sqlite3_finalize(stmt.stmt_ref);
    }

    pub fn reset(stmt: Statement) void {
        _ = c.sqlite3_reset(stmt.stmt_ref);
    }

    pub fn step(stmt: Statement) ?Result {
        const result = c.sqlite3_step(stmt.stmt_ref);
        if (result == c.SQLITE_DONE) {
            return Result{ .done = void{} };
        } else if (result == c.SQLITE_ROW) {
            return Result{ .row = .{ .stmt = stmt } };
        } else {
            return null;
        }
    }

    pub fn nextRow(stmt: Statement) ?Result.Row {
        if (stmt.step()) |result| {
            if (result == .row) return result.row;
        }
        return null;
    }

    pub fn bindValues(stmt: Statement, values: anytype) void {
        inline for (std.meta.fields(@TypeOf(values)), 1..) |field, index| {
            stmt.bindValue(index, @field(values, field.name));
        }
    }

    pub fn bindValue(stmt: Statement, index: usize, value: anytype) void {
        const type_info = @typeInfo(@TypeOf(value));
        switch (type_info) {
            .Null => stmt.bindNull(index),
            .Int, .ComptimeInt => stmt.bindInt(index, value),
            .Float, .ComptimeFloat => stmt.bindFloat(index, value),
            .Pointer => |ptr| switch (ptr.size) {
                .One => stmt.bindValue(index, value.*),
                .Slice => switch (ptr.child) {
                    u8 => stmt.bindText(index, value),
                    else => @compileError("Cannot bind a value with the type of " ++ @typeName(@TypeOf(value))),
                },
                else => @compileError("Cannot bind a value with the type of " ++ @typeName(@TypeOf(value))),
            },
            .Array => |array| switch (array.child) {
                u8 => stmt.bindText(index, &value),
                else => @compileError("Cannot bind a value with the type of " ++ @typeName(@TypeOf(value))),
            },
            .Optional => if (value) |v| {
                stmt.bindValue(index, v);
            } else {
                stmt.bindValue(index, null);
            },
            .Struct => {
                if (@TypeOf(value) == Blob) {
                    stmt.bindBlob(index, value.content);
                } else @compileError("Cannot bind a value with the type of " ++ @typeName(@TypeOf(value)));
            },
            else => @compileError("Cannot bind a value with the type of " ++ @typeName(@TypeOf(value))),
        }
    }

    pub fn expandedSql(stmt: Statement) []const u8 {
        return std.mem.span(c.sqlite3_expanded_sql(stmt.stmt_ref));
    }

    fn bindNull(stmt: Statement, index: usize) void {
        _ = c.sqlite3_bind_null(stmt.stmt_ref, @intCast(index));
    }

    fn bindInt(stmt: Statement, index: usize, value: i64) void {
        _ = c.sqlite3_bind_int64(stmt.stmt_ref, @intCast(index), value);
    }

    fn bindFloat(stmt: Statement, index: usize, value: f64) void {
        _ = c.sqlite3_bind_double(stmt.stmt_ref, @intCast(index), value);
    }

    fn bindText(stmt: Statement, index: usize, value: []const u8) void {
        _ = c.sqlite3_bind_text(stmt.stmt_ref, @intCast(index), value.ptr, @intCast(value.len), c.SQLITE_STATIC);
    }

    fn bindBlob(stmt: Statement, index: usize, value: []const u8) void {
        _ = c.sqlite3_bind_blob(stmt.stmt_ref, @intCast(index), value.ptr, @intCast(value.len), c.SQLITE_STATIC);
    }
};

pub const Result = union(enum) {
    row: Row,
    done: void,

    pub const Row = struct {
        stmt: Statement,

        pub fn int(row: Row, index: usize) i64 {
            return @intCast(c.sqlite3_column_int64(row.stmt.stmt_ref, @intCast(index)));
        }

        pub fn nullableInt(row: Row, index: usize) ?i64 {
            if (c.sqlite3_column_type(row.stmt.stmt_ref, @intCast(index)) == c.SQLITE_INTEGER) {
                return row.int(index);
            } else {
                return null;
            }
        }

        pub fn float(row: Row, index: usize) f64 {
            return @floatCast(c.sqlite3_column_double(row.stmt.stmt_ref, @intCast(index)));
        }

        pub fn nullableFloat(row: Row, index: usize) ?f64 {
            if (c.sqlite3_column_type(row.stmt.stmt_ref, @intCast(index)) == c.SQLITE_FLOAT) {
                return row.float(index);
            } else {
                return null;
            }
        }

        pub fn text(row: Row, index: usize) []const u8 {
            const bytes = c.sqlite3_column_bytes(row.stmt.stmt_ref, @intCast(index));
            if (bytes == 0) {
                return "";
            }

            const data = c.sqlite3_column_text(row.stmt.stmt_ref, @intCast(index));
            return data[0..@intCast(bytes)];
        }

        pub fn nullableText(row: Row, index: usize) ?[]const u8 {
            if (c.sqlite3_column_type(row.stmt.stmt_ref, @intCast(index)) == c.SQLITE_TEXT) {
                return row.text(index);
            } else {
                return null;
            }
        }

        pub fn blob(row: Row, index: usize) Blob {
            const bytes = c.sqlite3_column_bytes(row.stmt.stmt_ref, @intCast(index));
            if (bytes == 0) {
                return Blob{ .content = &.{} };
            }

            const data: [*c]const u8 = @ptrCast(c.sqlite3_column_blob(row.stmt.stmt_ref, @intCast(index)));
            const content: []const u8 = data[0..@intCast(bytes)];
            return Blob{ .content = content };
        }

        pub fn nullableBlob(row: Row, index: usize) ?Blob {
            if (c.sqlite3_column_type(row.stmt.stmt_ref, @intCast(index)) == c.SQLITE_BLOB) {
                return row.blob(index);
            } else {
                return null;
            }
        }

        pub fn read(row: Row, comptime Type: type) Type {
            const type_info = @typeInfo(Type);
            const column_size: usize = @intCast(c.sqlite3_column_count(row.stmt.stmt_ref));
            switch (type_info) {
                .Struct => |s| {
                    std.debug.assert(s.fields.len == column_size);
                    return row.readStruct(Type);
                },
                else => @compileError("Unsupported type of " ++ @typeName(type) ++ ". Only a struct is supported."),
            }
        }

        fn readStruct(row: Row, comptime Type: type) Type {
            const struct_type_info = @typeInfo(Type).Struct;
            var result: Type = undefined;

            inline for (struct_type_info.fields, 0..) |field, index| {
                @field(result, field.name) = row.readField(field.type, index).?;
            }

            return result;
        }

        fn readField(row: Row, comptime FieldType: type, index: usize) ?FieldType {
            const field_type_info = @typeInfo(FieldType);

            switch (field_type_info) {
                .Int => return row.int(index),
                .Float => return row.float(index),
                .Pointer => |ptr| switch (ptr.size) {
                    .One => return row.readField(@TypeOf(ptr.*), index),
                    .Slice => switch (ptr.child) {
                        u8 => return row.text(index),
                        else => @compileError("Cannot read a value with the type of " ++ @typeName(FieldType)),
                    },
                    else => @compileError("Cannot read a value with the type of " ++ @typeName(FieldType)),
                },
                .Array => |array| switch (array.child) {
                    u8 => return row.text(index),
                    else => @compileError("Cannot read a value with the type of " ++ @typeName(FieldType)),
                },
                .Optional => |opt| return row.readField(opt.child, index),
                .Struct => {
                    if (FieldType == Blob) {
                        return row.blob(index);
                    } else @compileError("Cannot read a value with the type of " ++ @typeName(FieldType));
                },
                else => @compileError("Cannot read a value with the type of " ++ @typeName(FieldType)),
            }
        }
    };
};

pub const Blob = struct {
    content: []const u8,
};

pub const ExtraResults = struct {
    error_msg: ?*[]const u8 = null,
    leftover_query: ?*[]const u8 = null,
};

test {
    var db = try Sqlite.open(":memory:");
    defer db.close();

    {
        var create_stmt = try db.prepare(
            "CREATE TABLE IF NOT EXISTS sample_table (id INTEGER PRIMARY KEY AUTOINCREMENT, number_1 REAL NOT NULL, number_2 INTEGER NOT NULL, text TEXT NOT NULL, blob BLOB NOT NULL);",
            null,
        );
        defer create_stmt.reset();
        _ = create_stmt.step();
    }

    {
        const alloc = std.testing.allocator;
        inline for (0..10) |i| {
            const text = try std.fmt.allocPrint(alloc, "Index {d}", .{i});
            defer alloc.free(text);
            try insertTestRow(db, i, text);
        }
    }

    {
        var select_stmt = try db.prepare(
            \\SELECT * FROM sample_table;
        ,
            null,
        );
        defer select_stmt.finalize();

        while (select_stmt.nextRow()) |row| {
            const id = row.int(0);
            const number_1 = row.int(1);
            const number_2 = row.float(2);
            const text = row.text(3);
            const blob = row.blob(4);
            std.debug.print("{d} - {d} {d} {s} {any}\n", .{ id, number_1, number_2, text, blob });
        }
    }

    {
        var select_alt_stmt = try db.prepare(
            \\SELECT * FROM sample_table;
        ,
            null,
        );
        defer select_alt_stmt.finalize();

        while (select_alt_stmt.nextRow()) |row| {
            const QueryResult = struct {
                id: i64,
                number_1: i64,
                number_2: f64,
                text: []const u8,
                blob: Blob,
            };
            const result = row.read(QueryResult);
            std.debug.print("{any}\n", .{result});
        }
    }
}

fn insertTestRow(db: Sqlite, number: i32, text: []const u8) !void {
    var stmt = try db.prepare(
        \\INSERT INTO sample_table(number_1, number_2, text, blob) VALUES (?, ?, ?, ?);
    ,
        null,
    );
    defer stmt.finalize();

    const values = .{
        .idx = number,
        .flt = @as(f64, @floatFromInt(number)) + 0.99,
        .txt = text,
        .blb = Blob{ .content = text },
    };

    stmt.bindValues(values);
    _ = stmt.step();
}
