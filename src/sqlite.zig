const std = @import("std");
const c = @cImport({
    @cInclude("sqlite3.h");
});
const Allocator = std.mem.Allocator;

/// A wrapper around a SQLite database connection.
pub const Sqlite = struct {
    sqlite_ref: *c.sqlite3,

    /// Opens a SQLite database connection.
    ///
    /// The `filename` parameter specifies the path to the database file.
    /// If the file does not exist, it will be created.
    ///
    /// Returns a `Sqlite` instance on success, or an error on failure.
    pub fn open(filename: []const u8) !Sqlite {
        var db: ?*c.sqlite3 = null;
        const result = c.sqlite3_open(filename.ptr, &db);
        if (result == c.SQLITE_OK) {
            return .{ .sqlite_ref = db.? };
        }
        return error.OpenDatabaseError;
    }

    /// Closes a SQLite database connection.
    pub fn close(db: *Sqlite) void {
        _ = c.sqlite3_close(db.sqlite_ref);
        db.sqlite_ref = undefined;
    }

    /// Prepares an SQL statement for execution.
    ///
    /// The `query` parameter specifies the SQL statement to prepare.
    /// The `extras` parameter is optional and can be used to provide additional information about the query.
    ///
    /// Returns a `Statement` instance on success, or an error on failure.
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

    /// Executes an SQL statement.
    ///
    /// The `query` parameter specifies the SQL statement to execute.
    ///
    /// This function is useful for executing SQL statements that do not return any data, such as `CREATE TABLE` or `INSERT`.
    /// For statements that return data, use the `Sqlite.prepare` and `Statement.step` functions.
    ///
    /// Returns an error on failure.
    pub fn exec(db: Sqlite, query: []const u8) !void {
        const result = c.sqlite3_exec(db.sqlite_ref, query.ptr, null, null, null);

        if (result != c.SQLITE_OK) {
            return error.SqliteExecError;
        }
    }

    /// Executes an SQL statement, returning an allocated error message on failure.
    ///
    /// The `alloc` parameter specifies the allocator to use for allocating the error message.
    /// The `query` parameter specifies the SQL statement to execute.
    ///
    /// This function is useful for executing SQL statements that do not return any data,
    /// such as `CREATE TABLE` or `INSERT`. For statements that return data, use the
    /// `Sqlite.prepare` and `Statement.step` functions.
    ///
    /// Returns an optional allocated error message. If the operation is successful,
    /// `null` is returned. If an error occurs, an allocated error message is returned,
    /// which is owned by the caller and must be freed using the provided allocator when no longer needed.
    pub fn execWithError(db: Sqlite, alloc: std.mem.Allocator, query: []const u8) !?[]const u8 {
        var err_msg: ?[*:0]u8 = null;
        const result = c.sqlite3_exec(db.sqlite_ref, query.ptr, null, null, &err_msg);

        if (result != c.SQLITE_OK) {
            if (err_msg) |msg| {
                const zig_msg: []const u8 = std.mem.sliceTo(msg, 0);
                const msg_copy = try alloc.dupe(u8, zig_msg);
                c.sqlite3_free(msg);
                return msg_copy;
            }
        }
        return null;
    }

    /// Returns the Row ID of the most recent successful INSERT into the database from the database connection in the first argument.
    /// If no successful INSERTs have ever occurred on that database connection, zero is returned.
    pub fn lastInsertRowId(db: Sqlite) i64 {
        return c.sqlite3_last_insert_rowid(db.sqlite_ref);
    }
};

/// A wrapper around a prepared SQLite statement.
pub const Statement = struct {
    sqlite_ref: *c.sqlite3,
    stmt_ref: *c.sqlite3_stmt,

    /// Destroys a prepared statement object.
    pub fn finalize(stmt: Statement) void {
        _ = c.sqlite3_finalize(stmt.stmt_ref);
    }

    /// Resets a prepared statement object.
    pub fn reset(stmt: Statement) void {
        _ = c.sqlite3_reset(stmt.stmt_ref);
    }

    /// Executes the next step of a prepared statement.
    pub fn step(stmt: Statement) ?Result {
        const result = c.sqlite3_step(stmt.stmt_ref);
        if (result == c.SQLITE_ROW) {
            return Result{ .row = .{ .stmt = stmt } };
        } else if (result == c.SQLITE_DONE) {
            return Result{ .done = void{} };
        } else if (result == c.SQLITE_BUSY) {
            return Result{ .busy = void{} };
        } else {
            return null;
        }
    }

    /// Advances to the next row in the result set.
    pub fn nextRow(stmt: Statement) ?Result.Row {
        if (stmt.step()) |result| {
            if (result == .row) return result.row;
        }
        return null;
    }

    /// Binds values to the parameters of a prepared statement.
    ///
    /// The `values` parameter must be a struct with fields that correspond to the parameters in the SQL statement.
    /// The fields will be bound to the parameters in the order they are declared in the struct.
    pub fn bindValues(stmt: Statement, values: anytype) void {
        inline for (std.meta.fields(@TypeOf(values)), 1..) |field, index| {
            stmt.bindValue(index, @field(values, field.name));
        }
    }

    /// Binds a value to a parameter of a prepared statement.
    ///
    /// The `index` parameter specifies the index of the parameter to bind to (starting from 1).
    /// The `value` parameter specifies the value to bind.
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

    /// Returns the SQL text of a prepared statement with bound parameters expanded.
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

/// Represents the result of executing a prepared statement.
pub const Result = union(enum) {
    /// A row of data was returned.
    row: Row,
    /// The database is busy.
    busy: void,
    /// The statement has been executed successfully.
    done: void,

    /// Represents a row of data returned by a query.
    pub const Row = struct {
        stmt: Statement,

        /// Returns the value of the column at the given index as an integer.
        pub fn int(row: Row, index: usize) i64 {
            return @intCast(c.sqlite3_column_int64(row.stmt.stmt_ref, @intCast(index)));
        }

        /// Returns the value of the column at the given index as a nullable integer.
        pub fn nullableInt(row: Row, index: usize) ?i64 {
            if (c.sqlite3_column_type(row.stmt.stmt_ref, @intCast(index)) != c.SQLITE_NULL) {
                return row.int(index);
            } else {
                return null;
            }
        }

        /// Returns the value of the column at the given index as a float.
        pub fn float(row: Row, index: usize) f64 {
            return @floatCast(c.sqlite3_column_double(row.stmt.stmt_ref, @intCast(index)));
        }

        /// Returns the value of the column at the given index as a nullable float.
        pub fn nullableFloat(row: Row, index: usize) ?f64 {
            if (c.sqlite3_column_type(row.stmt.stmt_ref, @intCast(index)) != c.SQLITE_NULL) {
                return row.float(index);
            } else {
                return null;
            }
        }

        /// Returns the value of the column at the given index as a text string.
        pub fn text(row: Row, index: usize) []const u8 {
            const bytes = c.sqlite3_column_bytes(row.stmt.stmt_ref, @intCast(index));
            if (bytes == 0) {
                return "";
            }

            const data = c.sqlite3_column_text(row.stmt.stmt_ref, @intCast(index));
            return data[0..@intCast(bytes)];
        }

        /// Returns the value of the column at the given index as a nullable text string.
        pub fn nullableText(row: Row, index: usize) ?[]const u8 {
            if (c.sqlite3_column_type(row.stmt.stmt_ref, @intCast(index)) != c.SQLITE_NULL) {
                return row.text(index);
            } else {
                return null;
            }
        }

        /// Returns the value of the column at the given index as a blob.
        pub fn blob(row: Row, index: usize) Blob {
            const bytes = c.sqlite3_column_bytes(row.stmt.stmt_ref, @intCast(index));
            if (bytes == 0) {
                return Blob{ .content = &.{} };
            }

            const data: [*c]const u8 = @ptrCast(c.sqlite3_column_blob(row.stmt.stmt_ref, @intCast(index)));
            const content: []const u8 = data[0..@intCast(bytes)];
            return Blob{ .content = content };
        }

        /// Returns the value of the column at the given index as a nullable blob.
        pub fn nullableBlob(row: Row, index: usize) ?Blob {
            if (c.sqlite3_column_type(row.stmt.stmt_ref, @intCast(index)) != c.SQLITE_NULL) {
                return row.blob(index);
            } else {
                return null;
            }
        }

        /// Reads the current row into a struct.
        ///
        /// The `Type` parameter must be a struct with fields that correspond to the columns in the result set.
        /// The fields will be populated with the values from the columns in the order they are declared in the struct.
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
                @field(result, field.name) = row.readField(field.type, index);
            }

            return result;
        }

        fn readField(row: Row, comptime FieldType: type, index: usize) FieldType {
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
                .Optional => |opt| return row.readOptionalField(opt.child, index),
                .Struct => {
                    if (FieldType == Blob) {
                        return row.blob(index);
                    } else @compileError("Cannot read a value with the type of " ++ @typeName(FieldType));
                },
                else => @compileError("Cannot read a value with the type of " ++ @typeName(FieldType)),
            }
        }

        fn readOptionalField(row: Row, comptime FieldType: type, index: usize) FieldType {
            if (c.sqlite3_column_type(row.stmt.stmt_ref, @intCast(index)) == c.SQLITE_NULL) {
                return null;
            }

            return row.readField(FieldType, index);
        }
    };
};

/// Represents a blob of data.
pub const Blob = struct {
    content: []const u8,
};

/// Contains extra results from preparing a statement.
pub const ExtraResults = struct {
    /// If an error occurs, this field will be populated with the error message.
    error_msg: ?*[]const u8 = null,
    /// If the query contains more than one statement, this field will be populated with the remaining SQL.
    leftover_query: ?*[]const u8 = null,
};

test {
    var db = try Sqlite.open(":memory:");
    defer db.close();

    // Create table query
    {
        _ = try db.exec(
            \\
            \\CREATE TABLE IF NOT EXISTS sample_table(
            \\id INTEGER PRIMARY KEY AUTOINCREMENT, 
            \\number_1 REAL NOT NULL, 
            \\number_2 INTEGER NOT NULL, 
            \\text TEXT NOT NULL, 
            \\blob BLOB NOT NULL
            \\);
            \\
        );
    }

    // Insert table query
    {
        const alloc = std.testing.allocator;
        inline for (0..10) |i| {
            const text = try std.fmt.allocPrint(alloc, "Index {d}", .{i});
            defer alloc.free(text);
            try insertTestRow(db, i, text);
        }

        const row_id = db.lastInsertRowId();
        try std.testing.expectEqual(10, row_id);
    }

    // Select query using read column with index
    {
        var select_stmt = try db.prepare(
            \\SELECT * FROM sample_table;
        ,
            null,
        );
        defer select_stmt.finalize();

        var id: usize = 1;
        var index: usize = 0;

        const alloc = std.testing.allocator;

        while (select_stmt.nextRow()) |row| : ({
            id += 1;
            index += 1;
        }) {
            const text = try std.fmt.allocPrint(alloc, "Index {d}", .{index});
            defer alloc.free(text);

            try std.testing.expectEqual(@as(i64, @intCast(id)), row.int(0));
            try std.testing.expectEqual(@as(i64, @intCast(index)), row.int(1));
            try std.testing.expectEqual(@as(f64, @floatFromInt(index)) + 0.99, row.float(2));
            try std.testing.expectEqualSlices(u8, text, row.text(3));
            try std.testing.expectEqualDeep(Blob{ .content = text }, row.blob(4));
        }
    }

    // Select query using write to structure data
    {
        var select_alt_stmt = try db.prepare(
            \\SELECT * FROM sample_table;
        ,
            null,
        );
        defer select_alt_stmt.finalize();

        var id: usize = 1;
        var index: usize = 0;

        const alloc = std.testing.allocator;

        while (select_alt_stmt.nextRow()) |row| : ({
            id += 1;
            index += 1;
        }) {
            const QueryResult = struct {
                id: i64,
                number_1: i64,
                number_2: f64,
                text: []const u8,
                blob: Blob,
            };
            const result = row.read(QueryResult);

            const text = try std.fmt.allocPrint(alloc, "Index {d}", .{index});
            defer alloc.free(text);

            try std.testing.expectEqualDeep(
                QueryResult{
                    .id = @intCast(id),
                    .number_1 = @intCast(index),
                    .number_2 = @as(f64, @floatFromInt(index)) + 0.99,
                    .text = text,
                    .blob = Blob{ .content = text },
                },
                result,
            );
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

test "execWithError API" {
    var sqlite = try Sqlite.open(":memory:");
    defer sqlite.close();

    const allocator = std.testing.allocator;

    const err_msg = try sqlite.execWithError(allocator, "RANDOM SYNTAX IT SHOULD PRINT ERROR;");
    if (err_msg) |msg| {
        defer allocator.free(msg);
        try std.testing.expectEqualStrings("near \"RANDOM\": syntax error", msg);
    }
}
