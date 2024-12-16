const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addStaticLibrary(.{
        .name = "sqlite.zig",
        .root_source_file = b.path("src/sqlite.zig"),
        .target = target,
        .optimize = optimize,
    });
    addSqliteDependencies(b, lib);

    b.installArtifact(lib);

    const lib_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/sqlite.zig"),
        .target = target,
        .optimize = optimize,
    });
    addSqliteDependencies(b, lib_unit_tests);

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    _ = b.addModule("sqlite", .{
        .root_source_file = b.path("src/sqlite.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
}

fn addSqliteDependencies(b: *std.Build, compile: *std.Build.Step.Compile) void {
    compile.linkLibC();
    compile.addIncludePath(b.path("lib"));
    compile.addCSourceFiles(.{
        .root = b.path("lib"),
        .files = &.{"sqlite3.c"},
    });
}
