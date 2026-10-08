#!/usr/bin/env python3
"""Validate and migrate a quiescent, checkpointed fork database to a separate copy.

Only the destination copy is opened with the new writer. The two libraries run
in separate processes to avoid mixing DuckDB versions in one address space.
"""
import argparse
import ctypes as ct
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


class Result(ct.Structure):
    _fields_ = [("columns_count", ct.c_uint64), ("rows_count", ct.c_uint64),
                ("rows_changed", ct.c_uint64), ("columns", ct.c_void_p),
                ("error", ct.c_void_p), ("internal", ct.c_void_p)]


class Database:
    def __init__(self, library, database, readonly=True):
        self.lib = ct.CDLL(str(Path(library).resolve()))
        ptr, integer = ct.c_void_p, ct.c_uint64
        specs = {
            "create_config": ([ct.POINTER(ptr)], ct.c_int),
            "set_config": ([ptr, ct.c_char_p, ct.c_char_p], ct.c_int),
            "destroy_config": ([ct.POINTER(ptr)], None),
            "open_ext": ([ct.c_char_p, ct.POINTER(ptr), ptr, ct.POINTER(ptr)], ct.c_int),
            "connect": ([ptr, ct.POINTER(ptr)], ct.c_int),
            "query": ([ptr, ct.c_char_p, ct.POINTER(Result)], ct.c_int),
            "result_error": ([ct.POINTER(Result)], ct.c_char_p),
            "row_count": ([ct.POINTER(Result)], integer),
            "column_count": ([ct.POINTER(Result)], integer),
            "value_is_null": ([ct.POINTER(Result), integer, integer], ct.c_bool),
            "value_varchar": ([ct.POINTER(Result), integer, integer], ptr),
            "free": ([ptr], None), "destroy_result": ([ct.POINTER(Result)], None),
            "get_table_version": ([ptr, ct.c_char_p, ct.c_char_p, ct.POINTER(ptr)], integer),
            "get_column_version": ([ptr, ct.c_char_p, ct.c_char_p, ct.c_char_p, ct.POINTER(ptr)], integer),
            "disconnect": ([ct.POINTER(ptr)], None), "close": ([ct.POINTER(ptr)], None),
        }
        for name, (args, result) in specs.items():
            function = getattr(self.lib, "duckdb_" + name)
            function.argtypes, function.restype = args, result
        config, error = ptr(), ptr()
        self.db, self.con = ptr(), ptr()
        if self.lib.duckdb_create_config(ct.byref(config)):
            raise RuntimeError("Cannot create database configuration")
        try:
            if readonly and self.lib.duckdb_set_config(config, b"access_mode", b"READ_ONLY"):
                raise RuntimeError("Cannot enable read-only access")
            path = str(Path(database).resolve()).encode() if database else None
            if self.lib.duckdb_open_ext(path, ct.byref(self.db), config, ct.byref(error)):
                message = ct.string_at(error).decode() if error.value else "Cannot open database"
                if error.value:
                    self.lib.duckdb_free(error)
                raise RuntimeError(message)
            if self.lib.duckdb_connect(self.db, ct.byref(self.con)):
                self.lib.duckdb_close(ct.byref(self.db))
                raise RuntimeError("Cannot connect to database")
        finally:
            self.lib.duckdb_destroy_config(ct.byref(config))

    def close(self):
        self.lib.duckdb_disconnect(ct.byref(self.con))
        self.lib.duckdb_close(ct.byref(self.db))

    def query(self, sql):
        result = Result()
        try:
            if self.lib.duckdb_query(self.con, sql.encode(), ct.byref(result)):
                raise RuntimeError(self.lib.duckdb_result_error(ct.byref(result)).decode())
            rows = []
            for row in range(self.lib.duckdb_row_count(ct.byref(result))):
                values = []
                for column in range(self.lib.duckdb_column_count(ct.byref(result))):
                    if self.lib.duckdb_value_is_null(ct.byref(result), column, row):
                        values.append(None)
                        continue
                    value = self.lib.duckdb_value_varchar(ct.byref(result), column, row)
                    try:
                        if not value:
                            raise RuntimeError("C API could not convert a non-null result value to text")
                        values.append(ct.string_at(value).decode())
                    finally:
                        self.lib.duckdb_free(value)
                rows.append(values)
            return rows
        finally:
            self.lib.duckdb_destroy_result(ct.byref(result))

    def version(self, function, *args):
        error = ct.c_void_p()
        value = getattr(self.lib, "duckdb_" + function)(
            self.con, *(arg.encode() for arg in args), ct.byref(error))
        if error.value:
            try:
                raise RuntimeError(ct.string_at(error).decode())
            finally:
                self.lib.duckdb_free(error)
        return value


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def snapshot(library, database):
    db = Database(library, database)
    try:
        result = {"tables": []}
        # Verify persistent catalog definitions as well as table contents and counters.
        catalog_queries = {
            "schemas": "SELECT schema_name FROM duckdb_schemas() WHERE NOT internal ORDER BY 1",
            "table_definitions": "SELECT schema_name,table_name,sql FROM duckdb_tables() WHERE NOT internal ORDER BY 1,2",
            "indexes": "SELECT schema_name,table_name,index_name,sql FROM duckdb_indexes() ORDER BY 1,2,3",
            "views": "SELECT schema_name,view_name,sql FROM duckdb_views() WHERE NOT internal ORDER BY 1,2",
            "sequences": "SELECT schema_name,sequence_name,start_value,min_value,max_value,increment_by,cycle,last_value "
                         "FROM duckdb_sequences() ORDER BY 1,2",
        }
        result["catalog"] = {name: db.query(sql) for name, sql in catalog_queries.items()}
        tables = db.query("SELECT schema_name,table_name FROM duckdb_tables() WHERE NOT internal ORDER BY 1,2")
        columns = db.query("SELECT schema_name,table_name,column_name,data_type FROM duckdb_columns() "
                           "WHERE NOT internal ORDER BY 1,2,column_index")
        for schema, table in tables:
            table_columns = [(c, t) for s, n, c, t in columns if (s, n) == (schema, table)]
            projection = ",".join("hex(CAST(" + identifier(c) + " AS VARCHAR))" for c, _ in table_columns)
            rows = db.query("SELECT " + projection + " FROM " + identifier(schema) + "." + identifier(table))
            values = sorted(json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in rows)
            digest = hashlib.sha256("\n".join(values).encode()).hexdigest()
            result["tables"].append({
                "schema": schema, "name": table, "rows": len(rows), "digest": digest,
                "version": db.version("get_table_version", schema, table),
                "columns": [{"name": c, "type": t,
                             "version": db.version("get_column_version", schema, table, c)}
                            for c, t in table_columns],
            })
        return result
    finally:
        db.close()


def checksum(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def worker(operation, library, database, output=None):
    command = [sys.executable, str(Path(__file__).resolve()), operation,
               "--library", str(Path(library).resolve()), "--database", str(database)]
    if output:
        command += ["--output", str(output)]
    subprocess.run(command, check=True)


def migrate(args):
    source, destination = Path(args.source).resolve(), Path(args.destination).resolve()
    if not args.source_is_quiescent:
        raise RuntimeError("Stop database writers and pass --source-is-quiescent. Online copying is unsupported.")
    if not source.is_file() or source == destination or destination.exists():
        raise RuntimeError("Source must exist, and destination must be a different, unused path")
    manifest = destination.with_name(destination.name + ".migration.json")
    if manifest.exists():
        raise RuntimeError("Migration manifest already exists")
    for suffix in (".wal", ".wal.checkpoint", ".wal.recovery"):
        path = Path(str(source) + suffix)
        if path.exists() and path.stat().st_size:
            raise RuntimeError("Checkpoint/recover with the legacy fork first: " + str(path))
    original_hash = checksum(source)
    with tempfile.TemporaryDirectory(prefix="anybase-migration-", dir=destination.parent) as directory:
        directory = Path(directory)
        candidate = directory / "candidate.db"
        shutil.copy2(source, candidate)
        if checksum(candidate) != original_hash or checksum(source) != original_hash:
            raise RuntimeError("Source changed during copying")
        before_path, read_path, after_path = [directory / name for name in ("before.json", "read.json", "after.json")]
        worker("inspect", args.legacy_library, candidate, before_path)
        before = json.loads(before_path.read_text())
        if any("VARIANT" in column["type"] for table in before["tables"] for column in table["columns"]):
            raise RuntimeError("Legacy VARIANT requires a separately validated converter; no destination was published")
        worker("inspect", args.new_library, candidate, read_path)
        if before != json.loads(read_path.read_text()):
            raise RuntimeError("New reader did not preserve legacy data/schema/version counters")
        worker("checkpoint", args.new_library, candidate)
        worker("inspect", args.new_library, candidate, after_path)
        if before != json.loads(after_path.read_text()):
            raise RuntimeError("Checkpoint did not preserve data/schema/version counters")
        if checksum(source) != original_hash:
            raise RuntimeError("Source changed during migration")
        for suffix in (".wal", ".wal.checkpoint", ".wal.recovery"):
            path = Path(str(source) + suffix)
            if path.exists() and path.stat().st_size:
                raise RuntimeError("Source acquired a WAL during migration")
        report = {"source": str(source), "destination": str(destination), "source_sha256": original_hash,
                  "destination_sha256": checksum(candidate), "table_version_field": 60000,
                  "column_version_field": 60001, "verification": before}
        with candidate.open("rb") as file:
            os.fsync(file.fileno())
        # Hard-link publication refuses an existing destination, including one created during validation.
        os.link(candidate, destination)
        with manifest.open("x") as output:
            json.dump(report, output, indent=2)
            output.write("\n")
    print("Validated migrated copy: " + str(destination))
    print("Verification manifest: " + str(manifest))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="operation", required=True)
    for operation in ("inspect", "checkpoint"):
        sub = commands.add_parser(operation)
        sub.add_argument("--library", required=True)
        sub.add_argument("--database", required=True)
        if operation == "inspect":
            sub.add_argument("--output", required=True)
    sub = commands.add_parser("migrate")
    for argument in ("source", "destination", "legacy-library", "new-library"):
        sub.add_argument("--" + argument, required=True)
    sub.add_argument("--source-is-quiescent", action="store_true")
    args = parser.parse_args()
    if args.operation == "inspect":
        Path(args.output).write_text(json.dumps(snapshot(args.library, args.database), indent=2) + "\n")
    elif args.operation == "checkpoint":
        db = Database(args.library, None, readonly=False)
        try:
            db.query("ATTACH " + literal(Path(args.database).resolve()) + " AS migrated (STORAGE_VERSION 'v1.5.0')")
            db.query("PRAGMA force_checkpoint")
            db.query("FORCE CHECKPOINT migrated")
            db.query("DETACH migrated")
        finally:
            db.close()
    else:
        migrate(args)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
