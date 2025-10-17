"""
PostgreSQL MCP Server - Provides CRUD access to PostgreSQL tables via MCP protocol
"""
import asyncio
import json
from typing import Any, Optional
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
import asyncpg


class PostgresMCPServer:
    def __init__(self, host: str, user: str, password: str, database: str, port: int = 5432):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.server = Server("postgres-mcp-server")
        self._setup_handlers()

    async def _get_connection(self):
        """Create and return a PostgreSQL connection"""
        return await asyncpg.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port
        )

    def _setup_handlers(self):
        """Setup MCP protocol handlers"""

        @self.server.list_tools()
        async def list_tools() -> list[Tool]:
            """List available tools"""
            return [
                Tool(
                    name="pg_create_record",
                    description="Insert a new record into a PostgreSQL table (e.g. lighthouseV2)",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table": {"type": "string"},
                            "data": {"type": "object"}
                        },
                        "required": ["table", "data"]
                    }
                ),
                Tool(
                    name="pg_read_records",
                    description="Read records from a PostgreSQL table",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table": {"type": "string"},
                            "conditions": {"type": "object"},
                            "limit": {"type": "integer"}
                        },
                        "required": ["table"]
                    }
                ),
                Tool(
                    name="pg_update_records",
                    description="Update records in a PostgreSQL table",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table": {"type": "string"},
                            "data": {"type": "object"},
                            "conditions": {"type": "object"}
                        },
                        "required": ["table", "data", "conditions"]
                    }
                ),
                Tool(
                    name="pg_delete_records",
                    description="Delete records from a PostgreSQL table",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "table": {"type": "string"},
                            "conditions": {"type": "object"}
                        },
                        "required": ["table", "conditions"]
                    }
                ),
                Tool(
                    name="pg_list_tables",
                    description="List all tables in the PostgreSQL database",
                    inputSchema={"type": "object", "properties": {}}
                ),
                Tool(
                    name="pg_describe_table",
                    description="Get the structure of a PostgreSQL table",
                    inputSchema={
                        "type": "object",
                        "properties": {"table": {"type": "string"}},
                        "required": ["table"]
                    }
                ),
                Tool(
                    name="insert_in_postgres",
                    description="Insert the lighthouse retrieved fields in postgres",
                    inputSchema={
                        "type": "object",
                        "properties": {"table": {"type": "string"}, "data": {"type": "object"}},
                        "required": ["table", "data"]
                    }
                ),
                Tool(
                    name="get_from_postgres",
                    description="Get the lighthouse fields from postgres",
                    inputSchema={
                        "type": "object",
                        "properties": {"table": {"type": "string"}, "conditions": {"type": "object"}, "limit": {"type": "integer"}},
                        "required": ["table"]
                    }
                ),
                Tool(
                    name="get_base_site",
                    description="Retrieve the latest base_site from the table",
                    inputSchema={
                        "type": "object",
                        "properties": {"table": {"type": "string"}},
                        "required": ["table"]
                    }
                )
            ]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: Any) -> list[TextContent]:
            """Handle tool calls"""
            try:
                if name == "pg_create_record":
                    result = await self._create_record(arguments["table"], arguments["data"])
                elif name == "pg_read_records":
                    result = await self._read_records(
                        arguments["table"],
                        arguments.get("conditions"),
                        arguments.get("limit")
                    )
                elif name == "pg_update_records":
                    result = await self._update_records(
                        arguments["table"], arguments["data"], arguments["conditions"]
                    )
                elif name == "pg_delete_records":
                    result = await self._delete_records(arguments["table"], arguments["conditions"])
                elif name == "pg_list_tables":
                    result = await self._list_tables()
                elif name == "pg_describe_table":
                    result = await self._describe_table(arguments["table"])
                elif name == "insert_in_postgres":
                    data = arguments.get("data")
                    if not data:
                        return [TextContent(type="text", text=json.dumps({"error": "Missing 'data' argument"}))]

                    # Convert calculated_time string to datetime
                    if "calculated_time" in data and isinstance(data["calculated_time"], str):
                        from datetime import datetime
                        data["calculated_time"] = datetime.fromisoformat(data["calculated_time"])

                    result = await self._create_record(arguments["table"], data)

                elif name == "get_from_postgres":
                    result = await self._read_records(
                        arguments["table"],
                        arguments.get("conditions"),
                        arguments.get("limit")
                    )
                elif name == "get_base_site":
                    result = await self._get_base_site(arguments["table"])
                else:
                    result = {"error": f"Unknown tool: {name}"}

                return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]

    # ===================== CRUD OPERATIONS ===================== #

    async def _create_record(self, table: str, data: dict) -> dict:
        conn = await self._get_connection()
        try:
            columns = ", ".join(data.keys())
            placeholders = ", ".join(f"${i+1}" for i in range(len(data)))
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING id;"
            values = list(data.values())
            inserted_id = await conn.fetchval(query, *values)
            return {"success": True, "inserted_id": inserted_id}
        finally:
            await conn.close()

    async def _read_records(self, table: str, conditions: Optional[dict] = None, limit: Optional[int] = None) -> dict:
        conn = await self._get_connection()
        try:
            query = f"SELECT * FROM {table}"
            params = []
            if conditions:
                clauses = [f"{k} = ${i+1}" for i, k in enumerate(conditions.keys())]
                query += " WHERE " + " AND ".join(clauses)
                params = list(conditions.values())
            if limit:
                query += f" LIMIT {limit}"
            rows = await conn.fetch(query, *params)
            return {"success": True, "data": [dict(r) for r in rows]}
        finally:
            await conn.close()

    async def _update_records(self, table: str, data: dict, conditions: dict) -> dict:
        conn = await self._get_connection()
        try:
            set_clauses = [f"{k} = ${i+1}" for i, k in enumerate(data.keys())]
            where_start = len(set_clauses)
            where_clauses = [f"{k} = ${i + where_start + 1}" for i, k in enumerate(conditions.keys())]
            query = f"UPDATE {table} SET {', '.join(set_clauses)} WHERE {' AND '.join(where_clauses)}"
            values = list(data.values()) + list(conditions.values())
            result = await conn.execute(query, *values)
            return {"success": True, "message": result}
        finally:
            await conn.close()

    async def _delete_records(self, table: str, conditions: dict) -> dict:
        conn = await self._get_connection()
        try:
            where_clauses = [f"{k} = ${i+1}" for i, k in enumerate(conditions.keys())]
            query = f"DELETE FROM {table} WHERE {' AND '.join(where_clauses)}"
            result = await conn.execute(query, *conditions.values())
            return {"success": True, "message": result}
        finally:
            await conn.close()

    async def _list_tables(self) -> dict:
        conn = await self._get_connection()
        try:
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
            rows = await conn.fetch(query)
            tables = [r["table_name"] for r in rows]
            return {"success": True, "tables": tables}
        finally:
            await conn.close()

    async def _describe_table(self, table: str) -> dict:
        conn = await self._get_connection()
        try:
            query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = $1;
            """
            rows = await conn.fetch(query, table)
            return {"success": True, "table": table, "columns": [dict(r) for r in rows]}
        finally:
            await conn.close()

    # ===================== Custom MCP Tool ===================== #

    async def _get_base_site(self, table: str) -> dict:
        conn = await self._get_connection()
        try:
            query = f"""
            SELECT base_site 
            FROM {table} 
            ORDER BY calculated_time DESC 
            LIMIT 1;
            """
            row = await conn.fetchrow(query)
            if row:
                return {"success": True, "base_site": row["base_site"]}
            else:
                return {"success": True, "base_site": None}
        finally:
            await conn.close()

    # ===================== RUN SERVER ===================== #

    async def run(self):
        """Run the MCP server over stdio"""
        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                self.server.create_initialization_options()
            )


async def main():
    """Main entry point"""
    server = PostgresMCPServer(
        host="localhost",
        user="postgres",
        password="pass",
        database="db_1",
        port=5432
    )
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
