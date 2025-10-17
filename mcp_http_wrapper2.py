from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

from postgres_mcp_server import PostgresMCPServer  # Import your MCP class

app = FastAPI(title="PostgreSQL MCP API Server")

# Initialize PostgreSQL MCP server
mcp_server = PostgresMCPServer(
    host="localhost",
    user="postgres",
    password="pass",      # 🔹 Update with your PostgreSQL password
    database="db_1",      # 🔹 Update with your DB name
    port=5432
)

class ToolRequest(BaseModel):
    tool_name: str
    arguments: dict

@app.post("/execute")
async def execute_tool(request: ToolRequest):
    """
    Execute a PostgreSQL MCP tool (CRUD operation)
    """
    try:
        tool = request.tool_name
        args = request.arguments

        if tool == "pg_create_record" or tool == "insert_in_postgres":
            result = await mcp_server._create_record(args["table"], args["data"])

        elif tool == "pg_read_records" or tool == "get_from_postgres":
            result = await mcp_server._read_records(args["table"], args.get("conditions"), args.get("limit"))

        elif tool == "pg_update_records":
            result = await mcp_server._update_records(args["table"], args["data"], args["conditions"])

        elif tool == "pg_delete_records":
            result = await mcp_server._delete_records(args["table"], args["conditions"])

        elif tool == "pg_list_tables":
            result = await mcp_server._list_tables()

        elif tool == "pg_describe_table":
            result = await mcp_server._describe_table(args["table"])

        elif tool == "get_base_site":
            result = await mcp_server._get_base_site(args["table"])

        else:
            raise HTTPException(status_code=400, detail=f"Unknown tool name: {tool}")

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
