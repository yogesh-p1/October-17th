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
        if request.tool_name == "pg_create_record":
            result = await mcp_server._create_record(
                request.arguments["table"],
                request.arguments["data"]
            )

        elif request.tool_name == "pg_read_records":
            result = await mcp_server._read_records(
                request.arguments["table"],
                request.arguments.get("conditions"),
                request.arguments.get("limit")
            )

        elif request.tool_name == "pg_update_records":
            result = await mcp_server._update_records(
                request.arguments["table"],
                request.arguments["data"],
                request.arguments["conditions"]
            )

        elif request.tool_name == "pg_delete_records":
            result = await mcp_server._delete_records(
                request.arguments["table"],
                request.arguments["conditions"]
            )

        elif request.tool_name == "pg_list_tables":
            result = await mcp_server._list_tables()

        elif request.tool_name == "pg_describe_table":
            result = await mcp_server._describe_table(
                request.arguments["table"]
            )

        elif request.tool_name == "insert_in_postgres":
            result = await mcp_server._create_record(
                request.arguments["table"],
                request.arguments["data"]
            )

        elif request.tool_name == "insert_in_postgres":
            result = await mcp_server._read_records(
                request.arguments["table"],
                request.arguments["data"]
            )

        elif request.tool_name == "insert_in_postgres":
            result = await mcp_server._read_records(
                request.arguments["table"],
                request.arguments["data"]
            )

        else:
            raise HTTPException(status_code=400, detail="Unknown tool name")

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=0)
