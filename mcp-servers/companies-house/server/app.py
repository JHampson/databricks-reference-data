"""
FastAPI application configuration for the Companies House MCP server.

This module sets up the FastMCP server with tools for accessing
UK company registration data via the Companies House API.
"""

from fastapi import FastAPI
from fastmcp import FastMCP

from .tools import load_tools

# Create the FastMCP server
mcp_server = FastMCP(name="companies-house-mcp-server")

# Load and register all tools
load_tools(mcp_server)

# Convert to HTTP application
mcp_app = mcp_server.http_app()

# Create FastAPI instance for additional endpoints
app = FastAPI(
    title="Companies House MCP Server",
    description="MCP Server for UK company registration data",
    version="0.1.0",
    lifespan=mcp_app.lifespan,
)


@app.get("/", include_in_schema=False)
async def root():
    """Root endpoint with server info."""
    return {
        "name": "Companies House MCP Server",
        "status": "running",
        "endpoints": {
            "mcp": "/mcp",
            "docs": "/docs",
        },
    }


# Combined application with MCP and custom routes
combined_app = FastAPI(
    title="Companies House MCP Server",
    routes=[
        *mcp_app.routes,
        *app.routes,
    ],
    lifespan=mcp_app.lifespan,
)
