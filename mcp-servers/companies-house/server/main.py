"""
Main entry point for the Companies House MCP server.
"""

import argparse

import uvicorn


def main():
    """Start the Companies House MCP server using uvicorn."""
    parser = argparse.ArgumentParser(description="Start the Companies House MCP server")
    parser.add_argument(
        "--port", type=int, default=8000, help="Port to run the server on (default: 8000)"
    )
    args = parser.parse_args()

    uvicorn.run(
        "server.app:combined_app",
        host="0.0.0.0",
        port=args.port,
    )


if __name__ == "__main__":
    main()
