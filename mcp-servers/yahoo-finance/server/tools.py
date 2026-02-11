"""
Tools module for the Yahoo Finance MCP server.

This module defines the MCP tools for accessing stock market data
using the yfinance library. No API key is required.
"""

import yfinance as yf


def load_tools(mcp_server):
    """
    Register all Yahoo Finance MCP tools with the server.

    Args:
        mcp_server: The FastMCP server instance to register tools with.
    """

    @mcp_server.tool
    def health() -> dict:
        """
        Check the health of the Yahoo Finance MCP server.

        Returns:
            dict: Health status information.
        """
        return {
            "status": "healthy",
            "message": "Yahoo Finance MCP Server is running.",
            "note": "No API key required - uses yfinance library.",
        }

    @mcp_server.tool
    def get_stock_info(symbol: str) -> dict:
        """
        Get comprehensive stock information from Yahoo Finance.

        Args:
            symbol: Stock ticker symbol (e.g., "AAPL", "MSFT", "GOOGL").

        Returns:
            dict: Company details including market cap, P/E ratio, sector,
                  industry, and current price information.

        Example:
            get_stock_info("AAPL")
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            if not info or info.get("regularMarketPrice") is None:
                return {"error": f"No data found for symbol: {symbol}"}

            return info
        except Exception as e:
            return {"error": "Request failed", "message": str(e)}

    @mcp_server.tool
    def get_stock_history(
        symbol: str,
        period: str,
        interval: str,
    ) -> dict:
        """
        Get historical OHLCV (Open, High, Low, Close, Volume) data for a stock.

        Args:
            symbol: Stock ticker symbol (e.g., "AAPL", "MSFT").
            period: Data period - valid values: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max.
            interval: Data interval - valid values: 1m, 2m, 5m, 15m, 30m, 60m, 90m,
                1h, 1d, 5d, 1wk, 1mo, 3mo.

        Returns:
            dict: Historical price data with dates and OHLCV values.

        Example:
            get_stock_history("MSFT", "1mo", "1d")
        """
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period, interval=interval)

            if hist.empty:
                return {"error": f"No historical data found for symbol: {symbol}"}

            # Convert to JSON-serializable format
            hist = hist.reset_index()
            hist["Date"] = hist["Date"].astype(str)
            records = hist.to_dict(orient="records")

            return {
                "symbol": symbol,
                "period": period,
                "interval": interval,
                "data": records,
            }
        except Exception as e:
            return {"error": "Request failed", "message": str(e)}

    @mcp_server.tool
    def get_financials(symbol: str, statement_type: str) -> dict:
        """
        Get financial statements for a company.

        Args:
            symbol: Stock ticker symbol (e.g., "AAPL", "GOOGL").
            statement_type: Type of statement - valid values: "income", "balance", "cashflow".

        Returns:
            dict: Financial statement data with line items and values.

        Example:
            get_financials("GOOGL", "income")
        """
        try:
            ticker = yf.Ticker(symbol)

            if statement_type == "income":
                df = ticker.financials
            elif statement_type == "balance":
                df = ticker.balance_sheet
            elif statement_type == "cashflow":
                df = ticker.cashflow
            else:
                return {
                    "error": f"Invalid statement_type: {statement_type}. "
                    "Use 'income', 'balance', or 'cashflow'"
                }

            if df is None or df.empty:
                return {"error": f"No financial data found for symbol: {symbol}"}

            # Convert to JSON-serializable format
            df.columns = df.columns.astype(str)
            result = df.to_dict()

            return {
                "symbol": symbol,
                "statement_type": statement_type,
                "data": result,
            }
        except Exception as e:
            return {"error": "Request failed", "message": str(e)}

    @mcp_server.tool
    def get_recommendations(symbol: str) -> dict:
        """
        Get analyst recommendations and ratings for a stock.

        Args:
            symbol: Stock ticker symbol (e.g., "AAPL", "NVDA").

        Returns:
            dict: Analyst recommendations with firms, ratings, and dates.

        Example:
            get_recommendations("NVDA")
        """
        try:
            ticker = yf.Ticker(symbol)
            recommendations = ticker.recommendations

            if recommendations is None or recommendations.empty:
                return {"error": f"No recommendations found for symbol: {symbol}"}

            # Convert to JSON-serializable format
            recommendations = recommendations.reset_index()
            if "Date" in recommendations.columns:
                recommendations["Date"] = recommendations["Date"].astype(str)

            records = recommendations.to_dict(orient="records")

            return {
                "symbol": symbol,
                "recommendations": records,
            }
        except Exception as e:
            return {"error": "Request failed", "message": str(e)}

    @mcp_server.tool
    def get_dividends(symbol: str) -> dict:
        """
        Get dividend payment history for a stock.

        Args:
            symbol: Stock ticker symbol (e.g., "JNJ", "KO").

        Returns:
            dict: Dividend payment history with dates and amounts.

        Example:
            get_dividends("JNJ")
        """
        try:
            ticker = yf.Ticker(symbol)
            dividends = ticker.dividends

            if dividends is None or dividends.empty:
                return {"error": f"No dividend data found for symbol: {symbol}"}

            # Convert to list of records
            records = [
                {"date": str(date), "dividend": float(value)} for date, value in dividends.items()
            ]

            return {
                "symbol": symbol,
                "dividends": records,
            }
        except Exception as e:
            return {"error": "Request failed", "message": str(e)}
