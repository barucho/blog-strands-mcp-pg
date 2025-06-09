from typing import Any, List, Optional
from mcp.server import FastMCP
import psycopg 
from psycopg.rows import Row
from contextlib import contextmanager
import argparse


# Initialize FastMCP server
mcp = FastMCP("postgresqlperf")

def get_pg_uri_from_args() -> str:
    """
    Get PostgreSQL connection URI from command line arguments
    Returns:
        str: PostgreSQL connection URI
    """
    parser = argparse.ArgumentParser(description='PostgreSQL connection settings')
    parser.add_argument('--pg-uri', type=str, required=True,
                      help='PostgreSQL connection URI (postgresql://user:password@host:port)')
    args = parser.parse_args()
    return args.pg_uri


class DatabaseError(Exception):
    """Custom exception for database-related errors."""
    pass

@contextmanager
def get_db_connection(pg_uri: str) -> psycopg.Connection:
    """
    Create and manage a PostgreSQL database connection.
    Args:
        pg_uri (str): PostgreSQL connection URI        
    Yields:
        psycopg.Connection: Database connection object
    Raises:
        DatabaseError: If connection cannot be established
    """
    try:
        conn_dict = psycopg.conninfo.conninfo_to_dict(pg_uri)
        connection = psycopg.connect(**conn_dict, autocommit=True)
        yield connection
    except Exception as e:
        raise DatabaseError(f"Failed to connect to database: {str(e)}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()

def execute_query(connection: psycopg.Connection, query: str) -> List[Row]:
    """
    Execute a SQL query and return the results.
    Args:
        connection (psycopg.Connection): Active database connection
        query (str): SQL query to execute
    Returns:
        List[Row]: Query results
    Raises:
        DatabaseError: If query execution fails
    """
    try:
        with connection.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()
    except Exception as e:
        raise DatabaseError(f"Failed to execute query: {str(e)}")



@mcp.tool(description="Get all table names in the specified database")
async def get_table_names(schema_name='public') -> List[str]:
    """
    Get all table names in the specified database 
    the default scheme will be public to change the scheme name use schema_name Arg.
    Args:
    schema_name can be used for different schema name 
    Returns:
        List[str]: List of table names
    """
    try:
        with get_db_connection(pg_uri) as conn:
            results = execute_query(conn, f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'")
            return [row[0] for row in results]
    except DatabaseError as e:
        print(f"Database operation failed: {e}")
        return []

@mcp.tool()
async def get_table_definition(table: str) -> List[Row]:
    """
    Get the definition of a specified table in the database.
    Args:
        table (str): Table name
    Returns:
        List[Row]: Table schema information
    """
    try:
        with get_db_connection(pg_uri) as conn:
            results = execute_query(conn, f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")
            return results
    except DatabaseError as e:
        print(f"Database operation failed: {e}")
        return []

@mcp.tool()
async def get_schemas_names_for_current_db() -> List[str]:
    """
    Get the names of all schemas in the specified database (current user connection).
    Args:
        no args needed the schemas are for the current user database 
    Returns:
        List[str]: List of schema names
    """
    try:
        with get_db_connection(pg_uri) as conn:
            results = execute_query(conn, "SELECT schema_name FROM information_schema.schemata;")
            return [row[0] for row in results]
    except DatabaseError as e:
        print(f"Database operation failed: {e}")
        return []

@mcp.tool(description="Get all database names in the specified server")
async def get_list_of_databases() -> List[str]:
    """
    Get the names of all database in the specified server
    Args:
        no args needed 
    Returns:
        List[str]: List of schema names
    """
    try:
        with get_db_connection(pg_uri) as conn:
            results = execute_query(conn, "SELECT datname FROM pg_database;")
            return [row[0] for row in results]
    except DatabaseError as e:
        print(f"Database operation failed: {e}")
        return []


@mcp.tool()
async def get_tables_size(schema: str)-> List[Row]:
    """
    Get the size of all tables in the specified database.
    Args:
        schema (str): schema name
    Returns:
        List[Row]: Table sizes
    """
    try:
        with get_db_connection(pg_uri) as conn:
            results = execute_query(conn, f"SELECT table_name, pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS size FROM information_schema.tables WHERE table_schema = '{schema}'")
            return results
    except DatabaseError as e:
        print(f"Database operation failed: {e}")
        return []

@mcp.tool()
async def list_running_queries() -> List[Row]:
    """
    Get all running queries running on the postgresql.
    Returns:
        List[Row]: Top ten running queries
    """
    try:
        with get_db_connection(pg_uri) as conn:
            results = execute_query(conn, "SELECT pid, query, state, query_start FROM pg_stat_activity WHERE query_start IS NOT NULL ORDER BY query_start DESC;")
            return results
    except DatabaseError as e:
        print(f"Database operation failed: {e}")
        return []

@mcp.tool()
async def list_top_running_queries_by_running_time() -> List[Row]:
    """
    Get top 10 running queries running on the postgresql by running time.
    Returns:
        List[Row]: Top ten running queries by running time
    """
    try:
        with get_db_connection(pg_uri) as conn:
            results = execute_query(conn, "SELECT query,calls,total_exec_time,rows FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 10;")
            return results
    except DatabaseError as e:
        print(f"Database operation failed: {e}")
        return []

@mcp.tool()
async def list_top_running_queries_by_cpu() -> List[Row]:
    """
    Get top 10 running queries running on the postgresql by cpu.
    Returns:
        List[Row]: Top ten running queries by cpu
    """
    try:
        with get_db_connection(pg_uri) as conn:
            results = execute_query(conn, """SELECT
             pss.userid,
             pss.dbid,
             pd.datname as db_name,
             round((pss.total_exec_time + pss.total_plan_time)::numeric, 2) as total_time, 
             pss.calls, 
             round((pss.mean_exec_time+pss.mean_plan_time)::numeric, 2) as mean, 
             round((100 * (pss.total_exec_time + pss.total_plan_time) / sum((pss.total_exec_time + pss.total_plan_time)::numeric) OVER ())::numeric, 2) as cpu_portion_pctg,
             substr(pss.query, 1, 200) short_query
            FROM pg_stat_statements pss, pg_database pd 
            WHERE pd.oid=pss.dbid
            ORDER BY (pss.total_exec_time + pss.total_plan_time)
            DESC LIMIT 30;""")
            return results
    except DatabaseError as e:
        print(f"Database operation failed: {e}")
        return []


if __name__ == "__main__":
    # Initialize and run the server
    pg_uri = get_pg_uri_from_args()
    print("Starting postgresql mcp server...")
    mcp.run(transport='streamable-http')



    ## test
    try:
        with get_db_connection(pg_uri) as conn:
            results = execute_query(conn, "SELECT 'db is up'")
            for row in results:
                print(row)
    except DatabaseError as e:
        print(f"Database operation failed: {e}")

    