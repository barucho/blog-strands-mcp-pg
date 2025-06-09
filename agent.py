from strands import Agent
from strands.models import BedrockModel
# for using MCP 
from strands.tools.mcp import MCPClient
from mcp.client.streamable_http import streamablehttp_client
import logging
def main():
    # Create a BedrockModel
    bedrock_model = BedrockModel(
    model_id="us.anthropic.claude-3-7-sonnet-20250219-v1:0",
    region_name='us-east-1',
    temperature=0.3,)

    # we will use simple sse connection option 
    # Connect to an MCP server using SSE transport
    try:
        streamable_http_mcp_client = MCPClient(lambda: streamablehttp_client("http://localhost:8000/mcp"))
    except Exception as e:
        logging.error(f"Failed to connect to MCP server: {e}")
        raise

    system_prompt="""
    You are database admin agent your task is to help developers 
    with postgresql performance issues you have MCP tools 
    to connect to the database for performance and schema information
    when  
    """
    with streamable_http_mcp_client:
        tools = streamable_http_mcp_client.list_tools_sync()    
        # create the agent 
        agent = Agent(system_prompt=system_prompt,model=bedrock_model,tools=tools)
        print(agent.model.config)
        message = """
        1. find me the top query ?
        2. what are the tables that have the queries in the top queries by cpu and by time ? 
        3. can you find me how to improve this query ? 
        """
        agent(message)

if __name__ == "__main__":
    main()