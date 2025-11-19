#test_tool.py
from ibm_watsonx_orchestrate.agent_builder.tools import tool


@tool()
def my_tool(input: str) -> str:
    """Executes the tool's action based on the provided input.Test

    Args:
        input (str): The input of the tool.

    Returns:
        str: The action of the tool.
    """

    #functionality of the tool

    return f"Hello, {input}"