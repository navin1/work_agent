"""LangChain agent builder with memory and streaming."""
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
from langgraph.checkpoint.memory import MemorySaver

from core import config
from tools import ALL_TOOLS
from agent.system_prompt import build_system_prompt
from agent.preprocessor import preprocess_prompt

_THREAD_ID = "default"


def build_agent():
    llm = ChatGoogleGenerativeAI(
        model=config.AGENT_MODEL,
        temperature=0,
    )
    return create_react_agent(
        model=llm,
        tools=ALL_TOOLS,
        prompt=build_system_prompt(),
        checkpointer=MemorySaver(),
    )


def run_agent(agent, raw_prompt: str) -> dict:
    processed = preprocess_prompt(raw_prompt)
    result = agent.invoke(
        {"messages": [HumanMessage(content=processed)]},
        config={"configurable": {"thread_id": _THREAD_ID}},
    )

    messages = result.get("messages", [])

    output = ""
    for msg in reversed(messages):
        if isinstance(msg, AIMessage) and msg.content:
            output = msg.content
            break

    # Reconstruct intermediate_steps in the shape app.py expects:
    # [(action_with_.tool, tool_output_str), ...]
    intermediate_steps = []
    tool_call_names: dict[str, str] = {}
    for msg in messages:
        if isinstance(msg, AIMessage) and msg.tool_calls:
            for tc in msg.tool_calls:
                tool_call_names[tc["id"]] = tc["name"]
        elif isinstance(msg, ToolMessage):
            tool_name = tool_call_names.get(msg.tool_call_id, "unknown")
            action = type("_Action", (), {"tool": tool_name})()
            intermediate_steps.append((action, msg.content))

    return {"output": output, "intermediate_steps": intermediate_steps}
