"""LangChain agent builder with memory and streaming."""
import re

from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage, SystemMessage
from langgraph.checkpoint.memory import MemorySaver

from core import config
from tools import ALL_TOOLS
from agent.system_prompt import build_system_prompt
from agent.preprocessor import preprocess_prompt


def _dynamic_prompt(state):
    """Rebuild the system prompt on every invocation so LOADED EXCEL TABLES is always current."""
    return [SystemMessage(content=build_system_prompt())] + state["messages"]

_THREAD_ID = "default"

_CONVERSATIONAL = re.compile(
    r"^\s*(hi+|hello|hey|howdy|thanks?|thank you|ok(ay)?|cool|great|"
    r"good\s+(morning|afternoon|evening)|bye|goodbye|"
    r"who are you|what can you do|what do you do|help)\W*$",
    re.IGNORECASE,
)


def build_agent():
    llm = ChatGoogleGenerativeAI(
        model=config.AGENT_MODEL,
        temperature=0,
    )
    return create_react_agent(
        model=llm,
        tools=ALL_TOOLS,
        prompt=_dynamic_prompt,
        checkpointer=MemorySaver(),
    )


def _llm() -> ChatGoogleGenerativeAI:
    return ChatGoogleGenerativeAI(model=config.AGENT_MODEL, temperature=0)


def run_agent(agent, raw_prompt: str) -> dict:
    processed = preprocess_prompt(raw_prompt)

    if _CONVERSATIONAL.match(processed.strip()):
        response = _llm().invoke([
            SystemMessage(content=build_system_prompt()),
            HumanMessage(content=processed),
        ])
        content = response.content
        if isinstance(content, list):
            content = " ".join(b["text"] for b in content if isinstance(b, dict) and b.get("type") == "text")
        return {"output": content, "intermediate_steps": []}

    result = agent.invoke(
        {"messages": [HumanMessage(content=processed)]},
        config={"configurable": {"thread_id": _THREAD_ID}},
    )

    messages = result.get("messages", [])

    output = ""
    for msg in reversed(messages):
        if isinstance(msg, AIMessage) and msg.content:
            content = msg.content
            if isinstance(content, list):
                output = " ".join(
                    b["text"] for b in content
                    if isinstance(b, dict) and b.get("type") == "text"
                )
            else:
                output = content
            break

    # Only collect tool calls from the current turn (after the last HumanMessage).
    # Iterating all messages would include tool calls from previous turns.
    last_human = max(
        (i for i, m in enumerate(messages) if isinstance(m, HumanMessage)),
        default=0,
    )
    current_turn = messages[last_human + 1:]

    intermediate_steps = []
    tool_call_names: dict[str, str] = {}
    for msg in current_turn:
        if isinstance(msg, AIMessage) and msg.tool_calls:
            for tc in msg.tool_calls:
                tool_call_names[tc["id"]] = tc["name"]
        elif isinstance(msg, ToolMessage):
            tool_name = tool_call_names.get(msg.tool_call_id, "unknown")
            action = type("_Action", (), {"tool": tool_name})()
            intermediate_steps.append((action, msg.content))

    return {"output": output, "intermediate_steps": intermediate_steps}
