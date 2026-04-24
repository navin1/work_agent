"""Shared LLM factory — single source for the configured generative model."""
from core import config


def get_llm():
    from langchain_google_genai import ChatGoogleGenerativeAI
    return ChatGoogleGenerativeAI(model=config.AGENT_MODEL, temperature=0)
