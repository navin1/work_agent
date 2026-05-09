# Kernel Rules

## What the kernel owns

- **Registry**: maps skill name → skill instance
- **invoke()**: runs a named skill, logs duration and errors automatically
- **dispatch()**: LLM tool-calling loop — picks skill + extracts params from user message

## What the kernel does NOT own

- Business logic (lives in skills)
- Session/user state (belongs in the UI layer)
- Credentials beyond what's needed to initialise the LLM

## KernelContext

Only three things: `project_id`, `location`, `model`.
Do not add fields to `KernelContext` for passing data between skills.
Skills communicate through return values, not shared context.

## Registration pattern

```python
kernel = Kernel()
# primitives first — domain skills may depend on them
kernel.register(SQLSkill(kernel),   domain=False)
kernel.register(LLMSkill(kernel),   domain=False)
kernel.register(ExcelSkill(kernel), domain=False)
# domain skills are LLM dispatch targets
kernel.register(MappingSkill(kernel))
```

## Dispatch (LLM tool calling)

`dispatch()` converts every domain skill's `InputModel` into a `StructuredTool` and
calls the LLM with all tools at once. The LLM returns a `tool_call` naming the skill
and the populated arguments. The kernel validates the args against `InputModel` and
calls `invoke()`. No if/elif chains, no string-matching on intents.

## Logging

`invoke()` logs `skill=<name> elapsed=<seconds>` on every call.
Do not add manual logging inside skill `execute()` methods — the kernel covers it.
