# Skill Contract Rules

## Every skill must

- Inherit `BaseSkill` from `base.py`
- Declare `name`, `description`, `InputModel`, `OutputModel` as `ClassVar`
- Define `InputModel` and `OutputModel` as Pydantic `BaseModel` subclasses
- Implement `async def execute(self, input: InputModel) -> OutputModel`
- Use `await asyncio.to_thread(self._run, input)` for synchronous blocking work

## Primitives vs Domain

**Primitives** (`skills/primitives/`) are called by other skills via `self.kernel.invoke()`.
Set `domain=False` when registering: `kernel.register(SQLSkill(kernel), domain=False)`.
They are never dispatch targets for the LLM.

**Domain skills** (`skills/domain/`) are LLM dispatch targets.
Their `InputModel` docstring becomes the LLM tool description — write it from the
user's perspective, not the engineer's.
Their `InputModel` field descriptions are what the LLM reads to extract parameters.

## Pydantic + LLM tool calling

The `InputModel` IS the LLM tool schema. This means:
- Every field needs a `Field(description=...)` — the LLM reads these to extract values
- The class docstring of `InputModel` is the tool description shown to the LLM
- Optional parameters must have sensible defaults so the LLM does not have to provide them

## Skill-to-skill calls

Always call other skills through the kernel, never import and instantiate directly:

```python
# correct
result = await self.kernel.invoke("SQLSkill", SQLFetchInput(...))

# wrong — bypasses logging, error handling, and the skill contract
sql = SQLSkill(self.kernel)._run(input)
```

## Never

- Import a domain skill from another domain skill (use primitives only)
- Store request-scoped state on `self` (skills are shared across requests)
- Put business logic in `kernel.py` or `base.py`
