from __future__ import annotations

import ast


_ALLOWED_NODES = (
    ast.Expression,
    ast.BinOp,
    ast.UnaryOp,
    ast.Constant,
    ast.Name,
    ast.Load,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.Pow,
    ast.Mod,
    ast.USub,
    ast.UAdd,
    ast.FloorDiv,
    ast.Call,  # blocked by validator
)


def safe_eval(expr: str, variables: dict[str, float]) -> float:
    """
    Evaluate a numeric expression with variables, without allowing function calls or attribute access.

    Examples:
      - "2.5 * electricity_kwh"
      - "ng_kg * 0.2 + 1"
    """
    tree = ast.parse(expr, mode="eval")

    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise ValueError(f"Disallowed expression element: {type(node).__name__}")
        if isinstance(node, ast.Call):
            raise ValueError("Function calls are not allowed in expressions.")
        if isinstance(node, ast.Name) and node.id not in variables:
            raise ValueError(f"Unknown variable: {node.id}")

    code = compile(tree, "<expr>", "eval")
    val = eval(code, {"__builtins__": {}}, dict(variables))
    if not isinstance(val, (int, float)):
        raise ValueError("Expression did not evaluate to a number.")
    return float(val)
