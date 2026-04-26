"""
Test correct argument order for get_financial_statement.
"""
from FiinQuantX import FiinSession

USERNAME = "DNSE_FG_164@fiinquant.vn"
PASSWORD = "DNSE_FG_FiinQuant_@0@6"

client = FiinSession(username=USERNAME, password=PASSWORD).login()
fa = client.FundamentalAnalysis()

# Signature: (statement, tickers, years, type, audited=None, quarters=None, fields=None)
# type is POSITIONAL – must be arg #4

print("=== Test: quarterly balancesheet (consolidated) ===")
try:
    r = fa.get_financial_statement(
        "balancesheet",      # statement (pos 1)
        ["ACB"],             # tickers   (pos 2)
        [2023],              # years     (pos 3)
        "consolidated",      # type      (pos 4) - REQUIRED positional
        quarters=[4],
    )
    print("SUCCESS:", type(r), len(r))
    if r:
        import json
        print(json.dumps(r[0], indent=2, default=str)[:800])
except Exception as e:
    print("FAIL:", e)

print("\n=== Test: annual incomestatement (separate) ===")
try:
    r = fa.get_financial_statement(
        "incomestatement",
        ["ACB"],
        [2023],
        "separate",
    )
    print("SUCCESS:", type(r), len(r))
    if r:
        import json
        print(json.dumps(r[0], indent=2, default=str)[:800])
except Exception as e:
    print("FAIL:", e)

print("\n=== Test: get_ratios full output ===")
try:
    r = fa.get_ratios(
        tickers=["ACB"],
        years=[2023],
        quarters=[4],
        type="consolidated",
    )
    print("SUCCESS:", type(r), len(r))
    if r:
        import json
        print(json.dumps(r[0], indent=2, default=str)[:1000])
except Exception as e:
    print("FAIL:", e)
