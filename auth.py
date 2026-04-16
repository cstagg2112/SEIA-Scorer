from fastapi.responses import HTMLResponse
import secrets
import os

APP_PASSWORD = os.environ.get("APP_PASSWORD", "signaturemascore")
SESSION_TOKENS = set()

def make_token():
    return secrets.token_hex(32)

def verify_token(token: str) -> bool:
    return token in SESSION_TOKENS

def login_page(error=False):
    return HTMLResponse(f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>SEIA M&A Scorer</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f8f8f7;display:flex;align-items:center;justify-content:center;height:100vh}}
.card{{background:#fff;border:0.5px solid #e5e4e0;border-radius:12px;padding:2rem;width:360px}}
h1{{font-size:16px;font-weight:600;margin-bottom:4px}}
p{{font-size:12px;color:#6b6a66;margin-bottom:1.5rem}}
label{{font-size:11px;font-weight:500;color:#6b6a66;display:block;margin-bottom:4px}}
input{{width:100%;font-size:13px;padding:8px 10px;border:0.5px solid #d0cfc9;border-radius:8px;margin-bottom:12px;font-family:inherit}}
input:focus{{outline:none;border-color:#999}}
button{{width:100%;font-size:13px;padding:9px;background:#1a1a18;color:#fff;border:none;border-radius:8px;cursor:pointer;font-family:inherit}}
button:hover{{opacity:0.85}}
.error{{font-size:12px;color:#993C1D;background:#FAECE7;padding:8px 10px;border-radius:6px;margin-bottom:12px}}
</style>
</head>
<body>
<div class="card">
  <h1>SEIA M&A Scorer</h1>
  <p>RIA acquisition targeting</p>
  {"<div class='error'>Incorrect password — try again</div>" if error else ""}
  <form method="POST" action="/login">
    <label>Password</label>
    <input type="password" name="password" placeholder="Enter password" autofocus>
    <button type="submit">Sign in</button>
  </form>
</div>
</body>
</html>""")
