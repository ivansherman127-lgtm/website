---
description: "Use when handling deploy/redeploy/pull/restart/release tasks in this website repo. Enforce server-first deployment workflow, avoid Cloudflare-centric guidance unless explicitly requested, and source server credentials from keys.json."
---
# Server Deployment Workflow

- This project deploys on the user's own server, not Cloudflare-first workflows.
- For deploy or redeploy requests, prioritize SSH-based server operations in the server checkout.
- Use credentials and connection details from keys.json.
- Do not ask to switch to Cloudflare deployment unless the user explicitly requests Cloudflare.
- If both paths are possible, default to the server path and state what was run on server.
- Preserve server state safely before pull/checkout:
  - inspect git status
  - stash local changes when needed
  - resolve permissions issues if git cannot write objects
  - pull the intended branch and deploy from that server checkout
- Keep secrets out of logs and responses; never echo raw credentials in chat output.
