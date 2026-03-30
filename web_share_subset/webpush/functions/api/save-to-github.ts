interface SavePayload {
  path?: string;
  rows?: Array<Record<string, unknown>>;
}

interface Env {
  GITHUB_TOKEN?: string;
  GITHUB_OWNER?: string;
  GITHUB_REPO?: string;
  GITHUB_BRANCH?: string;
}

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function toBase64Utf8(input: string): string {
  const bytes = new TextEncoder().encode(input);
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary);
}

function contentsUrl(owner: string, repo: string, repoPath: string): string {
  return `https://api.github.com/repos/${owner}/${repo}/contents/${encodeURIComponent(repoPath)}`;
}

async function githubGetSha(
  url: string,
  token: string,
  branch: string,
): Promise<{ sha?: string; error?: string }> {
  const getResp = await fetch(`${url}?ref=${encodeURIComponent(branch)}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
    },
  });
  if (getResp.ok) {
    const got = (await getResp.json()) as { sha?: string };
    return { sha: got.sha };
  }
  if (getResp.status === 404) {
    return {};
  }
  const txt = await getResp.text();
  return { error: txt };
}

async function githubPutFile(
  url: string,
  token: string,
  branch: string,
  repoPath: string,
  content: string,
  sha: string | undefined,
): Promise<{ ok: boolean; error?: string }> {
  const putBody = {
    message: `update ${repoPath} via web editor`,
    content: toBase64Utf8(content),
    branch,
    sha,
  };
  const putResp = await fetch(url, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "content-type": "application/json",
    },
    body: JSON.stringify(putBody),
  });
  if (!putResp.ok) {
    const txt = await putResp.text();
    return { ok: false, error: txt };
  }
  return { ok: true };
}

/** Relative to repo root: payload path is `data/...` under each `public/`. */
function repoPathsForData(dataPath: string): string[] {
  return [`web/public/${dataPath}`, `web_share_subset/webpush/public/${dataPath}`];
}

export const onRequestPost: PagesFunction<Env> = async (ctx) => {
  try {
    const token = (ctx.env.GITHUB_TOKEN || "").trim();
    const owner = (ctx.env.GITHUB_OWNER || "").trim();
    const repo = (ctx.env.GITHUB_REPO || "").trim();
    const branch = (ctx.env.GITHUB_BRANCH || "main").trim();
    if (!token || !owner || !repo) {
      return json(400, {
        ok: false,
        error: "Missing env vars: GITHUB_TOKEN, GITHUB_OWNER, GITHUB_REPO",
      });
    }

    const payload = (await ctx.request.json()) as SavePayload;
    const path = String(payload.path || "").trim();
    const rows = payload.rows || [];
    if (!path.startsWith("data/")) {
      return json(400, { ok: false, error: "Invalid path. Expected data/*" });
    }

    const content = JSON.stringify(rows, null, 2) + "\n";
    const targets = repoPathsForData(path);
    const written: string[] = [];

    for (const repoPath of targets) {
      const url = contentsUrl(owner, repo, repoPath);
      const { sha, error: readErr } = await githubGetSha(url, token, branch);
      if (readErr) {
        return json(500, {
          ok: false,
          error: `GitHub read failed for ${repoPath}: ${readErr}`,
          written,
        });
      }
      const put = await githubPutFile(url, token, branch, repoPath, content, sha);
      if (!put.ok) {
        return json(500, {
          ok: false,
          error: `GitHub write failed for ${repoPath}: ${put.error}`,
          written,
        });
      }
      written.push(repoPath);
    }

    return json(200, {
      ok: true,
      path,
      rows: rows.length,
      branch,
      repo_paths: targets,
    });
  } catch (e) {
    return json(500, { ok: false, error: String(e) });
  }
};
