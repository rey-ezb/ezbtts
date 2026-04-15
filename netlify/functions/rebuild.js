function jsonResponse(status, payload) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function jsonErrorResponse(status, message) {
  return jsonResponse(status, { message, error: message });
}

async function triggerBuildHook() {
  const hookUrl = String(process.env.NETLIFY_BUILD_HOOK_URL || "").trim();
  if (!hookUrl) {
    throw new Error("Rebuild is not configured yet. Add NETLIFY_BUILD_HOOK_URL in Netlify.");
  }
  const response = await fetch(hookUrl, { method: "POST" });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`Netlify build hook failed: ${response.status} ${detail}`);
  }
}

export default async (request) => {
  if (request.method !== "POST") return jsonErrorResponse(405, "Method not allowed.");

  try {
    await triggerBuildHook();
    return jsonResponse(200, {
      message: "Snapshot rebuild triggered.",
      rebuildTriggered: true,
    });
  } catch (error) {
    return jsonErrorResponse(500, error instanceof Error ? error.message : "Snapshot rebuild failed.");
  }
};

export const config = {
  path: "/api/rebuild",
};
