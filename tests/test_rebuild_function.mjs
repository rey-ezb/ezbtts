import test from "node:test";
import assert from "node:assert/strict";

import rebuildHandler from "../netlify/functions/rebuild.js";

test("rebuild triggers the Netlify build hook when configured", async () => {
  const originalFetch = global.fetch;
  const originalHook = process.env.NETLIFY_BUILD_HOOK_URL;

  process.env.NETLIFY_BUILD_HOOK_URL = "https://api.netlify.com/build_hooks/test-hook";

  const seenUrls = [];
  global.fetch = async (url, options) => {
    seenUrls.push({ url: String(url), method: options?.method || "GET" });
    return new Response("", { status: 200 });
  };

  try {
    const request = new Request("http://localhost/api/rebuild", { method: "POST" });
    const response = await rebuildHandler(request);
    const payload = await response.json();

    assert.equal(response.status, 200);
    assert.equal(payload.rebuildTriggered, true);
    assert.equal(seenUrls.length, 1);
    assert.deepEqual(seenUrls[0], {
      url: "https://api.netlify.com/build_hooks/test-hook",
      method: "POST",
    });
  } finally {
    global.fetch = originalFetch;
    if (originalHook === undefined) {
      delete process.env.NETLIFY_BUILD_HOOK_URL;
    } else {
      process.env.NETLIFY_BUILD_HOOK_URL = originalHook;
    }
  }
});

test("rebuild returns a clear error when the hook is not configured", async () => {
  const originalFetch = global.fetch;
  const originalHook = process.env.NETLIFY_BUILD_HOOK_URL;

  delete process.env.NETLIFY_BUILD_HOOK_URL;
  global.fetch = async () => new Response("", { status: 200 });

  try {
    const request = new Request("http://localhost/api/rebuild", { method: "POST" });
    const response = await rebuildHandler(request);
    const payload = await response.json();

    assert.equal(response.status, 500);
    assert.match(payload.message, /Rebuild is not configured yet/i);
    assert.equal(payload.error, payload.message);
  } finally {
    global.fetch = originalFetch;
    if (originalHook === undefined) {
      delete process.env.NETLIFY_BUILD_HOOK_URL;
    } else {
      process.env.NETLIFY_BUILD_HOOK_URL = originalHook;
    }
  }
});
