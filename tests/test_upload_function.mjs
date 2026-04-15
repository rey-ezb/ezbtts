import test from "node:test";
import assert from "node:assert/strict";

import uploadHandler from "../netlify/functions/upload.js";

test("hosted upload failures include an error field for the dashboard client", async () => {
  const originalFetch = global.fetch;
  const originalEnv = {
    SUPABASE_URL: process.env.SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY,
    SUPABASE_UPLOAD_BUCKET: process.env.SUPABASE_UPLOAD_BUCKET,
  };

  process.env.SUPABASE_URL = "https://example.supabase.co";
  process.env.SUPABASE_SERVICE_ROLE_KEY = "sb_secret_test";
  process.env.SUPABASE_UPLOAD_BUCKET = "dashboard-uploads";

  let fetchCalls = 0;
  global.fetch = async () => {
    fetchCalls += 1;
    return new Response("storage exploded", { status: 500 });
  };

  try {
    const formData = new FormData();
    formData.set("upload_kind", "sales");
    formData.append("files", new File(["Order ID,Paid Time\n1,04/01/2026 01:00:00 PM\n"], "April 2026.csv", { type: "text/csv" }));

    const request = new Request("http://localhost/api/upload", {
      method: "POST",
      body: formData,
    });

    const response = await uploadHandler(request);
    const payload = await response.json();

    assert.equal(response.status, 500);
    assert.equal(fetchCalls, 1);
    assert.match(payload.message, /Supabase storage upload failed/i);
    assert.equal(payload.error, payload.message);
  } finally {
    global.fetch = originalFetch;
    for (const [key, value] of Object.entries(originalEnv)) {
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  }
});

test("hosted upload encodes spaces in Supabase storage object paths", async () => {
  const originalFetch = global.fetch;
  const originalEnv = {
    SUPABASE_URL: process.env.SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY,
    SUPABASE_UPLOAD_BUCKET: process.env.SUPABASE_UPLOAD_BUCKET,
  };

  process.env.SUPABASE_URL = "https://example.supabase.co";
  process.env.SUPABASE_SERVICE_ROLE_KEY = "sb_secret_test";
  process.env.SUPABASE_UPLOAD_BUCKET = "dashboard-uploads";

  const seenUrls = [];
  global.fetch = async (url) => {
    seenUrls.push(String(url));
    return new Response(JSON.stringify([{ id: "upload-1", original_filename: "All order 2026-04-15.csv", storage_path: "uploads/sales/20260415T143837Z__All order 2026-04-15.csv", file_size_bytes: 12 }]), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };

  try {
    const formData = new FormData();
    formData.set("upload_kind", "sales");
    formData.append("files", new File(["Order ID\n1\n"], "All order 2026-04-15.csv", { type: "text/csv" }));

    const request = new Request("http://localhost/api/upload", { method: "POST", body: formData });
    const response = await uploadHandler(request);

    assert.equal(response.status, 200);
    assert.match(seenUrls[0], /All%20order%202026-04-15\.csv$/);
  } finally {
    global.fetch = originalFetch;
    for (const [key, value] of Object.entries(originalEnv)) {
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  }
});

test("hosted upload does not trigger the build hook automatically", async () => {
  const originalFetch = global.fetch;
  const originalEnv = {
    SUPABASE_URL: process.env.SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY,
    SUPABASE_UPLOAD_BUCKET: process.env.SUPABASE_UPLOAD_BUCKET,
    NETLIFY_BUILD_HOOK_URL: process.env.NETLIFY_BUILD_HOOK_URL,
  };

  process.env.SUPABASE_URL = "https://example.supabase.co";
  process.env.SUPABASE_SERVICE_ROLE_KEY = "sb_secret_test";
  process.env.SUPABASE_UPLOAD_BUCKET = "dashboard-uploads";
  process.env.NETLIFY_BUILD_HOOK_URL = "https://api.netlify.com/build_hooks/test-hook";

  const seenUrls = [];
  global.fetch = async (url) => {
    seenUrls.push(String(url));
    if (String(url).includes("/storage/v1/object/")) {
      return new Response("", { status: 200 });
    }
    if (String(url).includes("/rest/v1/upload_batches")) {
      return new Response(
        JSON.stringify([{ id: "upload-1", original_filename: "April 2026.csv", storage_path: "uploads/sales/20260415T143837Z__April 2026.csv", file_size_bytes: 42 }]),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }
    return new Response("unexpected", { status: 500 });
  };

  try {
    const formData = new FormData();
    formData.set("upload_kind", "sales");
    formData.append("files", new File(["Order ID\n1\n"], "April 2026.csv", { type: "text/csv" }));

    const request = new Request("http://localhost/api/upload", { method: "POST", body: formData });
    const response = await uploadHandler(request);
    const payload = await response.json();

    assert.equal(response.status, 200);
    assert.equal(payload.rebuildTriggered, false);
    assert.equal(seenUrls.length, 2);
    assert.ok(seenUrls.every((url) => !url.includes("/build_hooks/")));
  } finally {
    global.fetch = originalFetch;
    for (const [key, value] of Object.entries(originalEnv)) {
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  }
});
