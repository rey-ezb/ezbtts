const UPLOAD_TARGET_FOLDERS = {
  sales: "All orders",
  samples: "Samples",
  replacements: "Replacements",
  statements: "Finance Tab",
};

function jsonResponse(status, payload) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function sanitizeUploadFilename(filename) {
  const parts = String(filename || "").split(/[/\\]/);
  const name = (parts[parts.length - 1] || "").trim();
  if (!name) throw new Error("Missing filename");
  const cleaned = name.replace(/[<>:"/\\|?*]+/g, "-").replace(/^[. ]+|[. ]+$/g, "");
  if (!cleaned) throw new Error("Invalid filename");
  return cleaned;
}

function uploadPrefix() {
  return String(process.env.SUPABASE_UPLOAD_PREFIX || "uploads").trim().replace(/^\/+|\/+$/g, "");
}

function storageObjectPath(uploadKind, filename) {
  const cleaned = sanitizeUploadFilename(filename);
  const timestamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
  return [uploadPrefix(), uploadKind, `${timestamp}__${cleaned}`].filter(Boolean).join("/");
}

async function uploadToSupabase({ objectPath, content, contentType }) {
  const supabaseUrl = String(process.env.SUPABASE_URL || "").replace(/\/+$/, "");
  const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY || "");
  const bucket = String(process.env.SUPABASE_UPLOAD_BUCKET || "");
  const response = await fetch(`${supabaseUrl}/storage/v1/object/${bucket}/${objectPath}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${serviceRoleKey}`,
      apikey: serviceRoleKey,
      "Content-Type": contentType || "application/octet-stream",
      "x-upsert": "true",
    },
    body: content,
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`Supabase storage upload failed: ${response.status} ${detail}`);
  }
}

async function insertUploadBatch(record) {
  const supabaseUrl = String(process.env.SUPABASE_URL || "").replace(/\/+$/, "");
  const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY || "");
  const response = await fetch(`${supabaseUrl}/rest/v1/upload_batches?select=id,original_filename,storage_path,file_size_bytes,uploaded_at`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${serviceRoleKey}`,
      apikey: serviceRoleKey,
      "Content-Type": "application/json",
      Prefer: "return=representation",
    },
    body: JSON.stringify([record]),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`Supabase metadata insert failed: ${response.status} ${detail}`);
  }
  const rows = await response.json();
  return rows[0] || record;
}

async function triggerBuildHook() {
  const hookUrl = String(process.env.NETLIFY_BUILD_HOOK_URL || "").trim();
  if (!hookUrl) return false;
  const response = await fetch(hookUrl, { method: "POST" });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`Netlify build hook failed: ${response.status} ${detail}`);
  }
  return true;
}

export default async (request) => {
  if (request.method !== "POST") return jsonResponse(405, { message: "Method not allowed." });
  if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY || !process.env.SUPABASE_UPLOAD_BUCKET) {
    return jsonResponse(503, { message: "Hosted uploads are not configured yet." });
  }

  const formData = await request.formData();
  const uploadKind = String(formData.get("upload_kind") || "").trim().toLowerCase();
  if (!UPLOAD_TARGET_FOLDERS[uploadKind]) {
    return jsonResponse(400, { message: "Unsupported upload type." });
  }

  const files = formData.getAll("files").filter((entry) => typeof entry?.arrayBuffer === "function");
  if (!files.length) {
    return jsonResponse(400, { message: "Choose at least one file to upload." });
  }

  try {
    const savedFiles = [];
    for (const file of files) {
      const filename = sanitizeUploadFilename(file.name);
      const objectPath = storageObjectPath(uploadKind, filename);
      const content = Buffer.from(await file.arrayBuffer());
      await uploadToSupabase({ objectPath, content, contentType: file.type });
      const uploaded = await insertUploadBatch({
        upload_type: uploadKind,
        original_filename: filename,
        stored_filename: objectPath.split("/").pop(),
        storage_path: objectPath,
        file_size_bytes: content.byteLength,
        notes: "Uploaded from hosted dashboard",
      });
      savedFiles.push({
        filename: uploaded.original_filename || filename,
        size: uploaded.file_size_bytes || content.byteLength,
        folder: uploadKind,
        storage_path: uploaded.storage_path || objectPath,
      });
    }
    const rebuildTriggered = await triggerBuildHook();
    return jsonResponse(200, {
      message: rebuildTriggered
        ? `Uploaded ${savedFiles.length} file(s) to hosted storage. A rebuild was triggered.`
        : `Uploaded ${savedFiles.length} file(s) to hosted storage.`,
      savedFiles,
      storageMode: "supabase",
      rebuildTriggered,
    });
  } catch (error) {
    return jsonResponse(500, { message: error instanceof Error ? error.message : "Hosted upload failed." });
  }
};

export const config = {
  path: "/api/upload",
};
