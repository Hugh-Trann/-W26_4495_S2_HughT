let selectedFile = null;
let uploadedFileId = null;

const dropZone = document.getElementById("dropZone");
const fileInput = document.getElementById("fileInput");
const btnUpload = document.getElementById("btnUpload");
const btnPreview = document.getElementById("btnPreview");
const btnRun = document.getElementById("btnRun");

const progressBar = document.getElementById("progressBar");
const progressText = document.getElementById("progressText");

const alertArea = document.getElementById("alertArea");
const fileBadge = document.getElementById("fileBadge");
const previewArea = document.getElementById("previewArea");
const statusArea = document.getElementById("statusArea");
const outputsArea = document.getElementById("outputsArea");

function showAlert(type, message) {
  alertArea.innerHTML = `
    <div class="alert alert-${type} alert-dismissible fade show" role="alert">
      ${message}
      <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    </div>
  `;
}

function resetProgress() {
  progressBar.style.width = "0%";
  progressText.textContent = "0%";
}

function setProgress(pct) {
  const p = Math.max(0, Math.min(100, pct));
  progressBar.style.width = `${p}%`;
  progressText.textContent = `${p}%`;
}

function humanFileSize(bytes) {
  const units = ["B", "KB", "MB", "GB"];
  let i = 0;
  let n = bytes;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function onFileChosen(file) {
  if (!file) return;

  const name = file.name.toLowerCase();
  const ok = name.endsWith(".csv") || name.endsWith(".xlsx");
  if (!ok) {
    showAlert("danger", "Only .csv or .xlsx files are allowed.");
    return;
  }
  if (file.size === 0) {
    showAlert("danger", "File is empty.");
    return;
  }
  if (file.size > 50 * 1024 * 1024) {
    showAlert("danger", "Max file size is 50 MB.");
    return;
  }

  selectedFile = file;
  uploadedFileId = null;

  fileBadge.className = "badge text-bg-primary";
  fileBadge.textContent = `${file.name} • ${humanFileSize(file.size)}`;

  btnUpload.disabled = false;
  btnPreview.disabled = true;
  btnRun.disabled = true;

  previewArea.innerHTML =
    `<div class="text-secondary">Ready to upload. Click <strong>Upload now</strong>.</div>`;
  statusArea.className = "text-secondary";
  statusArea.textContent = "Waiting to run pipeline.";
  outputsArea.innerHTML = "";

  resetProgress();
}

dropZone.addEventListener("dragover", (e) => {
  e.preventDefault();
  dropZone.classList.add("dragover");
});

dropZone.addEventListener("dragleave", () => {
  dropZone.classList.remove("dragover");
});

dropZone.addEventListener("drop", (e) => {
  e.preventDefault();
  dropZone.classList.remove("dragover");
  const file = e.dataTransfer.files?.[0];
  onFileChosen(file);
});

fileInput.addEventListener("change", (e) => {
  const file = e.target.files?.[0];
  onFileChosen(file);
});

btnUpload.addEventListener("click", async () => {
  if (!selectedFile) return;

  showAlert("info", "Uploading file...");
  btnUpload.disabled = true;
  btnPreview.disabled = true;
  btnRun.disabled = true;
  resetProgress();

  // Use XMLHttpRequest for real upload progress
  const formData = new FormData();
  formData.append("file", selectedFile);

  const xhr = new XMLHttpRequest();
  xhr.open("POST", "/api/upload");

  xhr.upload.onprogress = (event) => {
    if (event.lengthComputable) {
      const pct = Math.round((event.loaded / event.total) * 100);
      setProgress(pct);
    }
  };

  xhr.onload = () => {
    try {
      const res = JSON.parse(xhr.responseText);
      if (xhr.status >= 200 && xhr.status < 300) {
        uploadedFileId = res.file_id;
        showAlert("success", `Upload successful: <strong>${res.original_name}</strong>`);
        btnPreview.disabled = false;
        btnRun.disabled = false;
        previewArea.innerHTML =
          `<div class="text-secondary">Uploaded. Click <strong>Preview first rows</strong>.</div>`;
      } else {
        showAlert("danger", res.detail || "Upload failed.");
        btnUpload.disabled = false;
      }
    } catch {
      showAlert("danger", "Upload failed (invalid server response).");
      btnUpload.disabled = false;
    }
  };

  xhr.onerror = () => {
    showAlert("danger", "Upload failed (network error).");
    btnUpload.disabled = false;
  };

  xhr.send(formData);
});

btnPreview.addEventListener("click", async () => {
  if (!uploadedFileId) return;

  previewArea.innerHTML = `<div class="text-secondary">Loading preview...</div>`;
  outputsArea.innerHTML = "";

  const res = await fetch(`/api/preview?file_id=${encodeURIComponent(uploadedFileId)}&rows=15`);
  const data = await res.json();

  if (!res.ok) {
    previewArea.innerHTML = `<div class="text-danger">${data.detail || "Preview failed."}</div>`;
    return;
  }

  const cols = data.columns || [];
  const rows = data.rows || [];

  if (cols.length === 0) {
    previewArea.innerHTML = `<div class="text-secondary">No columns found.</div>`;
    return;
  }

  const thead = cols.map((c) => `<th class="text-nowrap">${escapeHtml(String(c))}</th>`).join("");
  const tbody = rows
    .map((r) => {
      const tds = cols
        .map((c) => `<td class="text-nowrap">${escapeHtml(String(r[c] ?? ""))}</td>`)
        .join("");
      return `<tr>${tds}</tr>`;
    })
    .join("");

  previewArea.innerHTML = `
    <div class="small text-secondary mb-2">Showing ${data.row_count} rows</div>
    <div class="table-responsive" style="max-height: 360px;">
      <table class="table table-sm table-hover align-middle">
        <thead class="table-light"><tr>${thead}</tr></thead>
        <tbody>${tbody}</tbody>
      </table>
    </div>
  `;
});

btnRun.addEventListener("click", async () => {
  if (!uploadedFileId) return;

  statusArea.className = "text-secondary";
  statusArea.textContent = "Starting pipeline...";
  outputsArea.innerHTML = "";

  const res = await fetch(`/api/run?file_id=${encodeURIComponent(uploadedFileId)}`, {
    method: "POST",
  });
  const data = await res.json();

  if (!res.ok) {
    statusArea.className = "text-danger";
    statusArea.textContent = data.detail || "Could not start pipeline.";
    return;
  }

  const jobId = data.job_id;
  await pollStatus(jobId);
});

async function pollStatus(jobId) {
  for (let i = 0; i < 10; i++) {
    const res = await fetch(`/api/status?job_id=${encodeURIComponent(jobId)}`);
    const data = await res.json();

    if (!res.ok) {
      statusArea.className = "text-danger";
      statusArea.textContent = data.detail || "Status check failed.";
      return;
    }

    statusArea.className = "text-secondary";
    statusArea.innerHTML = `
      <div class="d-flex justify-content-between">
        <div><strong>Status:</strong> ${escapeHtml(data.status)}</div>
        <div>${escapeHtml(String(data.progress ?? 0))}%</div>
      </div>
      <div class="mt-1">${escapeHtml(data.message || "")}</div>
    `;

    if (data.status === "completed") {
      statusArea.className = "text-success";
      if (Array.isArray(data.outputs) && data.outputs.length > 0) {
        outputsArea.innerHTML = `
          <div class="fw-semibold mb-2">Outputs</div>
          <ul class="list-group">
            ${data.outputs
              .map(
                (o) => `
              <li class="list-group-item d-flex justify-content-between align-items-center">
                <span>${escapeHtml(o.label || "Output")}</span>
                <span class="badge text-bg-light border">${escapeHtml(o.path || "")}</span>
              </li>`
              )
              .join("")}
          </ul>
        `;
      }
      return;
    }

    if (data.status === "failed") {
      statusArea.className = "text-danger";
      return;
    }

    await sleep(800);
  }

  statusArea.className = "text-warning";
  statusArea.textContent = "Still running... (demo polling ended).";
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function escapeHtml(str) {
  return str
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
