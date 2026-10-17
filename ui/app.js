const form = document.querySelector("#upload-form");
const fileInput = document.querySelector("#csv-file");
const dropzone = document.querySelector("#dropzone");
const dropTitle = document.querySelector("#drop-title");
const dropHint = document.querySelector("#drop-hint");
const formError = document.querySelector("#form-error");
const submitButton = document.querySelector("#submit-button");
const buttonLabel = document.querySelector("#button-label");
const buttonSpinner = document.querySelector("#button-spinner");
const panel = document.querySelector("#status-panel");
const label = document.querySelector("#status-label");
const badge = document.querySelector("#status-badge");
const details = document.querySelector("#batch-details");
const batchIdElement = document.querySelector("#batch-id");
const rowCountElement = document.querySelector("#row-count");

const STEP_ORDER = ["prepare", "upload", "process", "match"];
const steps = Object.fromEntries(
  STEP_ORDER.map((name) => [name, document.querySelector(`[data-step="${name}"]`)]),
);

const DEFAULT_DETAILS = {
  prepare: "Request an upload link",
  upload: "Send the CSV",
  process: "Check each row and store users",
  match: "Start loan matching for this batch",
};

const sleep = (milliseconds) =>
  new Promise((resolve) => window.setTimeout(resolve, milliseconds));

function formatSize(bytes) {
  const units = ["B", "KB", "MB", "GB"];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  const digits = unit === 0 || value >= 10 ? 0 : 1;
  return `${value.toFixed(digits)} ${units[unit]}`;
}

function savedLabel(count) {
  const noun = count === 1 ? "user" : "users";
  return `${count.toLocaleString("en-US")} ${noun} saved`;
}

function setFormError(text) {
  formError.hidden = !text;
  formError.textContent = text || "";
}

function showSelectedFile(file) {
  dropzone.classList.add("has-file");
  dropTitle.textContent = file.name;
  dropHint.textContent = `${formatSize(file.size)} · click or drop to replace`;
  setFormError("");
}

function setBusy(isBusy) {
  submitButton.disabled = isBusy;
  submitButton.setAttribute("aria-busy", isBusy ? "true" : "false");
  buttonSpinner.hidden = !isBusy;
  buttonLabel.textContent = isBusy ? "Working" : "Upload CSV";
  dropzone.classList.toggle("is-busy", isBusy);
}

function setHeading(title, status, tone) {
  label.textContent = title;
  badge.textContent = status;
  badge.dataset.tone = tone;
}

function setStep(name, state, detail) {
  const item = steps[name];
  item.dataset.state = state;
  if (detail) {
    item.querySelector(".step-detail").textContent = detail;
  }
  if (state === "active") {
    item.setAttribute("aria-current", "step");
  } else {
    item.removeAttribute("aria-current");
  }
}

function resetProgress() {
  for (const name of STEP_ORDER) {
    setStep(name, "pending", DEFAULT_DETAILS[name]);
  }
  details.hidden = true;
  batchIdElement.textContent = "";
  rowCountElement.textContent = "—";
}

function revealBatch(batchId) {
  batchIdElement.textContent = batchId;
  details.hidden = false;
}

function failFrom(name, text) {
  let seen = false;
  for (const step of STEP_ORDER) {
    if (step === name) {
      setStep(step, "error", text);
      seen = true;
    } else if (seen) {
      setStep(step, "skipped", "Not run");
    }
  }
  setHeading("Upload failed", "failed", "bad");
}

async function readError(response) {
  try {
    const body = await response.json();
    return body.detail || JSON.stringify(body);
  } catch {
    return `${response.status} ${response.statusText}`;
  }
}

async function pollBatch(batchId) {
  let matchWaits = 0;

  for (;;) {
    const response = await fetch(`/api/batches/${batchId}`);
    if (!response.ok) {
      throw new Error(await readError(response));
    }

    const batch = await response.json();
    rowCountElement.textContent =
      batch.status === "completed" ? batch.row_count.toLocaleString("en-US") : "—";

    if (batch.status === "failed") {
      failFrom("process", batch.error_message || "The CSV could not be saved.");
      return;
    }

    if (batch.status === "completed") {
      const saved = savedLabel(batch.row_count);
      setStep("process", "done", saved);

      if (batch.workflow_triggered_at) {
        setStep("match", "done", "Matching started for this batch");
        setHeading(saved, "completed", "done");
        return;
      }

      if (batch.workflow_trigger_error) {
        setStep("match", "error", batch.workflow_trigger_error);
        setHeading(saved, "completed", "done");
        return;
      }

      matchWaits += 1;
      if (matchWaits > 16) {
        setStep("match", "skipped", "Matching did not start");
        setHeading(saved, "completed", "done");
        return;
      }

      setStep("match", "active", "Starting loan matching");
      setHeading(saved, "matching", "active");
      await sleep(1000);
      continue;
    }

    if (batch.status === "processing") {
      setStep("process", "active", "Checking each row and saving users");
      setHeading("Saving users", "processing", "active");
    } else {
      setStep("process", "active", "Waiting for the file to be picked up");
      setHeading("Waiting to save users", "pending", "active");
    }

    await sleep(1000);
  }
}

fileInput.addEventListener("change", () => {
  const file = fileInput.files[0];
  if (!file) {
    return;
  }
  if (!file.name.toLowerCase().endsWith(".csv")) {
    fileInput.value = "";
    dropzone.classList.remove("has-file");
    dropTitle.textContent = "Drop a CSV here";
    dropHint.textContent = "or click to browse · UTF-8";
    setFormError("Only .csv files are accepted.");
    return;
  }
  showSelectedFile(file);
});

["dragover", "dragenter"].forEach((eventName) => {
  dropzone.addEventListener(eventName, (event) => {
    event.preventDefault();
    if (!dropzone.classList.contains("is-busy")) {
      dropzone.classList.add("is-dragover");
    }
  });
});

dropzone.addEventListener("dragleave", () => {
  dropzone.classList.remove("is-dragover");
});

dropzone.addEventListener("drop", (event) => {
  event.preventDefault();
  dropzone.classList.remove("is-dragover");
  if (dropzone.classList.contains("is-busy")) {
    return;
  }
  const file = event.dataTransfer?.files?.[0];
  if (!file) {
    return;
  }
  const transfer = new DataTransfer();
  transfer.items.add(file);
  fileInput.files = transfer.files;
  fileInput.dispatchEvent(new Event("change"));
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const file = fileInput.files[0];
  if (!file) {
    setFormError("Choose a CSV file.");
    return;
  }
  if (!file.name.toLowerCase().endsWith(".csv")) {
    setFormError("Only .csv files are accepted.");
    return;
  }

  setBusy(true);
  setFormError("");
  resetProgress();
  panel.hidden = false;
  setHeading("Getting an upload link", "preparing", "active");
  setStep("prepare", "active", "Requesting an upload link");

  let failedStep = "prepare";

  try {
    const contentType = "text/csv";
    const sessionResponse = await fetch("/api/uploads", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filename: file.name, content_type: contentType }),
    });
    if (!sessionResponse.ok) {
      throw new Error(await readError(sessionResponse));
    }

    const session = await sessionResponse.json();
    revealBatch(session.batch_id);
    setStep("prepare", "done", "Upload link ready");
    failedStep = "upload";
    setStep("upload", "active", `Sending ${file.name}`);
    setHeading("Sending the file", "uploading", "active");

    const uploadResponse = await fetch(session.upload_url, {
      method: "PUT",
      headers: { "Content-Type": contentType },
      body: file,
    });
    if (!uploadResponse.ok) {
      throw new Error(`Upload failed: ${await readError(uploadResponse)}`);
    }

    setStep("upload", "done", "File sent");
    failedStep = "process";
    setStep("process", "active", "Waiting for the file to be picked up");
    setHeading("Waiting to save users", "pending", "active");
    await pollBatch(session.batch_id);
  } catch (error) {
    const text = error instanceof Error ? error.message : "Something went wrong.";
    failFrom(failedStep, text);
  } finally {
    setBusy(false);
  }
});
