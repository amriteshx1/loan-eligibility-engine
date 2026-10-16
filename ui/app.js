const form = document.querySelector("#upload-form");
const fileInput = document.querySelector("#csv-file");
const submitButton = document.querySelector("#submit-button");
const panel = document.querySelector("#status-panel");
const label = document.querySelector("#status-label");
const badge = document.querySelector("#status-badge");
const message = document.querySelector("#status-message");
const details = document.querySelector("#batch-details");
const batchIdElement = document.querySelector("#batch-id");
const rowCountElement = document.querySelector("#row-count");

const sleep = (milliseconds) =>
  new Promise((resolve) => window.setTimeout(resolve, milliseconds));

function showStatus(title, state, text, isError = false) {
  panel.hidden = false;
  panel.classList.toggle("is-error", isError);
  label.textContent = title;
  badge.textContent = state;
  message.textContent = text;
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
  for (;;) {
    const response = await fetch(`/api/batches/${batchId}`);
    if (!response.ok) {
      throw new Error(await readError(response));
    }

    const batch = await response.json();
    badge.textContent = batch.status;
    rowCountElement.textContent = String(batch.row_count);

    if (batch.status === "completed") {
      showStatus(
        "CSV processing complete",
        batch.status,
        `${batch.row_count} user record${batch.row_count === 1 ? "" : "s"} stored in PostgreSQL.`,
      );
      return;
    }

    if (batch.status === "failed") {
      showStatus(
        "CSV processing failed",
        batch.status,
        batch.error_message || "The batch could not be processed.",
        true,
      );
      return;
    }

    showStatus(
      "Processing CSV",
      batch.status,
      "The upload was accepted. User records are being validated and stored.",
    );
    await sleep(1000);
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const file = fileInput.files[0];
  if (!file) {
    return;
  }

  submitButton.disabled = true;
  details.hidden = true;
  showStatus("Preparing upload", "pending", "Requesting a secure upload URL.");

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
    batchIdElement.textContent = session.batch_id;
    rowCountElement.textContent = "0";
    details.hidden = false;
    showStatus(
      "Uploading CSV",
      "uploading",
      "Sending the file directly to object storage.",
    );

    const uploadResponse = await fetch(session.upload_url, {
      method: "PUT",
      headers: { "Content-Type": contentType },
      body: file,
    });
    if (!uploadResponse.ok) {
      throw new Error(`Object upload failed: ${await readError(uploadResponse)}`);
    }

    await pollBatch(session.batch_id);
  } catch (error) {
    showStatus(
      "Upload failed",
      "failed",
      error instanceof Error ? error.message : "An unexpected error occurred.",
      true,
    );
  } finally {
    submitButton.disabled = false;
  }
});

