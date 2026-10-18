import json
from pathlib import Path

# Kept in sync with backend.app.matching_service.USER_PAGE_SIZE.
# The matching Lambda pages the set-based numeric join at this size.
# n8n no longer walks user pages; 2000 remains the recorded page size.
USER_PAGE_SIZE = 2000

STATUS_SQL = """
SELECT
  requested.id AS batch_id,
  COALESCE(b.status, '') AS status
FROM (SELECT $1::uuid AS id) requested
LEFT JOIN ingestion_batches b ON b.id = requested.id
""".strip()

UPSERT_SQL = """
WITH incoming AS (
  SELECT *
  FROM jsonb_to_recordset($3::jsonb) AS match(
    user_record_id bigint,
    product_id bigint,
    match_method text,
    match_reason text
  )
),
upserted AS (
  INSERT INTO matches (
    batch_id,
    user_record_id,
    product_id,
    match_method,
    match_reason,
    evaluated_at
  )
  SELECT
    $1::uuid,
    incoming.user_record_id,
    incoming.product_id,
    incoming.match_method,
    incoming.match_reason,
    $2::timestamptz
  FROM incoming
  ON CONFLICT (user_record_id, product_id)
  DO UPDATE SET
    batch_id = EXCLUDED.batch_id,
    match_method = EXCLUDED.match_method,
    match_reason = EXCLUDED.match_reason,
    evaluated_at = EXCLUDED.evaluated_at
  WHERE matches.notified_at IS NULL
  RETURNING 1
)
SELECT COUNT(*)::int AS matches_written FROM upserted
""".strip()

STALE_SQL = """
WITH removed AS (
  DELETE FROM matches
  WHERE $3::text = 'completed'
    AND batch_id = $1::uuid
    AND notified_at IS NULL
    AND evaluated_at < $2::timestamptz
  RETURNING 1
)
SELECT COUNT(*)::int AS stale_matches_removed
FROM removed
""".strip()

RUN_SQL = """
WITH inserted AS (
  INSERT INTO matching_runs (
    batch_id,
    status,
    users_in_batch,
    active_products,
    theoretical_pairs,
    sql_candidates,
    deterministic_remaining,
    qualitative_pairs,
    qualitative_llm_pairs,
    llm_calls,
    llm_errors,
    matches_written,
    stale_matches_removed,
    pages_processed,
    products_skipped_currency
  ) VALUES (
    $1::uuid, $2, $3, $4, $5,
    $6, $7, $8, $9,
    $10, $11, $12, $13,
    $14, $15
  )
  RETURNING id
)
SELECT
  inserted.id AS matching_run_id,
  (SELECT COUNT(*)::int FROM matches WHERE batch_id = $1::uuid) AS matches_in_batch
FROM inserted
""".strip()

READ_HANDOFF_JS = r"""
function asArray(value) {
  if (Array.isArray(value)) return value;
  if (value === null || value === undefined || value === '') return [];
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return [];
    try {
      const parsed = JSON.parse(trimmed);
      return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
      return [];
    }
  }
  return [];
}

const source = $json.body ?? $json;
let raw = source;
if (typeof raw === 'string') {
  try {
    raw = JSON.parse(raw);
  } catch (error) {
    raw = {};
  }
}
if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
  throw new Error('Matching handoff must be a JSON object.');
}
if (raw.deterministic_complete !== true) {
  throw new Error('Matching handoff must come from the deterministic matcher.');
}
const batchId = String(raw.batch_id || '').trim().toLowerCase();
if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(batchId)) {
  throw new Error('batch_id must be a UUID');
}
const runStarted = String(raw.run_started || '').trim();
if (!runStarted) throw new Error('run_started is required');
const llmChecks = asArray(raw.llm_checks);
const pending = asArray(raw.pending);
const number = (value) => Number(value || 0);
return [{
  json: {
    batch_id: batchId,
    run_started: runStarted,
    users_in_batch: number(raw.users_in_batch),
    active_products: number(raw.active_products),
    theoretical_pairs: number(raw.theoretical_pairs),
    products_skipped_currency: number(raw.products_skipped_currency),
    sql_candidates: number(raw.sql_candidates),
    deterministic_remaining: number(raw.deterministic_remaining),
    qualitative_pairs: number(raw.qualitative_pairs),
    qualitative_llm_pairs: number(raw.qualitative_llm_pairs),
    llm_calls: number(raw.llm_calls),
    deterministic_matches_written: number(raw.deterministic_matches_written),
    pages_processed: number(raw.pages_processed),
    user_page_size: number(raw.user_page_size) || 2000,
    pending,
    llm_checks: llmChecks,
    needs_llm: llmChecks.length > 0,
  },
}];
""".strip()

REJECT_JS = r"""
const status = $json.status || 'missing';
throw new Error('Matching requires a completed ingestion batch. This batch is ' + status + '.');
""".strip()

DETERMINISTIC_RESULT_JS = r"""
const handoff = $('Read match handoff').first().json;
return [{
  json: {
    batch_id: handoff.batch_id,
    run_started: handoff.run_started,
    status: 'completed',
    users_in_batch: Number(handoff.users_in_batch || 0),
    active_products: Number(handoff.active_products || 0),
    theoretical_pairs: Number(handoff.theoretical_pairs || 0),
    products_skipped_currency: Number(handoff.products_skipped_currency || 0),
    sql_candidates: Number(handoff.sql_candidates || 0),
    deterministic_remaining: Number(handoff.deterministic_remaining || 0),
    qualitative_pairs: Number(handoff.qualitative_pairs || 0),
    qualitative_llm_pairs: Number(handoff.qualitative_llm_pairs || 0),
    llm_calls: Number(handoff.llm_calls || 0),
    llm_errors: 0,
    matches_written: Number(handoff.deterministic_matches_written || 0),
    pages_processed: Number(handoff.pages_processed || 0),
    user_page_size: Number(handoff.user_page_size || 2000),
    users_seen: Number(handoff.users_in_batch || 0),
  },
}];
""".strip()

EXPAND_JS = r"""
function executionKey() {
  try {
    if (typeof $execution !== 'undefined' && $execution.id) return String($execution.id);
  } catch (error) {}
  return 'current';
}

function geminiBody(checks) {
  const users = new Map();
  for (const check of checks) {
    if (!users.has(check.user_record_id)) {
      users.set(check.user_record_id, {
        user_record_id: check.user_record_id,
        employment_status: check.employment_status,
        criteria: [],
      });
    }
    users.get(check.user_record_id).criteria.push({
      check_id: check.check_id,
      product_id: check.product_id,
      criterion: check.criterion,
    });
  }
  return {
    systemInstruction: {
      parts: [{
        text: 'You judge qualitative employment criteria for loan products. Use only the employment_status supplied for each user. Return one result for every criterion. decision must be PASS, FAIL, or UNKNOWN. FAIL when the supplied employment_status clearly contradicts the criterion. student, unemployed, and retired contradict a requirement to be salaried or self-employed. contract contradicts that requirement unless the criterion explicitly allows contract work. PASS only when every part of the criterion is about employment and the supplied status clearly satisfies it. UNKNOWN when any part needs an attribute other than employment_status, or when the status is not enough to decide. Do not infer income, credit score, age, SSN, address, residency, citizenship, visa, or a bank account. Do not change numeric eligibility. Copy check_id, user_record_id, and product_id from the input. reason is one short sentence and may mention only the supplied employment_status.',
      }],
    },
    contents: [{
      role: 'user',
      parts: [{ text: JSON.stringify({ users: Array.from(users.values()) }) }],
    }],
    generationConfig: {
      temperature: 0,
      maxOutputTokens: 2048,
      responseMimeType: 'application/json',
      responseJsonSchema: {
        type: 'object',
        properties: {
          results: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                check_id: { type: 'string' },
                user_record_id: { type: 'integer' },
                product_id: { type: 'integer' },
                decision: { type: 'string', enum: ['PASS', 'FAIL', 'UNKNOWN'] },
                reason: { type: 'string' },
              },
              required: ['check_id', 'user_record_id', 'product_id', 'decision', 'reason'],
            },
          },
        },
        required: ['results'],
      },
      thinkingConfig: { thinkingBudget: 0 },
    },
  };
}

const rules = $('Read match handoff').first().json;
const chunks = [];
for (let index = 0; index < rules.llm_checks.length; index += 25) {
  const checks = rules.llm_checks.slice(index, index + 25);
  chunks.push({ checks, gemini_body: geminiBody(checks) });
}
const store = $getWorkflowStaticData('global');
store.pages = store.pages || {};
store.pages[executionKey()] = {
  batch_id: rules.batch_id,
  run_started: rules.run_started,
  pending: rules.pending,
  chunks: chunks.map((chunk) => chunk.checks),
};
return chunks.map((chunk) => ({ json: { gemini_body: chunk.gemini_body } }));
""".strip()

VALIDATE_JS = r"""
function executionKey() {
  try {
    if (typeof $execution !== 'undefined' && $execution.id) return String($execution.id);
  } catch (error) {}
  return 'current';
}

function renderReason(parts) {
  const sentences = [
    parts.label + '. Likely eligible based on the supplied CSV data. This is not a guaranteed lender approval.',
  ];
  if (parts.passed.length) sentences.push('Passed: ' + parts.passed.join('; ') + '.');
  else sentences.push('No numeric product limits were stated.');
  if (parts.unstated.length) sentences.push('Not stated by the product: ' + parts.unstated.join(', ') + '.');
  sentences.push('Employment: ' + parts.employment_reason);
  if (parts.qualitative_pass.length) {
    sentences.push('Qualitative checks passed from supplied data: ' + parts.qualitative_pass.join('; ') + '.');
  }
  if (parts.qualitative_unknown.length) {
    sentences.push('Not verified because the CSV does not contain that information: ' + parts.qualitative_unknown.join('; ') + '.');
  }
  return sentences.join(' ').slice(0, 1800);
}

function modelText(payload) {
  if (!payload || payload.error) return '';
  const body = payload.candidates ? payload : (payload.body || payload.data || {});
  const parts = body.candidates && body.candidates[0] && body.candidates[0].content
    ? body.candidates[0].content.parts || []
    : [];
  return parts.filter((part) => part && !part.thought).map((part) => part.text || '').join('');
}

const NUMERIC_OVERRIDE = /\b(income|credit|fico|\$|years old|\bage\b)/i;
const FORBIDDEN_PASS = /\b(ssn|social security|bank|address|citizen|visa|residen)/i;
const handoff = $('Read match handoff').first().json;
const store = $getWorkflowStaticData('global');
const saved = store.pages ? store.pages[executionKey()] : null;
if (store.pages) delete store.pages[executionKey()];

if (!saved) {
  return [{
    json: {
      batch_id: handoff.batch_id,
      run_started: handoff.run_started,
      matches: [],
      llm_errors: 1,
      handoff_lost: true,
    },
  }];
}

const responses = $input.all().map((item) => item.json);
const decisions = new Map();
let llmErrors = 0;
if (responses.length !== saved.chunks.length) {
  llmErrors += 1;
} else {
  responses.forEach((payload, index) => {
    const expected = saved.chunks[index];
    let parsed = null;
    try {
      const text = modelText(payload).trim().replace(/^```json\s*|```$/g, '');
      parsed = text ? JSON.parse(text) : null;
    } catch (error) {
      parsed = null;
    }
    if (!parsed || !Array.isArray(parsed.results)) {
      llmErrors += 1;
      return;
    }
    const byId = new Map(expected.map((check) => [check.check_id, check]));
    for (const result of parsed.results) {
      const check = byId.get(String(result.check_id || ''));
      if (!check) continue;
      if (Number(result.user_record_id) !== Number(check.user_record_id)) continue;
      if (Number(result.product_id) !== Number(check.product_id)) continue;
      let decision = String(result.decision || '').toUpperCase();
      if (!['PASS', 'FAIL', 'UNKNOWN'].includes(decision)) decision = 'UNKNOWN';
      const reason = String(result.reason || '').replace(/\s+/g, ' ').trim().slice(0, 300);
      if (decision === 'FAIL' && NUMERIC_OVERRIDE.test(reason)) {
        decisions.set(check.check_id, { decision: 'UNKNOWN', reason: 'The model tried to apply a numeric rule, so the result was ignored.' });
        continue;
      }
      if (decision === 'PASS' && (!check.pass_allowed || FORBIDDEN_PASS.test(reason))) {
        decisions.set(check.check_id, { decision: 'UNKNOWN', reason: 'The criterion needs information the CSV does not contain.' });
        continue;
      }
      decisions.set(check.check_id, {
        decision,
        reason: reason || 'Employment wording was checked against the supplied status.',
      });
    }
  });
}

const matches = [];
for (const pair of saved.pending) {
  let failed = false;
  let usedLlm = false;
  for (const check of pair.checks) {
    usedLlm = true;
    const result = decisions.get(check.check_id) || {
      decision: 'UNKNOWN',
      reason: 'The qualitative check did not return a usable decision for this criterion.',
    };
    if (result.decision === 'FAIL') {
      failed = true;
      break;
    }
    if (result.decision === 'PASS') pair.qualitative_pass.push(check.criterion + ' (' + result.reason + ')');
    else pair.qualitative_unknown.push(check.criterion + ' (' + result.reason + ')');
  }
  if (failed) continue;
  matches.push({
    user_record_id: pair.user_record_id,
    product_id: pair.product_id,
    match_method: usedLlm ? 'deterministic+llm' : 'deterministic',
    match_reason: renderReason(pair),
  });
}

return [{
  json: {
    batch_id: saved.batch_id,
    run_started: saved.run_started,
    matches,
    llm_errors: llmErrors,
    handoff_lost: false,
  },
}];
""".strip()

COUNT_LLM_JS = r"""
const handoff = $('Read match handoff').first().json;
const decision = $('Validate LLM decisions').first().json;
const upsertFailed = Boolean($json.error);
const lost = Boolean(decision.handoff_lost);
const failed = upsertFailed || lost;
const llmWritten = failed ? 0 : Number($json.matches_written || 0);
return [{
  json: {
    batch_id: handoff.batch_id,
    run_started: handoff.run_started,
    status: failed ? 'failed' : 'completed',
    users_in_batch: Number(handoff.users_in_batch || 0),
    active_products: Number(handoff.active_products || 0),
    theoretical_pairs: Number(handoff.theoretical_pairs || 0),
    products_skipped_currency: Number(handoff.products_skipped_currency || 0),
    sql_candidates: Number(handoff.sql_candidates || 0),
    deterministic_remaining: Number(handoff.deterministic_remaining || 0),
    qualitative_pairs: Number(handoff.qualitative_pairs || 0),
    qualitative_llm_pairs: Number(handoff.qualitative_llm_pairs || 0),
    llm_calls: Number(handoff.llm_calls || 0),
    llm_errors: Number(decision.llm_errors || 0) + (upsertFailed ? 1 : 0),
    matches_written: Number(handoff.deterministic_matches_written || 0) + llmWritten,
    pages_processed: Number(handoff.pages_processed || 0),
    user_page_size: Number(handoff.user_page_size || 2000),
    users_seen: failed ? -1 : Number(handoff.users_in_batch || 0),
  },
}];
""".strip()

SUM_JS = r"""
return [{ json: $input.first().json }];
""".strip()

SUMMARY_JS = r"""
try {
  const store = $getWorkflowStaticData('global');
  const key = (typeof $execution !== 'undefined' && $execution.id) ? String($execution.id) : 'current';
  if (store.pages) delete store.pages[key];
} catch (error) {}

const run = $('Sum optimization counts').first().json;
const removed = Number($('Remove stale matches').first().json.stale_matches_removed || 0);
const summary = {
  status: run.status,
  batch_id: run.batch_id,
  users_in_batch: run.users_in_batch,
  active_products: run.active_products,
  theoretical_pairs: run.theoretical_pairs,
  sql_candidates: run.sql_candidates,
  deterministic_remaining: run.deterministic_remaining,
  qualitative_pairs: run.qualitative_pairs,
  qualitative_llm_pairs: run.qualitative_llm_pairs,
  llm_calls: run.llm_calls,
  llm_errors: run.llm_errors,
  matches_written: run.matches_written,
  matches_in_batch: Number($json.matches_in_batch || 0),
  stale_matches_removed: removed,
  pages_processed: run.pages_processed,
  user_page_size: Number(run.user_page_size || 2000),
  products_skipped_currency: run.products_skipped_currency,
  users_seen: run.users_seen,
  matching_run_id: $json.matching_run_id,
  reduction: 'Theoretical pairs ' + run.theoretical_pairs
    + '; SQL left ' + run.sql_candidates
    + '; deterministic rules left ' + run.deterministic_remaining
    + '; qualitative pairs ' + run.qualitative_pairs
    + '; LLM calls ' + run.llm_calls
    + '; matches ' + Number($json.matches_in_batch || 0) + '.',
};
if (run.status !== 'completed') {
  throw new Error('Matching did not finish. The run was recorded as failed and can be retried safely.');
}
return [{ json: summary }];
""".strip()

HANDOFF_JS = r"""
const summary = $('Optimization summary').first().json;
const response = $json || {};
let notificationError = null;
if (response.error) {
  const details = response.error;
  notificationError = typeof details === 'string'
    ? details
    : (details.message || details.description || 'User notification did not finish');
}
return [{
  json: {
    ...summary,
    notification_triggered: !notificationError,
    notification_error: notificationError,
    notification: notificationError ? null : response,
  },
}];
""".strip()

UPSERT_REPLACEMENT = "={{ [$json.batch_id, $json.run_started, JSON.stringify($json.matches || [])] }}"

RUN_REPLACEMENT = (
    "={{ [$('Sum optimization counts').first().json.batch_id, "
    "$('Sum optimization counts').first().json.status, "
    "$('Sum optimization counts').first().json.users_in_batch, "
    "$('Sum optimization counts').first().json.active_products, "
    "$('Sum optimization counts').first().json.theoretical_pairs, "
    "$('Sum optimization counts').first().json.sql_candidates, "
    "$('Sum optimization counts').first().json.deterministic_remaining, "
    "$('Sum optimization counts').first().json.qualitative_pairs, "
    "$('Sum optimization counts').first().json.qualitative_llm_pairs, "
    "$('Sum optimization counts').first().json.llm_calls, "
    "$('Sum optimization counts').first().json.llm_errors, "
    "$('Sum optimization counts').first().json.matches_written, "
    "$json.stale_matches_removed, "
    "$('Sum optimization counts').first().json.pages_processed, "
    "$('Sum optimization counts').first().json.products_skipped_currency] }}"
)


def code_node(name, node_id, position, js, mode="runOnceForAllItems"):
    return {
        "parameters": {"mode": mode, "jsCode": js},
        "id": node_id,
        "name": name,
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": position,
    }


def postgres_node(name, node_id, position, query, replacement, continue_on_fail=False):
    node = {
        "parameters": {
            "resource": "database",
            "operation": "executeQuery",
            "query": query,
            "options": {"queryReplacement": replacement},
        },
        "id": node_id,
        "name": name,
        "type": "n8n-nodes-base.postgres",
        "typeVersion": 2.7,
        "position": position,
    }
    if continue_on_fail:
        node["continueOnFail"] = True
        node["retryOnFail"] = True
        node["maxTries"] = 2
        node["waitBetweenTries"] = 1000
    return node


def if_node(name, node_id, position, condition_id, left, operator):
    return {
        "parameters": {
            "conditions": {
                "options": {
                    "caseSensitive": True,
                    "leftValue": "",
                    "typeValidation": "strict",
                    "version": 2,
                },
                "conditions": [
                    {
                        "id": condition_id,
                        "leftValue": left,
                        "rightValue": "completed" if operator.get("operation") == "equals" else "",
                        "operator": operator,
                    }
                ],
                "combinator": "and",
            },
            "options": {},
        },
        "id": node_id,
        "name": name,
        "type": "n8n-nodes-base.if",
        "typeVersion": 2.2,
        "position": position,
    }


def main():
    if str(USER_PAGE_SIZE) not in READ_HANDOFF_JS:
        raise RuntimeError("Workflow B must keep the 2000-user page size.")
    nodes = [
        {
            "parameters": {
                "httpMethod": "POST",
                "path": "user-loan-matching",
                "authentication": "headerAuth",
                "responseMode": "lastNode",
                "options": {},
            },
            "id": "b4e0a001-0000-4000-8000-000000000001",
            "name": "Ingestion webhook",
            "type": "n8n-nodes-base.webhook",
            "typeVersion": 2.2,
            "position": [0, 280],
            "webhookId": "user-loan-matching",
            "notes": (
                "The matching Lambda posts resolved metrics plus unresolved "
                "qualitative checks. Numeric filtering and deterministic "
                f"rules already ran. User page size is {USER_PAGE_SIZE}."
            ),
            "notesInFlow": True,
        },
        code_node("Read match handoff", "b4e0a001-0000-4000-8000-000000000002", [260, 280], READ_HANDOFF_JS),
        postgres_node(
            "Load batch status",
            "b4e0a001-0000-4000-8000-000000000003",
            [520, 280],
            STATUS_SQL,
            "={{ [$json.batch_id] }}",
        ),
        if_node(
            "Batch completed?",
            "b4e0a001-0000-4000-8000-000000000004",
            [780, 280],
            "batch-is-completed",
            "={{ $json.status }}",
            {"type": "string", "operation": "equals"},
        ),
        code_node("Reject incomplete batch", "b4e0a001-0000-4000-8000-000000000005", [1040, 460], REJECT_JS),
        if_node(
            "Needs qualitative LLM?",
            "b4e0a001-0000-4000-8000-000000000010",
            [1040, 140],
            "needs-qualitative-llm",
            "={{ $('Read match handoff').first().json.needs_llm }}",
            {"type": "boolean", "operation": "true", "singleValue": True},
        ),
        code_node("Expand LLM requests", "b4e0a001-0000-4000-8000-000000000011", [1300, 0], EXPAND_JS),
        {
            "parameters": {
                "method": "POST",
                "url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
                "authentication": "genericCredentialType",
                "genericAuthType": "httpHeaderAuth",
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": "={{ $json.gemini_body }}",
                "options": {"timeout": 60000},
            },
            "id": "b4e0a001-0000-4000-8000-000000000012",
            "name": "Gemini qualitative check",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.5,
            "position": [1560, 0],
            "continueOnFail": True,
            "retryOnFail": True,
            "maxTries": 2,
            "waitBetweenTries": 1500,
        },
        code_node("Validate LLM decisions", "b4e0a001-0000-4000-8000-000000000013", [1820, 0], VALIDATE_JS),
        postgres_node(
            "Upsert qualitative matches",
            "b4e0a001-0000-4000-8000-000000000014",
            [2080, 0],
            UPSERT_SQL,
            UPSERT_REPLACEMENT,
            continue_on_fail=True,
        ),
        code_node("Count LLM writes", "b4e0a001-0000-4000-8000-000000000015", [2340, 0], COUNT_LLM_JS),
        code_node("Deterministic result", "b4e0a001-0000-4000-8000-000000000016", [1300, 280], DETERMINISTIC_RESULT_JS),
        code_node("Sum optimization counts", "b4e0a001-0000-4000-8000-000000000018", [2600, 180], SUM_JS),
        postgres_node(
            "Remove stale matches",
            "b4e0a001-0000-4000-8000-000000000019",
            [2860, 180],
            STALE_SQL,
            "={{ [$json.batch_id, $json.run_started, $json.status] }}",
        ),
        postgres_node(
            "Record optimization run",
            "b4e0a001-0000-4000-8000-00000000001a",
            [3120, 180],
            RUN_SQL,
            RUN_REPLACEMENT,
        ),
        code_node("Optimization summary", "b4e0a001-0000-4000-8000-00000000001b", [3380, 180], SUMMARY_JS),
        {
            "parameters": {
                "method": "POST",
                "url": "http://127.0.0.1:5678/webhook/user-notification",
                "authentication": "genericCredentialType",
                "genericAuthType": "httpHeaderAuth",
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": "={{ JSON.stringify({ batch_id: $json.batch_id }) }}",
                "options": {"timeout": 180000},
            },
            "id": "b4e0a001-0000-4000-8000-00000000001c",
            "name": "Start user notification",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.5,
            "position": [3640, 180],
            "continueOnFail": True,
            "notes": "Hands the completed batch_id to Workflow C. Matching rows are already saved.",
            "notesInFlow": True,
        },
        code_node("Notification handoff", "b4e0a001-0000-4000-8000-00000000001d", [3900, 180], HANDOFF_JS),
    ]
    link = lambda target: {"node": target, "type": "main", "index": 0}
    connections = {
        "Ingestion webhook": {"main": [[link("Read match handoff")]]},
        "Read match handoff": {"main": [[link("Load batch status")]]},
        "Load batch status": {"main": [[link("Batch completed?")]]},
        "Batch completed?": {"main": [[link("Needs qualitative LLM?")], [link("Reject incomplete batch")]]},
        "Needs qualitative LLM?": {
            "main": [[link("Expand LLM requests")], [link("Deterministic result")]]
        },
        "Expand LLM requests": {"main": [[link("Gemini qualitative check")]]},
        "Gemini qualitative check": {"main": [[link("Validate LLM decisions")]]},
        "Validate LLM decisions": {"main": [[link("Upsert qualitative matches")]]},
        "Upsert qualitative matches": {"main": [[link("Count LLM writes")]]},
        "Count LLM writes": {"main": [[link("Sum optimization counts")]]},
        "Deterministic result": {"main": [[link("Sum optimization counts")]]},
        "Sum optimization counts": {"main": [[link("Remove stale matches")]]},
        "Remove stale matches": {"main": [[link("Record optimization run")]]},
        "Record optimization run": {"main": [[link("Optimization summary")]]},
        "Optimization summary": {"main": [[link("Start user notification")]]},
        "Start user notification": {"main": [[link("Notification handoff")]]},
    }
    names = {node["name"] for node in nodes}
    for source, targets in connections.items():
        if source not in names:
            raise RuntimeError(f"Connection source is not a node: {source}")
        for output in targets["main"]:
            for edge in output:
                if edge["node"] not in names:
                    raise RuntimeError(f"Connection target is not a node: {edge['node']}")
    workflow = {
        "name": "Workflow B - User Loan Matching",
        "nodes": nodes,
        "pinData": {},
        "connections": connections,
        "active": False,
        "settings": {
            "executionOrder": "v1",
            "timezone": "Asia/Kolkata",
            "saveManualExecutions": True,
            "saveExecutionProgress": True,
        },
        "meta": {"templateCredsSetupCompleted": False},
        "tags": [],
    }
    destination = Path(__file__).with_name("user-loan-matching.json")
    destination.write_text(json.dumps(workflow, indent=2) + "\n", encoding="utf-8")
    print(destination.name, len(nodes))


if __name__ == "__main__":
    main()
