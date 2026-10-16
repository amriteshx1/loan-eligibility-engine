import json
from pathlib import Path

READ_BATCH_JS = r"""
const source = $json.body ?? $json;
let raw = source;
if (typeof raw === 'string') {
  try {
    raw = JSON.parse(raw);
  } catch (error) {
    raw = {};
  }
}
const batchId = String(raw.batch_id || '').trim().toLowerCase();
if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(batchId)) {
  throw new Error('batch_id must be a UUID');
}
return [{ json: { batch_id: batchId } }];
""".strip()

LOAD_SQL = """
WITH requested AS (
  SELECT $1::uuid AS batch_id
),
batch AS (
  SELECT
    requested.batch_id,
    COALESCE(b.status, '') AS status
  FROM requested
  LEFT JOIN ingestion_batches b ON b.id = requested.batch_id
),
unnotified AS (
  SELECT
    u.id AS user_record_id,
    u.user_id,
    COUNT(*)::int AS match_count,
    jsonb_agg(
      jsonb_build_object(
        'match_id', m.id,
        'product_name', p.product_name,
        'source_name', p.source_name,
        'interest_rate_min', p.interest_rate_min,
        'interest_rate_max', p.interest_rate_max,
        'match_reason', COALESCE(m.match_reason, '')
      )
      ORDER BY p.product_name, m.id
    ) AS products
  FROM matches m
  JOIN users u ON u.id = m.user_record_id
  JOIN loan_products p ON p.id = m.product_id
  JOIN requested ON requested.batch_id = m.batch_id
  WHERE m.notified_at IS NULL
  GROUP BY u.id, u.user_id
)
SELECT
  batch.batch_id,
  batch.status,
  (
    SELECT COUNT(*)::int
    FROM matches m
    WHERE m.batch_id = batch.batch_id
      AND m.notified_at IS NULL
  ) AS unnotified_matches,
  (
    SELECT COUNT(*)::int
    FROM matches m
    WHERE m.batch_id = batch.batch_id
      AND m.notified_at IS NOT NULL
  ) AS notified_matches,
  COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'user_record_id', user_record_id,
        'user_id', user_id,
        'match_count', match_count,
        'products', products
      )
      ORDER BY user_record_id
    )
    FROM unnotified
  ), '[]'::jsonb) AS recipients
FROM batch
""".strip()

BUILD_JS = r"""
function readEnv(name) {
  try {
    if (typeof $env !== 'undefined' && $env && $env[name]) return String($env[name]).trim();
  } catch (error) {}
  try {
    if (typeof process !== 'undefined' && process.env && process.env[name]) {
      return String(process.env[name]).trim();
    }
  } catch (error) {}
  return '';
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  }
  return [];
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function formatPercent(value) {
  if (value === null || value === undefined || value === '') return null;
  const number = Number(value);
  if (!Number.isFinite(number)) return null;
  return number.toFixed(3).replace(/\.?0+$/, '') + '%';
}

function formatRate(min, max) {
  const lower = formatPercent(min);
  const upper = formatPercent(max);
  if (lower && upper) return lower === upper ? lower : lower + ' to ' + upper;
  if (lower) return 'from ' + lower;
  if (upper) return 'up to ' + upper;
  return 'not stated by the source';
}

function qualification(reason) {
  const text = String(reason || '').replace(/\s+/g, ' ').trim();
  const fallback = 'Likely eligible based on the supplied data. This is not a guaranteed lender approval.';
  if (!text) return fallback;
  if (!/not a guaranteed/i.test(text)) return text + ' ' + fallback;
  return text;
}

function gmailAlias(base, userId) {
  const trimmed = String(base || '').trim().toLowerCase();
  const match = trimmed.match(/^([a-z0-9](?:[a-z0-9.]{0,62}[a-z0-9])?)@gmail\.com$/);
  if (!match || trimmed.includes('+')) {
    throw new Error('NOTIFICATION_TEST_EMAIL_BASE must be a Gmail address without a plus tag');
  }
  const tag = String(userId || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
  if (!tag) throw new Error('user_id cannot be turned into an email alias');
  const address = match[1] + '+loan-' + tag + '@gmail.com';
  if (!address.includes('+loan-') || address === trimmed) {
    throw new Error('Test alias was not derived from the configured Gmail base');
  }
  return address;
}

const loaded = $input.first().json;
if (loaded.status !== 'completed') {
  throw new Error('Notification requires a completed ingestion batch');
}

const batchId = String(loaded.batch_id || '');
const recipients = asArray(loaded.recipients);
if (!recipients.length) {
  return [{
    json: {
      deliver: false,
      batch_id: batchId,
      notified_matches: Number(loaded.notified_matches || 0),
      unnotified_matches: Number(loaded.unnotified_matches || 0),
    },
  }];
}

const base = readEnv('NOTIFICATION_TEST_EMAIL_BASE');
if (!base) throw new Error('NOTIFICATION_TEST_EMAIL_BASE is not set on the n8n service');
const fromEmail = (readEnv('SES_FROM_EMAIL') || base).trim().toLowerCase();
if (!/^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$/.test(fromEmail) || fromEmail.endsWith('@example.com')) {
  throw new Error('SES_FROM_EMAIL must be a real sender address, not a sample CSV address');
}

return recipients.map((recipient) => {
  const userId = String(recipient.user_id || '').replace(/[\r\n]+/g, ' ').trim();
  const products = asArray(recipient.products);
  if (!userId || !products.length) {
    throw new Error('Unnotified match is missing a user id or product');
  }
  const toAddress = gmailAlias(base, userId);
  const matchIds = products.map((product) => Number(product.match_id));
  if (matchIds.some((id) => !Number.isInteger(id) || id <= 0)) {
    throw new Error('Unnotified match is missing a match id');
  }
  const productNames = products.map((product) => String(product.product_name || '').trim());
  if (productNames.some((name) => !name)) throw new Error('A matched product has no name');

  const items = products.map((product) => {
    return '<li>'
      + '<p><strong>' + escapeHtml(product.product_name) + '</strong> (' + escapeHtml(product.source_name || 'source') + ')</p>'
      + '<p>Interest-rate range: ' + escapeHtml(formatRate(product.interest_rate_min, product.interest_rate_max)) + '</p>'
      + '<p>' + escapeHtml(qualification(product.match_reason)) + '</p>'
      + '</li>';
  }).join('');
  const subject = 'Loan products you may qualify for (' + userId + ')';
  const bodyHtml = [
    '<p>Hello,</p>',
    '<p>New personal-loan matches were found for user <strong>' + escapeHtml(userId) + '</strong>.</p>',
    '<p><strong>Likely eligible based on the supplied data. This is not a guaranteed approval, and it is not a lender decision.</strong></p>',
    '<ol>' + items + '</ol>',
    '<p>Each product is listed with the qualification wording from its match. Requirements that were not in the supplied data remain unverified.</p>',
  ].join('');
  const outgoing = subject + '\n' + bodyHtml + '\n' + toAddress + '\n' + fromEmail;
  if (/example\.com/i.test(outgoing)) {
    throw new Error('Refusing to send because a sample CSV address would be included');
  }
  if (toAddress === fromEmail || !toAddress.includes('+loan-')) {
    throw new Error('The recipient must be the derived Gmail plus alias');
  }

  return {
    json: {
      deliver: true,
      batch_id: batchId,
      user_id: userId,
      to_address: toAddress,
      from_email: fromEmail,
      subject,
      body_html: bodyHtml,
      match_ids: matchIds,
      product_names: productNames,
      match_count: matchIds.length,
    },
  };
});
""".strip()

CLASSIFY_JS = r"""
function findMessageId(value, depth) {
  if (!value || depth > 6) return '';
  if (typeof value !== 'object') return '';
  if (Array.isArray(value)) {
    for (const entry of value) {
      const found = findMessageId(entry, depth + 1);
      if (found) return found;
    }
    return '';
  }
  if (typeof value.MessageId === 'string' && value.MessageId.trim()) return value.MessageId.trim();
  for (const key of Object.keys(value)) {
    if (key === 'error' || key === 'stack') continue;
    const found = findMessageId(value[key], depth + 1);
    if (found) return found;
  }
  return '';
}

function errorOf(payload) {
  if (!payload || typeof payload !== 'object') return 'SES returned an empty response';
  if (typeof payload.error === 'string' && payload.error.trim()) return payload.error.trim();
  if (payload.error && typeof payload.error === 'object') {
    return String(payload.error.message || payload.error.description || 'SES send failed').trim();
  }
  return '';
}

function builtItem() {
  try {
    const paired = $('Build notification emails').item;
    if (paired && paired.json && paired.json.user_id) return paired.json;
  } catch (error) {}
  const all = $('Build notification emails').all().map((item) => item.json).filter((item) => item.deliver === true);
  const index = typeof $itemIndex === 'number' ? $itemIndex : 0;
  if (all[index] && all[index].user_id) return all[index];
  throw new Error('Could not pair the SES result with the recipient');
}

const built = builtItem();
const failure = errorOf($json);
const messageId = failure ? '' : findMessageId($json, 0);
const sent = Boolean(messageId);
let errorMessage = '';
if (!sent) {
  errorMessage = failure || ('SES did not return a MessageId. Response keys: ' + Object.keys($json || {}).slice(0, 12).join(', '));
}

return {
  json: {
    batch_id: built.batch_id,
    user_id: built.user_id,
    to_address: built.to_address,
    send_status: sent ? 'sent' : 'failed',
    ses_message_id: messageId,
    match_ids: built.match_ids,
    product_names: built.product_names,
    match_count: built.match_count,
    error_message: errorMessage,
  },
};
""".strip()

RECORD_SQL = """
WITH requested AS (
  SELECT
    $1::uuid AS batch_id,
    $2::text AS send_status,
    $3::jsonb AS match_ids,
    $4::text AS user_id,
    $5::text AS to_address,
    $6::text AS ses_message_id,
    $7::jsonb AS product_names,
    $8::text AS error_message,
    $9::int AS match_count
),
updated AS (
  UPDATE matches AS m
  SET notified_at = NOW()
  FROM requested
  WHERE requested.send_status = 'sent'
    AND m.batch_id = requested.batch_id
    AND m.notified_at IS NULL
    AND m.id IN (
      SELECT value::bigint
      FROM jsonb_array_elements_text((SELECT match_ids FROM requested))
    )
  RETURNING m.id
)
SELECT
  requested.user_id,
  requested.to_address,
  requested.send_status,
  NULLIF(requested.ses_message_id, '') AS ses_message_id,
  requested.product_names,
  NULLIF(requested.error_message, '') AS error_message,
  requested.match_count,
  (SELECT COUNT(*)::int FROM updated) AS rows_notified
FROM requested
""".strip()

SUMMARY_JS = r"""
function asArray(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  }
  return [];
}

const rows = $input.all().map((item) => item.json);
if (rows.length === 1 && rows[0].deliver === false) {
  return [{
    json: {
      status: 'completed',
      batch_id: rows[0].batch_id,
      emails_sent: 0,
      emails_failed: 0,
      matches_notified: 0,
      already_notified_matches: Number(rows[0].notified_matches || 0),
      unnotified_matches_remaining: Number(rows[0].unnotified_matches || 0),
      recipients: [],
    },
  }];
}

const recipients = rows.map((row) => {
  const products = asArray(row.product_names);
  const recorded = Number(row.rows_notified || 0);
  const expected = Number(row.match_count || products.length || 0);
  const recordedOk = row.send_status === 'sent' && recorded === expected && expected > 0;
  return {
    user_id: row.user_id,
    to_address: row.to_address,
    products,
    send_status: recordedOk ? 'sent' : 'failed',
    ses_message_id: row.ses_message_id || null,
    rows_notified: recorded,
    match_count: expected,
    error: recordedOk ? null : (row.error_message || 'SES send was not recorded'),
  };
}).sort((left, right) => String(left.user_id).localeCompare(String(right.user_id)));

const emailsSent = recipients.filter((item) => item.send_status === 'sent').length;
const emailsFailed = recipients.length - emailsSent;
const summary = {
  status: emailsFailed === 0 ? 'completed' : 'failed',
  batch_id: $('Read notification request').first().json.batch_id,
  emails_sent: emailsSent,
  emails_failed: emailsFailed,
  matches_notified: recipients.reduce((sum, item) => sum + Number(item.rows_notified || 0), 0),
  recipients,
};
if (emailsFailed > 0) {
  throw new Error(
    'Notification incomplete: ' + emailsFailed
    + ' email(s) failed and were left unnotified. '
    + JSON.stringify(summary)
  );
}
return [{ json: summary }];
""".strip()

RECORD_REPLACEMENT = (
    "={{ [$json.batch_id, $json.send_status, JSON.stringify($json.match_ids || []), "
    "$json.user_id, $json.to_address, $json.ses_message_id || '', "
    "JSON.stringify($json.product_names || []), $json.error_message || '', "
    "$json.match_count] }}"
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


def postgres_node(name, node_id, position, query, replacement):
    return {
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


def main():
    nodes = [
        {
            "parameters": {
                "httpMethod": "POST",
                "path": "user-notification",
                "authentication": "headerAuth",
                "responseMode": "lastNode",
                "options": {},
            },
            "id": "c5e0a001-0000-4000-8000-000000000001",
            "name": "Notification webhook",
            "type": "n8n-nodes-base.webhook",
            "typeVersion": 2.2,
            "position": [0, 300],
            "webhookId": "user-notification",
            "notes": "Workflow B calls this with batch_id after matching completes.",
            "notesInFlow": True,
        },
        code_node(
            "Read notification request",
            "c5e0a001-0000-4000-8000-000000000002",
            [260, 300],
            READ_BATCH_JS,
        ),
        postgres_node(
            "Load unnotified matches",
            "c5e0a001-0000-4000-8000-000000000003",
            [520, 300],
            LOAD_SQL,
            "={{ [$json.batch_id] }}",
        ),
        code_node(
            "Build notification emails",
            "c5e0a001-0000-4000-8000-000000000004",
            [780, 300],
            BUILD_JS,
        ),
        {
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
                            "id": "has-unnotified-email",
                            "leftValue": "={{ $json.deliver }}",
                            "rightValue": "",
                            "operator": {
                                "type": "boolean",
                                "operation": "true",
                                "singleValue": True,
                            },
                        }
                    ],
                    "combinator": "and",
                },
                "options": {},
            },
            "id": "c5e0a001-0000-4000-8000-000000000005",
            "name": "Has unnotified matches?",
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.2,
            "position": [1040, 300],
        },
        {
            "parameters": {
                "authentication": "iam",
                "resource": "email",
                "operation": "send",
                "fromEmail": "={{ $json.from_email }}",
                "toAddresses": ["={{ $json.to_address }}"],
                "subject": "={{ $json.subject }}",
                "body": "={{ $json.body_html }}",
                "isBodyHtml": True,
                "additionalFields": {},
            },
            "id": "c5e0a001-0000-4000-8000-000000000006",
            "name": "Send match email",
            "type": "n8n-nodes-base.awsSes",
            "typeVersion": 1,
            "position": [1300, 180],
            "continueOnFail": True,
            "notes": "Sends one email per user to the derived Gmail alias. The CSV address is never used.",
            "notesInFlow": True,
        },
        code_node(
            "Classify SES result",
            "c5e0a001-0000-4000-8000-000000000007",
            [1560, 180],
            CLASSIFY_JS,
            mode="runOnceForEachItem",
        ),
        postgres_node(
            "Record notification",
            "c5e0a001-0000-4000-8000-000000000008",
            [1820, 180],
            RECORD_SQL,
            RECORD_REPLACEMENT,
        ),
        code_node(
            "Notification summary",
            "c5e0a001-0000-4000-8000-000000000009",
            [2080, 300],
            SUMMARY_JS,
        ),
    ]
    link = lambda target: {"node": target, "type": "main", "index": 0}
    connections = {
        "Notification webhook": {"main": [[link("Read notification request")]]},
        "Read notification request": {"main": [[link("Load unnotified matches")]]},
        "Load unnotified matches": {"main": [[link("Build notification emails")]]},
        "Build notification emails": {"main": [[link("Has unnotified matches?")]]},
        "Has unnotified matches?": {
            "main": [[link("Send match email")], [link("Notification summary")]]
        },
        "Send match email": {"main": [[link("Classify SES result")]]},
        "Classify SES result": {"main": [[link("Record notification")]]},
        "Record notification": {"main": [[link("Notification summary")]]},
    }
    workflow = {
        "name": "Workflow C - User Notification",
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
    destination = Path(__file__).with_name("user-notification.json")
    destination.write_text(json.dumps(workflow, indent=2) + "\n", encoding="utf-8")
    print(destination.name, len(nodes))


if __name__ == "__main__":
    main()
