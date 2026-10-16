import json
from pathlib import Path

SCOPE_SQL = """
SELECT
  requested.id AS batch_id,
  COALESCE(b.status, '') AS status,
  COALESCE(b.row_count, 0)::int AS row_count,
  (SELECT COUNT(*)::int FROM users u WHERE u.batch_id = requested.id) AS users_in_batch,
  (SELECT COUNT(*)::int FROM loan_products p WHERE p.active) AS active_products,
  (
    SELECT COUNT(*)::int
    FROM loan_products p
    WHERE p.active
      AND p.income_currency <> 'USD'
      AND p.minimum_monthly_income IS NOT NULL
  ) AS products_skipped_currency,
  COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'page_index', page_index,
        'first_id', first_id,
        'last_id', last_id,
        'users_in_page', users_in_page
      )
      ORDER BY page_index
    )
    FROM (
      SELECT
        page_index,
        MIN(id) AS first_id,
        MAX(id) AS last_id,
        COUNT(*)::int AS users_in_page
      FROM (
        SELECT id, ((ROW_NUMBER() OVER (ORDER BY id) - 1) / 150)::int AS page_index
        FROM users
        WHERE batch_id = requested.id
      ) numbered
      GROUP BY page_index
    ) pages
  ), '[]'::jsonb) AS pages
FROM (SELECT $1::uuid AS id) requested
LEFT JOIN ingestion_batches b ON b.id = requested.id
""".strip()

CANDIDATE_SQL = """
WITH page AS (
  SELECT
    id,
    user_id,
    email,
    monthly_income,
    credit_score,
    employment_status,
    age
  FROM users
  WHERE batch_id = $1::uuid
    AND id BETWEEN $2::bigint AND $3::bigint
),
candidates AS (
  SELECT
    u.id AS user_record_id,
    u.user_id,
    u.email,
    u.monthly_income,
    u.credit_score,
    u.employment_status,
    u.age,
    p.id AS product_id,
    p.product_name,
    p.source_name,
    p.minimum_monthly_income,
    p.income_currency,
    p.credit_score_min,
    p.credit_score_max,
    p.age_min,
    p.age_max,
    p.accepted_employment_statuses,
    p.qualitative_criteria
  FROM page u
  JOIN loan_products p
    ON p.active = TRUE
   AND (
     p.income_currency = 'USD'
     OR p.minimum_monthly_income IS NULL
   )
   AND (p.minimum_monthly_income IS NULL OR u.monthly_income >= p.minimum_monthly_income)
   AND (p.credit_score_min IS NULL OR u.credit_score >= p.credit_score_min)
   AND (p.credit_score_max IS NULL OR u.credit_score <= p.credit_score_max)
   AND (p.age_min IS NULL OR u.age >= p.age_min)
   AND (p.age_max IS NULL OR u.age <= p.age_max)
)
SELECT
  (SELECT COUNT(*)::int FROM page) AS users_in_page,
  (SELECT COUNT(*)::int FROM candidates) AS sql_candidates,
  $4::int AS users_in_batch,
  $5::int AS active_products,
  $6::int AS theoretical_pairs,
  $7::int AS products_skipped_currency,
  $8::int AS page_index,
  COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'user_record_id', user_record_id,
        'user_id', user_id,
        'email', email,
        'monthly_income', monthly_income,
        'credit_score', credit_score,
        'employment_status', employment_status,
        'age', age,
        'product_id', product_id,
        'product_name', product_name,
        'source_name', source_name,
        'minimum_monthly_income', minimum_monthly_income,
        'income_currency', income_currency,
        'credit_score_min', credit_score_min,
        'credit_score_max', credit_score_max,
        'age_min', age_min,
        'age_max', age_max,
        'accepted_employment_statuses', to_jsonb(accepted_employment_statuses),
        'qualitative_criteria', qualitative_criteria
      )
      ORDER BY user_record_id, product_id
    )
    FROM candidates
  ), '[]'::jsonb) AS candidates
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
SELECT
  (SELECT COUNT(*)::int FROM upserted) AS matches_written,
  $4::int AS users_in_page,
  $5::int AS sql_candidates,
  $6::int AS deterministic_remaining,
  $7::int AS qualitative_pairs,
  $8::int AS qualitative_llm_pairs,
  $9::int AS llm_calls,
  $10::int AS llm_errors,
  $11::int AS page_index
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
return [{ json: { batch_id: batchId, run_started: new Date().toISOString() } }];
""".strip()

REJECT_JS = r"""
const status = $json.status || 'missing';
throw new Error('Matching requires a completed ingestion batch. This batch is ' + status + '.');
""".strip()

BUILD_PAGES_JS = r"""
const scope = $input.first().json;
const request = $('Read batch request').first().json;
let pages = scope.pages;
if (typeof pages === 'string') pages = JSON.parse(pages || '[]');
if (!Array.isArray(pages)) pages = [];
const base = {
  batch_id: request.batch_id,
  run_started: request.run_started,
  users_in_batch: Number(scope.users_in_batch || 0),
  active_products: Number(scope.active_products || 0),
  theoretical_pairs: Number(scope.users_in_batch || 0) * Number(scope.active_products || 0),
  products_skipped_currency: Number(scope.products_skipped_currency || 0),
};
if (!pages.length) {
  return [{ json: { ...base, page_index: 0, first_id: 0, last_id: -1 } }];
}
return pages.map((page) => ({
  json: {
    ...base,
    page_index: Number(page.page_index),
    first_id: Number(page.first_id),
    last_id: Number(page.last_id),
  },
}));
""".strip()

RULES_JS = r"""
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

function candidatesOf(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = JSON.parse(value);
    if (Array.isArray(parsed)) return parsed;
    if (parsed && typeof parsed === 'object') return [parsed];
  }
  if (value && typeof value === 'object') return [value];
  return [];
}

function normalizeEmployment(value) {
  const compact = String(value || '').trim().toLowerCase().replace(/[\s_-]+/g, '');
  const mapped = {
    salaried: 'salaried',
    employed: 'salaried',
    fulltime: 'salaried',
    selfemployed: 'self_employed',
    retired: 'retired',
    unemployed: 'unemployed',
    student: 'student',
    contract: 'contract',
    contractor: 'contract',
  };
  return mapped[compact] || null;
}

function acceptedList(value) {
  return asArray(value).map((entry) => normalizeEmployment(entry)).filter(Boolean);
}

function employmentDecision(status, accepted) {
  const list = acceptedList(accepted);
  if (!list.length) {
    return { decision: 'UNKNOWN', reason: 'No accepted employment list was stated.' };
  }
  const normalized = normalizeEmployment(status);
  if (!normalized) {
    return {
      decision: 'UNKNOWN',
      reason: 'Employment status could not be mapped, so it was not treated as a pass or a fail.',
    };
  }
  if (list.includes(normalized)) {
    return { decision: 'PASS', reason: 'Employment status ' + normalized + ' is in the accepted list.' };
  }
  return {
    decision: 'FAIL',
    reason: 'Employment status ' + normalized + ' is outside the accepted list (' + list.join(', ') + ').',
  };
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function numericParts(candidate) {
  const incomeMin = numberOrNull(candidate.minimum_monthly_income);
  const creditMin = numberOrNull(candidate.credit_score_min);
  const creditMax = numberOrNull(candidate.credit_score_max);
  const ageMin = numberOrNull(candidate.age_min);
  const ageMax = numberOrNull(candidate.age_max);
  const currency = String(candidate.income_currency || 'USD').trim();
  const passed = [];
  const unstated = [];
  if (incomeMin === null) {
    unstated.push(currency === 'USD' ? 'minimum income' : 'minimum income (' + currency + ' was not compared to the CSV)');
  } else {
    passed.push('monthly income ' + Number(candidate.monthly_income).toFixed(2) + ' >= ' + incomeMin.toFixed(2) + ' ' + currency);
  }
  if (creditMin === null && creditMax === null) unstated.push('credit-score limits');
  else {
    if (creditMin !== null) passed.push('credit score ' + candidate.credit_score + ' >= ' + creditMin);
    if (creditMax !== null) passed.push('credit score ' + candidate.credit_score + ' <= ' + creditMax);
  }
  if (ageMin === null && ageMax === null) unstated.push('age limits');
  else {
    if (ageMin !== null) passed.push('age ' + candidate.age + ' >= ' + ageMin);
    if (ageMax !== null) passed.push('age ' + candidate.age + ' <= ' + ageMax);
  }
  return { passed, unstated };
}

const UNCHECKABLE = /\b(ssn|social security|bank|photo id|government-issued|government issued|address|reside|resident|residency|nevada|west virginia|citizen|citizenship|visa|payment history|credit usage)\b/i;
const EMPLOYMENT = /\b(employ(?:ed|ment|er)?|salaried|self-employed|self employed|student|retired|unemployed|occupation)\b/i;
const EMAIL = /\bemail\b/i;
const EMAIL_EXTRA = /\b(online|device|computer|phone|finalize|finalise)\b/i;

function classify(criterion) {
  const text = String(criterion || '').trim();
  if (!text) return 'unknown';
  const uncheckable = UNCHECKABLE.test(text);
  const employment = EMPLOYMENT.test(text);
  if (employment) return uncheckable ? 'llm_limited' : 'llm';
  if (EMAIL.test(text) && !EMAIL_EXTRA.test(text) && !uncheckable) return 'email';
  return 'unknown';
}

function criterionText(value) {
  if (typeof value === 'string') return value.trim();
  if (value && typeof value === 'object' && typeof value.criterion === 'string') return value.criterion.trim();
  return '';
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

const row = $input.first().json;
const request = $('Read batch request').first().json;
const candidates = candidatesOf(row.candidates);
const matches = [];
const pending = [];
const llmChecks = [];
let deterministicRemaining = 0;
let qualitativePairs = 0;

for (const candidate of candidates) {
  const employment = employmentDecision(candidate.employment_status, candidate.accepted_employment_statuses);
  if (employment.decision === 'FAIL') continue;
  deterministicRemaining += 1;
  const criteria = asArray(candidate.qualitative_criteria).map(criterionText).filter(Boolean);
  if (criteria.length) qualitativePairs += 1;
  const numeric = numericParts(candidate);
  const parts = {
    label: String(candidate.product_name || candidate.source_name || 'Loan product'),
    passed: numeric.passed,
    unstated: numeric.unstated,
    employment_reason: employment.reason,
    qualitative_pass: [],
    qualitative_unknown: [],
  };
  const llmForPair = [];
  for (const criterion of criteria) {
    const kind = classify(criterion);
    if (kind === 'email') {
      const email = String(candidate.email || '');
      if (email.includes('@')) parts.qualitative_pass.push(criterion + ' (an email address is present)');
      else parts.qualitative_unknown.push(criterion + ' (the email value was not usable)');
      continue;
    }
    if (kind === 'unknown') {
      parts.qualitative_unknown.push(criterion);
      continue;
    }
    const passAllowed = kind === 'llm';
    if (employment.decision === 'PASS' && passAllowed) {
      parts.qualitative_pass.push(criterion + ' (covered by the structured employment list)');
      continue;
    }
    if (employment.decision === 'PASS' && !passAllowed) {
      parts.qualitative_unknown.push(criterion);
      continue;
    }
    llmForPair.push({ criterion, pass_allowed: passAllowed });
  }
  if (!llmForPair.length) {
    matches.push({
      user_record_id: Number(candidate.user_record_id),
      product_id: Number(candidate.product_id),
      match_method: 'deterministic',
      match_reason: renderReason(parts),
    });
    continue;
  }
  const checks = llmForPair.map((check) => {
    const checkId = String(candidate.user_record_id) + ':' + String(candidate.product_id) + ':' + String(llmChecks.length);
    llmChecks.push({
      check_id: checkId,
      user_record_id: Number(candidate.user_record_id),
      product_id: Number(candidate.product_id),
      employment_status: String(candidate.employment_status || ''),
      criterion: check.criterion,
      pass_allowed: check.pass_allowed,
    });
    return { check_id: checkId, criterion: check.criterion, pass_allowed: check.pass_allowed };
  });
  pending.push({ ...parts, user_record_id: Number(candidate.user_record_id), product_id: Number(candidate.product_id), checks });
}

const pageContext = {
  users_in_page: Number(row.users_in_page || 0),
  sql_candidates: Number(row.sql_candidates || 0),
  deterministic_remaining: deterministicRemaining,
  qualitative_pairs: qualitativePairs,
  qualitative_llm_pairs: pending.length,
  llm_calls: Math.ceil(llmChecks.length / 25) || 0,
  llm_errors: 0,
  page_index: Number(row.page_index || 0),
};

return [{
  json: {
    batch_id: request.batch_id,
    run_started: request.run_started,
    needs_llm: llmChecks.length > 0,
    matches,
    pending,
    llm_checks: llmChecks,
    page_context: pageContext,
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

const rules = $input.first().json;
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
  matches: rules.matches,
  pending: rules.pending,
  page_context: rules.page_context,
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
const request = $('Read batch request').first().json;
const store = $getWorkflowStaticData('global');
const saved = store.pages ? store.pages[executionKey()] : null;
if (store.pages) delete store.pages[executionKey()];

function failedPage() {
  return [{
    json: {
      batch_id: request.batch_id,
      run_started: request.run_started,
      matches: [],
      page_context: {
        users_in_page: -1,
        sql_candidates: 0,
        deterministic_remaining: 0,
        qualitative_pairs: 0,
        qualitative_llm_pairs: 0,
        llm_calls: 0,
        llm_errors: 1,
        page_index: -1,
      },
    },
  }];
}

if (!saved) return failedPage();

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

const matches = saved.matches.slice();
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
    page_context: { ...saved.page_context, llm_errors: Number(saved.page_context.llm_errors || 0) + llmErrors },
  },
}];
""".strip()

PAGE_RESULT_JS = r"""
if ($json.error) {
  return {
    json: {
      users_in_page: -1,
      sql_candidates: 0,
      deterministic_remaining: 0,
      qualitative_pairs: 0,
      qualitative_llm_pairs: 0,
      llm_calls: 0,
      llm_errors: 0,
      matches_written: 0,
      page_index: -1,
      database_error: true,
    },
  };
}
return {
  json: {
    users_in_page: Number($json.users_in_page || 0),
    sql_candidates: Number($json.sql_candidates || 0),
    deterministic_remaining: Number($json.deterministic_remaining || 0),
    qualitative_pairs: Number($json.qualitative_pairs || 0),
    qualitative_llm_pairs: Number($json.qualitative_llm_pairs || 0),
    llm_calls: Number($json.llm_calls || 0),
    llm_errors: Number($json.llm_errors || 0),
    matches_written: Number($json.matches_written || 0),
    page_index: Number($json.page_index || 0),
    database_error: false,
  },
};
""".strip()

SUM_JS = r"""
const request = $('Read batch request').first().json;
const scopePage = $('Build pages').first().json;
const pages = $input.all().map((item) => item.json);
const total = (key) => pages.reduce((sum, page) => sum + Number(page[key] || 0), 0);
const usersSeen = total('users_in_page');
const databaseErrors = pages.filter((page) => page.database_error).length;
const usersInBatch = Number(scopePage.users_in_batch || 0);
const status = databaseErrors === 0 && usersSeen === usersInBatch ? 'completed' : 'failed';
return [{
  json: {
    batch_id: request.batch_id,
    run_started: request.run_started,
    status,
    users_in_batch: usersInBatch,
    active_products: Number(scopePage.active_products || 0),
    theoretical_pairs: Number(scopePage.theoretical_pairs || 0),
    products_skipped_currency: Number(scopePage.products_skipped_currency || 0),
    sql_candidates: total('sql_candidates'),
    deterministic_remaining: total('deterministic_remaining'),
    qualitative_pairs: total('qualitative_pairs'),
    qualitative_llm_pairs: total('qualitative_llm_pairs'),
    llm_calls: total('llm_calls'),
    llm_errors: total('llm_errors'),
    matches_written: total('matches_written'),
    pages_processed: pages.length,
    users_seen: usersSeen,
    database_errors: databaseErrors,
  },
}];
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
  throw new Error('Matching did not finish every user page. The run was recorded as failed and can be retried safely.');
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

UPSERT_REPLACEMENT = (
    "={{ [$json.batch_id, $json.run_started, JSON.stringify($json.matches || []), "
    "$json.page_context.users_in_page, $json.page_context.sql_candidates, "
    "$json.page_context.deterministic_remaining, $json.page_context.qualitative_pairs, "
    "$json.page_context.qualitative_llm_pairs, $json.page_context.llm_calls, "
    "$json.page_context.llm_errors, $json.page_context.page_index] }}"
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


def main():
    nodes = [
        {
            "parameters": {
                "httpMethod": "POST",
                "path": "user-loan-matching",
                "authentication": "headerAuth",
                "responseMode": "onReceived",
                "options": {},
            },
            "id": "b4e0a001-0000-4000-8000-000000000001",
            "name": "Ingestion webhook",
            "type": "n8n-nodes-base.webhook",
            "typeVersion": 2.2,
            "position": [0, 280],
            "webhookId": "user-loan-matching",
        },
        code_node("Read batch request", "b4e0a001-0000-4000-8000-000000000002", [260, 280], READ_BATCH_JS),
        postgres_node(
            "Load batch scope",
            "b4e0a001-0000-4000-8000-000000000003",
            [520, 280],
            SCOPE_SQL,
            "={{ [$json.batch_id] }}",
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
                            "id": "batch-is-completed",
                            "leftValue": "={{ $json.status }}",
                            "rightValue": "completed",
                            "operator": {"type": "string", "operation": "equals"},
                        }
                    ],
                    "combinator": "and",
                },
                "options": {},
            },
            "id": "b4e0a001-0000-4000-8000-000000000004",
            "name": "Batch completed?",
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.2,
            "position": [780, 280],
        },
        code_node("Reject incomplete batch", "b4e0a001-0000-4000-8000-000000000005", [1040, 460], REJECT_JS),
        code_node("Build pages", "b4e0a001-0000-4000-8000-000000000006", [1040, 140], BUILD_PAGES_JS),
        {
            "parameters": {"batchSize": 1, "options": {}},
            "id": "b4e0a001-0000-4000-8000-000000000007",
            "name": "Loop pages",
            "type": "n8n-nodes-base.splitInBatches",
            "typeVersion": 3,
            "position": [1300, 140],
        },
        postgres_node(
            "Load SQL candidates",
            "b4e0a001-0000-4000-8000-000000000008",
            [1560, 40],
            CANDIDATE_SQL,
            "={{ [$json.batch_id, $json.first_id, $json.last_id, $json.users_in_batch, $json.active_products, $json.theoretical_pairs, $json.products_skipped_currency, $json.page_index] }}",
        ),
        code_node("Apply deterministic rules", "b4e0a001-0000-4000-8000-000000000009", [1820, 40], RULES_JS),
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
                            "id": "needs-qualitative-llm",
                            "leftValue": "={{ $json.needs_llm }}",
                            "rightValue": "",
                            "operator": {"type": "boolean", "operation": "true", "singleValue": True},
                        }
                    ],
                    "combinator": "and",
                },
                "options": {},
            },
            "id": "b4e0a001-0000-4000-8000-000000000010",
            "name": "Needs qualitative LLM?",
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.2,
            "position": [2080, 40],
        },
        code_node("Expand LLM requests", "b4e0a001-0000-4000-8000-000000000011", [2340, -120], EXPAND_JS),
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
            "position": [2600, -120],
            "continueOnFail": True,
            "retryOnFail": True,
            "maxTries": 2,
            "waitBetweenTries": 1500,
        },
        code_node("Validate LLM decisions", "b4e0a001-0000-4000-8000-000000000013", [2860, -120], VALIDATE_JS),
        postgres_node(
            "Upsert matches after LLM",
            "b4e0a001-0000-4000-8000-000000000014",
            [3120, -120],
            UPSERT_SQL,
            UPSERT_REPLACEMENT,
            continue_on_fail=True,
        ),
        code_node(
            "Page result after LLM",
            "b4e0a001-0000-4000-8000-000000000015",
            [3380, -120],
            PAGE_RESULT_JS,
            mode="runOnceForEachItem",
        ),
        postgres_node(
            "Upsert matches",
            "b4e0a001-0000-4000-8000-000000000016",
            [2340, 220],
            UPSERT_SQL,
            UPSERT_REPLACEMENT,
            continue_on_fail=True,
        ),
        code_node(
            "Page result",
            "b4e0a001-0000-4000-8000-000000000017",
            [2600, 220],
            PAGE_RESULT_JS,
            mode="runOnceForEachItem",
        ),
        code_node("Sum optimization counts", "b4e0a001-0000-4000-8000-000000000018", [1560, 460], SUM_JS),
        postgres_node(
            "Remove stale matches",
            "b4e0a001-0000-4000-8000-000000000019",
            [1820, 460],
            STALE_SQL,
            "={{ [$json.batch_id, $json.run_started, $json.status] }}",
        ),
        postgres_node(
            "Record optimization run",
            "b4e0a001-0000-4000-8000-00000000001a",
            [2080, 460],
            RUN_SQL,
            "={{ [$('Sum optimization counts').first().json.batch_id, $('Sum optimization counts').first().json.status, $('Sum optimization counts').first().json.users_in_batch, $('Sum optimization counts').first().json.active_products, $('Sum optimization counts').first().json.theoretical_pairs, $('Sum optimization counts').first().json.sql_candidates, $('Sum optimization counts').first().json.deterministic_remaining, $('Sum optimization counts').first().json.qualitative_pairs, $('Sum optimization counts').first().json.qualitative_llm_pairs, $('Sum optimization counts').first().json.llm_calls, $('Sum optimization counts').first().json.llm_errors, $('Sum optimization counts').first().json.matches_written, $json.stale_matches_removed, $('Sum optimization counts').first().json.pages_processed, $('Sum optimization counts').first().json.products_skipped_currency] }}",
        ),
        code_node("Optimization summary", "b4e0a001-0000-4000-8000-00000000001b", [2340, 460], SUMMARY_JS),
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
            "position": [2600, 460],
            "continueOnFail": True,
            "notes": "Hands the completed batch_id to Workflow C. Matching rows are already saved.",
            "notesInFlow": True,
        },
        code_node("Notification handoff", "b4e0a001-0000-4000-8000-00000000001d", [2860, 460], HANDOFF_JS),
    ]
    link = lambda target: {"node": target, "type": "main", "index": 0}
    connections = {
        "Ingestion webhook": {"main": [[link("Read batch request")]]},
        "Read batch request": {"main": [[link("Load batch scope")]]},
        "Load batch scope": {"main": [[link("Batch completed?")]]},
        "Batch completed?": {"main": [[link("Build pages")], [link("Reject incomplete batch")]]},
        "Build pages": {"main": [[link("Loop pages")]]},
        "Loop pages": {"main": [[link("Sum optimization counts")], [link("Load SQL candidates")]]},
        "Load SQL candidates": {"main": [[link("Apply deterministic rules")]]},
        "Apply deterministic rules": {"main": [[link("Needs qualitative LLM?")]]},
        "Needs qualitative LLM?": {"main": [[link("Expand LLM requests")], [link("Upsert matches")]]},
        "Expand LLM requests": {"main": [[link("Gemini qualitative check")]]},
        "Gemini qualitative check": {"main": [[link("Validate LLM decisions")]]},
        "Validate LLM decisions": {"main": [[link("Upsert matches after LLM")]]},
        "Upsert matches after LLM": {"main": [[link("Page result after LLM")]]},
        "Page result after LLM": {"main": [[link("Loop pages")]]},
        "Upsert matches": {"main": [[link("Page result")]]},
        "Page result": {"main": [[link("Loop pages")]]},
        "Sum optimization counts": {"main": [[link("Remove stale matches")]]},
        "Remove stale matches": {"main": [[link("Record optimization run")]]},
        "Record optimization run": {"main": [[link("Optimization summary")]]},
        "Optimization summary": {"main": [[link("Start user notification")]]},
        "Start user notification": {"main": [[link("Notification handoff")]]},
    }
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
