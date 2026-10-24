# AGENTS.md

Guidance for coding agents working in this repository. `CLAUDE.md` is only an `@AGENTS.md` pointer,
so put instructions here and leave the pointer alone. `.github/workflows/agents-md-maintenance.yml`
updates this file inside each PR that changes the code.

## Project

An Apify Actor (TypeScript, ESM, npm) that copies items from an Apify dataset into an Airtable table.
The user authenticates with Airtable through an Apify OAuth account, picks a base and table, and gives
a list of field mappings (dataset path → Airtable field). Typical use is as an integration after a
scraper run, with `datasetId: {{resource.defaultDatasetId}}`. The Actor pushes a single summary item
(counts, timing, base/table, Airtable user) to its default dataset; `.actor/output_schema.json` points
at that dataset.

Three operations (`operation` input):

- `Append` — write into an existing table.
- `Override` — delete every record in the table, then write.
- `Create` — create the table if it is missing. If it exists, `clearOnCreate: true` clears it first
  and `clearOnCreate: false` (the input default) makes the run **fail**. The `operation` description
  in `input_schema.json` says Create "behaves like Append" on an existing table — that is wrong.

## Commands

```bash
npm install
npm run build          # tsc → dist/; this is the only type check
npm run lint           # eslint — but see below, it does not lint src/
npm run format:check   # prettier; currently flags README.md and agents-md-maintenance.yml
npm run start          # tsx src/main.ts (needs real input, see below)
```

- `npm run lint` uses `@apify/eslint-config/js.js` only, so ESLint ignores every `.ts` file
  (`npx eslint src/main.ts` reports "File ignored because no matching configuration"). It passes
  because it checks almost nothing; rely on `npm run build` for verification.
- There are no tests; `npm test` is the template stub that exits 1. There is no CI besides
  `agents-md-maintenance.yml`.
- A local run is not self-contained: `getAirtableClient` exchanges the OAuth account ID from the
  input for an Airtable token at `console-backend.apify.com/public/actor-oauth-accounts/<id>/token`
  using `APIFY_TOKEN`, so you need a real Apify token and a connected Airtable OAuth account.
- `apify push` deploys to the platform — ask the user first.
- The Docker image (`apify/actor-node:22`) builds with `npm run build` and runs `node dist/main.js`
  (`start:prod`). `package.json` says Node ≥ 18.

## Architecture

`src/main.ts` is a single top-level pipeline:

1. Validate input (`validation.ts#validateInput`), then drop mappings with an empty `target`.
2. Get an Airtable client (`api.ts#getAirtableClient`) — returns both the raw OAuth `token` and an
   `airtable` SDK instance (`sdk`).
3. `whoami`, then resolve the base (`resolveBaseId`). A `base` value starting with `app` is treated
   as an ID; anything else is matched case-insensitively against base names. So a base _named_
   "apple…" can only be addressed by ID. `listBases` reads only the first page (ignores `offset`).
4. `ensureTable` (find by ID or case-insensitive name; create it in `Create` mode) and
   `ensureFieldsExist` (create missing fields when the mapping's `targetType` is `new`, throw when
   it is `existing`).
5. Clear the table for `Override` or `Create` + `clearOnCreate: true`.
6. If `uniqueId` is set, load existing values of the mapped target field into a set.
7. Read the dataset in pages of `DATASET_BATCH_SIZE` (50), map, dedupe, and write in batches of
   `AIRTABLE_WRITE_BATCH_SIZE` (10) with a 200 ms sleep between write calls.

Two HTTP paths to Airtable — keep them apart:

- **Meta API** (schema, create table/field, whoami, list bases) goes through `fetchWithRetry` in
  `api.ts`: plain `fetch` with `Bearer <token>`, retrying 429/5xx/network errors 3 times (1 s, 2 s,
  4 s). The SDK cannot call these endpoints.
- **Record operations** (select, create, destroy) go through the `airtable` SDK, which does its own
  rate-limit retries.

The OAuth token fetch itself is a bare `fetch` with no retry. Responses from both the token endpoint
and the Meta API (except field creation, whose response is not parsed) are checked with the zod
schemas in `schemas.ts` (`validateResponse` logs the raw
payload and throws). Several schemas there and `AirtableOAuthAccountResponse` in `types.ts` are unused.

### Value conversion

Values are normalized twice with the same function, `utils.ts#normalizeCellValue`:

1. In `mapItemsToAirtableRecords`, against the mapping's declared `fieldType`, with the value's
   inferred Airtable type as the source type. Sources use dot paths (`getValueAtPath`); `null`
   results are omitted from the record.
2. In `api.ts#normalizeRecordFields`, against the real field type from the table schema
   (`schemaMap`), with `typeof value` as the source type.

Only `number`, `checkbox`, `singleLineText` and `multilineText` convert; any other Airtable type
(date, select, email, …) becomes `null`, so mapping into such an existing field writes nothing.
Objects and arrays become pretty-printed JSON; text is cut at `MAX_STRING_LENGTH` (10 000). The input
schema offers only those four `fieldType`s, although `types.ts` and `createTable` know more.

### Duplicates

`uniqueId` is a dataset path; its mapping's `target` is the Airtable field compared against. Values
are compared after `String().trim().toLowerCase()` on both sides, and each written ID is added to the
set, so duplicates inside the dataset are skipped too — even when `uniqueId` has no mapping and the
Airtable check is skipped.

### Resume state

Progress (`offset`, `importedCount`, `skippedDuplicates`) lives in `Actor.useState('import-progress')`
and is advanced after each dataset page that wrote records, so a migrated run resumes from there.

## Conventions and gotchas

- Local imports use `.js` extensions (`./api.js`) — required by `NodeNext` resolution; don't change
  them to `.ts`.
- The OAuth input key is the literal `oAuthAccount.BpW1howJtlI9fdEck`
  (`constants.ts#OAUTH_ACCOUNT_FIELD`, `types.ts#ActorInput`, `input_schema.json`); it ties the
  Actor to its Apify OAuth provider config, so change all three or none.
- `tsconfig.json` includes `lib: ["DOM"]` for `fetch`/`Response` types.
- Known bugs (don't "document around" them; fix them when you touch the code):
    - `schemaMap` in `main.ts` is built from the table schema fetched _before_ `ensureFieldsExist`
      creates fields, and `normalizeRecordFields` drops fields missing from `schemaMap`. So fields
      created for `targetType: "new"` in an existing table get no values in that run. (A table newly
      created by `Create` is re-fetched, so it is fine.)
    - The clear in step 5 runs before the resume state is read, so a resumed clearing run (`Override`, or `Create` with `clearOnCreate`) deletes
      the records it already imported and then continues from the saved offset. A resumed `Create` run
      that created the table with `clearOnCreate: false` fails instead, because the table now exists.
    - The comment on `AIRTABLE_WRITE_BATCH_SIZE` says "1"; the value is 10.
