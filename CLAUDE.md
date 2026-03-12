# CLAUDE.md

## Project Purpose

An Apify Actor that imports data from Apify datasets directly into Airtable. Supports flexible field mapping, duplicate detection, automatic table/field creation, and three import modes (Append, Override, Create).

## Repository Structure

```
.actor/
├── actor.json          # Actor metadata, version, dataset output schema
├── input_schema.json   # Input validation and Apify Console form definition
└── output_schema.json  # Output schema definition
src/
├── main.ts             # Entry point: orchestrates the import pipeline
├── api.ts              # Airtable API calls (auth, base/table/record operations)
├── validation.ts       # Input validation, ensureTable, ensureFieldsExist
├── utils.ts            # mapItemsToAirtableRecords and field transformation
├── schemas.ts          # Zod schemas for input/output
├── types.ts            # TypeScript type definitions
└── constants.ts        # Batch sizes, rate limit config, API URLs
.github/workflows/
└── claude-md-maintenance.yml  # Auto-updates this file on push to main/master
Dockerfile
AGENTS.md               # Apify Actor development guide for AI agents
```

## Technology Stack

- **Runtime**: Node.js ≥ 18, TypeScript (ESM)
- **Apify SDK**: `apify` ^3.4.2, `crawlee` ^3.13.8
- **Airtable SDK**: `airtable` ^0.12.2
- **Validation**: `zod` ^4.1.13
- **Tooling**: `tsx` (dev runner), `tsc` (build), ESLint (`@apify/eslint-config`), Prettier

## Build, Test & Run

```bash
npm install          # Install dependencies
npm run start        # Run locally with tsx (dev)
npm run build        # Compile TypeScript to dist/
npm run lint         # Lint with ESLint
npm run lint:fix     # Auto-fix lint issues
npm run format       # Format with Prettier
apify run            # Run Actor locally via Apify CLI
apify push           # Deploy to Apify platform
```

> No tests are currently configured (`npm test` exits with an error).

## Conventions

- **ESM modules**: `"type": "module"` — use `.js` extensions in imports even for `.ts` source files.
- **Batch processing**: Dataset items are fetched in batches of 50 (`DATASET_BATCH_SIZE`). Airtable writes/deletes use batches of 10.
- **Rate limiting**: 200ms base delay between Airtable requests; SDK handles exponential backoff.
- **State persistence**: Uses `Actor.useState('import-progress', ...)` to resume from offset on migration/restart.
- **Field mapping**: Supports dot-notation source paths (e.g., `contact.email`). Mappings with empty `target` are filtered out before processing.
- **OAuth**: Airtable authentication uses the OAuth account field `oAuthAccount.BpW1howJtlI9fdEck`.

## Key Notes for AI Assistants

- **No tests**: Do not attempt to run `npm test` — it is intentionally unimplemented.
- **Import extensions**: All local imports in `src/` use `.js` extensions (required for Node ESM); do not change to `.ts`.
- **Airtable field types**: Only `singleLineText`, `multilineText`, `number`, and `checkbox` are supported for new field creation.
- **String truncation**: Field values are capped at `MAX_STRING_LENGTH` (10,000 chars) before writing.
- **CLAUDE.md maintenance**: This file is automatically updated by `.github/workflows/claude-md-maintenance.yml` (calls `apify/workflows` reusable workflow) on every push to `main`/`master`. Do not manually rewrite this file wholesale — update surgically.
- **Deployment**: `apify push` requires prior `apify login`. Ask the user before deploying.
