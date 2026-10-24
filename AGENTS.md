# AGENTS.md

`CLAUDE.md` is only an `@AGENTS.md` pointer; put instructions here.
`.github/workflows/agents-md-maintenance.yml` keeps this file accurate inside each PR.

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
storage/                # Local storage (mirrors Cloud during development; gitignored)
├── datasets/           # Output items (JSON objects)
├── key_value_stores/   # Files, config, INPUT
└── request_queues/     # Pending crawl requests
.github/workflows/
└── agents-md-maintenance.yml  # Keeps this file accurate inside each PR
Dockerfile              # Container image definition
AGENTS.md               # AI agent instructions (this file)
CLAUDE.md               # `@AGENTS.md` pointer only
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
- **Deployment**: `apify push` requires prior `apify login`. Ask the user before deploying (see Safety and Permissions).

## Apify Actor development reference

Important: Before you begin, fill in the `generatedBy` property in the meta section of `.actor/actor.json`. Replace it with the model you're currently using. This helps Apify monitor and improve AGENTS.md for specific LLM models.

### What are Apify Actors?

- Actors are serverless programs that run in the cloud. They're inspired by the UNIX philosophy - programs that do one thing well and can be easily combined to build complex systems.
- Actors are programs packaged as Docker images that run in isolated containers

### Core Concepts

- Accept well-defined JSON input
- Perform isolated tasks (web scraping, automation, data processing)
- Produce structured JSON output to datasets and/or store data in key-value stores
- Can run from seconds to hours or even indefinitely
- Persist state and can be restarted

### Do

- accept well-defined JSON input and produce structured JSON output
- use Apify SDK (`apify`) for code running ON Apify platform
- validate input early with proper error handling and fail gracefully
- use CheerioCrawler for static HTML content (10x faster than browsers)
- use PlaywrightCrawler only for JavaScript-heavy sites and dynamic content
- use router pattern (createCheerioRouter/createPlaywrightRouter) for complex crawls
- implement retry strategies with exponential backoff for failed requests
- use proper concurrency settings (HTTP: 10-50, Browser: 1-5)
- set sensible defaults in `.actor/input_schema.json` for all optional fields
- set up output schema in `.actor/output_schema.json`
- clean and validate data before pushing to dataset
- use semantic CSS selectors and fallback strategies for missing elements
- respect robots.txt, ToS, and implement rate limiting with delays
- check which tools (cheerio/playwright/crawlee) are installed before applying guidance

### Don't

- do not rely on `Dataset.getInfo()` for final counts on Cloud platform
- do not use browser crawlers when HTTP/Cheerio works (massive performance gains with HTTP)
- do not hard code values that should be in input schema or environment variables
- do not skip input validation or error handling
- do not overload servers - use appropriate concurrency and delays
- do not scrape prohibited content or ignore Terms of Service
- do not store personal/sensitive data unless explicitly permitted
- do not use deprecated options like `requestHandlerTimeoutMillis` on CheerioCrawler (v3.x)
- do not use `additionalHttpHeaders` - use `preNavigationHooks` instead

### Safety and Permissions

Allowed without prompt:

- read files with `Actor.getValue()`
- push data with `Actor.pushData()`
- set values with `Actor.setValue()`
- enqueue requests to RequestQueue
- run locally with `apify run`

Ask first:

- npm/pip package installations
- apify push (deployment to cloud)
- proxy configuration changes (requires paid plan)
- Dockerfile changes affecting builds
- deleting datasets or key-value stores

### Actor Input Schema

The input schema defines the input parameters for an Actor. It's a JSON object comprising various field types supported by the Apify platform.

#### Structure

```json
{
    "title": "<INPUT-SCHEMA-TITLE>",
    "type": "object",
    "schemaVersion": 1,
    "properties": {
        /* define input fields here */
    },
    "required": []
}
```

#### Example

```json
{
    "title": "E-commerce Product Scraper Input",
    "type": "object",
    "schemaVersion": 1,
    "properties": {
        "startUrls": {
            "title": "Start URLs",
            "type": "array",
            "description": "URLs to start scraping from (category pages or product pages)",
            "editor": "requestListSources",
            "default": [{ "url": "https://example.com/category" }],
            "prefill": [{ "url": "https://example.com/category" }]
        },
        "followVariants": {
            "title": "Follow Product Variants",
            "type": "boolean",
            "description": "Whether to scrape product variants (different colors, sizes)",
            "default": true
        },
        "maxRequestsPerCrawl": {
            "title": "Max Requests per Crawl",
            "type": "integer",
            "description": "Maximum number of pages to scrape (0 = unlimited)",
            "default": 1000,
            "minimum": 0
        },
        "proxyConfiguration": {
            "title": "Proxy Configuration",
            "type": "object",
            "description": "Proxy settings for anti-bot protection",
            "editor": "proxy",
            "default": { "useApifyProxy": false }
        },
        "locale": {
            "title": "Locale",
            "type": "string",
            "description": "Language/country code for localized content",
            "default": "cs",
            "enum": ["cs", "en", "de", "sk"],
            "enumTitles": ["Czech", "English", "German", "Slovak"]
        }
    },
    "required": ["startUrls"]
}
```

### Actor Output Schema

The Actor output schema builds upon the schemas for the dataset and key-value store. It specifies where an Actor stores its output and defines templates for accessing that output. Apify Console uses these output definitions to display run results.

#### Structure

```json
{
    "actorOutputSchemaVersion": 1,
    "title": "<OUTPUT-SCHEMA-TITLE>",
    "properties": {
        /* define your outputs here */
    }
}
```

#### Example

```json
{
    "actorOutputSchemaVersion": 1,
    "title": "Output schema of the files scraper",
    "properties": {
        "files": {
            "type": "string",
            "title": "Files",
            "template": "{{links.apiDefaultKeyValueStoreUrl}}/keys"
        },
        "dataset": {
            "type": "string",
            "title": "Dataset",
            "template": "{{links.apiDefaultDatasetUrl}}/items"
        }
    }
}
```

#### Output Schema Template Variables

- `links` (object) - Contains quick links to most commonly used URLs
- `links.publicRunUrl` (string) - Public run url in format `https://console.apify.com/view/runs/:runId`
- `links.consoleRunUrl` (string) - Console run url in format `https://console.apify.com/actors/runs/:runId`
- `links.apiRunUrl` (string) - API run url in format `https://api.apify.com/v2/actor-runs/:runId`
- `links.apiDefaultDatasetUrl` (string) - API url of default dataset in format `https://api.apify.com/v2/datasets/:defaultDatasetId`
- `links.apiDefaultKeyValueStoreUrl` (string) - API url of default key-value store in format `https://api.apify.com/v2/key-value-stores/:defaultKeyValueStoreId`
- `links.containerRunUrl` (string) - URL of a webserver running inside the run in format `https://<containerId>.runs.apify.net/`
- `run` (object) - Contains information about the run same as it is returned from the `GET Run` API endpoint
- `run.defaultDatasetId` (string) - ID of the default dataset
- `run.defaultKeyValueStoreId` (string) - ID of the default key-value store

### Dataset Schema Specification

The dataset schema defines how your Actor's output data is structured, transformed, and displayed in the Output tab in the Apify Console.

#### Example

Consider an example Actor that calls `Actor.pushData()` to store data into dataset:

```javascript
import { Actor } from 'apify';
// Initialize the JavaScript SDK
await Actor.init();

/**
 * Actor code
 */
await Actor.pushData({
    numericField: 10,
    pictureUrl: 'https://www.google.com/images/branding/googlelogo/2x/googlelogo_color_92x30dp.png',
    linkUrl: 'https://google.com',
    textField: 'Google',
    booleanField: true,
    dateField: new Date(),
    arrayField: ['#hello', '#world'],
    objectField: {},
});

// Exit successfully
await Actor.exit();
```

To set up the Actor's output tab UI, reference a dataset schema file in `.actor/actor.json`:

```json
{
    "actorSpecification": 1,
    "name": "book-library-scraper",
    "title": "Book Library Scraper",
    "version": "1.0.0",
    "storages": {
        "dataset": "./dataset_schema.json"
    }
}
```

Then create the dataset schema in `.actor/dataset_schema.json`:

```json
{
    "actorSpecification": 1,
    "fields": {},
    "views": {
        "overview": {
            "title": "Overview",
            "transformation": {
                "fields": [
                    "pictureUrl",
                    "linkUrl",
                    "textField",
                    "booleanField",
                    "arrayField",
                    "objectField",
                    "dateField",
                    "numericField"
                ]
            },
            "display": {
                "component": "table",
                "properties": {
                    "pictureUrl": {
                        "label": "Image",
                        "format": "image"
                    },
                    "linkUrl": {
                        "label": "Link",
                        "format": "link"
                    },
                    "textField": {
                        "label": "Text",
                        "format": "text"
                    },
                    "booleanField": {
                        "label": "Boolean",
                        "format": "boolean"
                    },
                    "arrayField": {
                        "label": "Array",
                        "format": "array"
                    },
                    "objectField": {
                        "label": "Object",
                        "format": "object"
                    },
                    "dateField": {
                        "label": "Date",
                        "format": "date"
                    },
                    "numericField": {
                        "label": "Number",
                        "format": "number"
                    }
                }
            }
        }
    }
}
```

#### Structure

```json
{
    "actorSpecification": 1,
    "fields": {},
    "views": {
        "<VIEW_NAME>": {
            "title": "string (required)",
            "description": "string (optional)",
            "transformation": {
                "fields": ["string (required)"],
                "unwind": ["string (optional)"],
                "flatten": ["string (optional)"],
                "omit": ["string (optional)"],
                "limit": "integer (optional)",
                "desc": "boolean (optional)"
            },
            "display": {
                "component": "table (required)",
                "properties": {
                    "<FIELD_NAME>": {
                        "label": "string (optional)",
                        "format": "text|number|date|link|boolean|image|array|object (optional)"
                    }
                }
            }
        }
    }
}
```

**Dataset Schema Properties:**

- `actorSpecification` (integer, required) - Specifies the version of dataset schema structure document (currently only version 1)
- `fields` (JSONSchema object, required) - Schema of one dataset object (use JsonSchema Draft 2020-12 or compatible)
- `views` (DatasetView object, required) - Object with API and UI views description

**DatasetView Properties:**

- `title` (string, required) - Visible in UI Output tab and API
- `description` (string, optional) - Only available in API response
- `transformation` (ViewTransformation object, required) - Data transformation applied when loading from Dataset API
- `display` (ViewDisplay object, required) - Output tab UI visualization definition

**ViewTransformation Properties:**

- `fields` (string[], required) - Fields to present in output (order matches column order)
- `unwind` (string[], optional) - Deconstructs nested children into parent object
- `flatten` (string[], optional) - Transforms nested object into flat structure
- `omit` (string[], optional) - Removes specified fields from output
- `limit` (integer, optional) - Maximum number of results (default: all)
- `desc` (boolean, optional) - Sort order (true = newest first)

**ViewDisplay Properties:**

- `component` (string, required) - Only `table` is available
- `properties` (Object, optional) - Keys matching `transformation.fields` with ViewDisplayProperty values

**ViewDisplayProperty Properties:**

- `label` (string, optional) - Table column header
- `format` (string, optional) - One of: `text`, `number`, `date`, `link`, `boolean`, `image`, `array`, `object`

### Key-Value Store Schema Specification

The key-value store schema organizes keys into logical groups called collections for easier data management.

#### Example

Consider an example Actor that calls `Actor.setValue()` to save records into the key-value store:

```javascript
import { Actor } from 'apify';
// Initialize the JavaScript SDK
await Actor.init();

/**
 * Actor code
 */
await Actor.setValue('document-1', 'my text data', { contentType: 'text/plain' });

await Actor.setValue(`image-${imageID}`, imageBuffer, { contentType: 'image/jpeg' });

// Exit successfully
await Actor.exit();
```

To configure the key-value store schema, reference a schema file in `.actor/actor.json`:

```json
{
    "actorSpecification": 1,
    "name": "data-collector",
    "title": "Data Collector",
    "version": "1.0.0",
    "storages": {
        "keyValueStore": "./key_value_store_schema.json"
    }
}
```

Then create the key-value store schema in `.actor/key_value_store_schema.json`:

```json
{
    "actorKeyValueStoreSchemaVersion": 1,
    "title": "Key-Value Store Schema",
    "collections": {
        "documents": {
            "title": "Documents",
            "description": "Text documents stored by the Actor",
            "keyPrefix": "document-"
        },
        "images": {
            "title": "Images",
            "description": "Images stored by the Actor",
            "keyPrefix": "image-",
            "contentTypes": ["image/jpeg"]
        }
    }
}
```

#### Structure

```json
{
    "actorKeyValueStoreSchemaVersion": 1,
    "title": "string (required)",
    "description": "string (optional)",
    "collections": {
        "<COLLECTION_NAME>": {
            "title": "string (required)",
            "description": "string (optional)",
            "key": "string (conditional - use key OR keyPrefix)",
            "keyPrefix": "string (conditional - use key OR keyPrefix)",
            "contentTypes": ["string (optional)"],
            "jsonSchema": "object (optional)"
        }
    }
}
```

**Key-Value Store Schema Properties:**

- `actorKeyValueStoreSchemaVersion` (integer, required) - Version of key-value store schema structure document (currently only version 1)
- `title` (string, required) - Title of the schema
- `description` (string, optional) - Description of the schema
- `collections` (Object, required) - Object where each key is a collection ID and value is a Collection object

**Collection Properties:**

- `title` (string, required) - Collection title shown in UI tabs
- `description` (string, optional) - Description appearing in UI tooltips
- `key` (string, conditional) - Single specific key for this collection
- `keyPrefix` (string, conditional) - Prefix for keys included in this collection
- `contentTypes` (string[], optional) - Allowed content types for validation
- `jsonSchema` (object, optional) - JSON Schema Draft 07 format for `application/json` content type validation

Either `key` or `keyPrefix` must be specified for each collection, but not both.

### Apify MCP Tools

If MCP server is configured, use these tools for documentation:

- `search-apify-docs` - Search documentation
- `fetch-apify-docs` - Get full doc pages

Otherwise, reference: `@https://mcp.apify.com/`

### Resources

- [docs.apify.com/llms.txt](https://docs.apify.com/llms.txt) - Quick reference
- [docs.apify.com/llms-full.txt](https://docs.apify.com/llms-full.txt) - Complete docs
- [crawlee.dev](https://crawlee.dev) - Crawlee documentation
- [whitepaper.actor](https://raw.githubusercontent.com/apify/actor-whitepaper/refs/heads/master/README.md) - Complete Actor specification
