import Airtable from 'airtable';
import type {
    ActorInput,
    AirtableClient,
    AirtableTable,
    AirtableRecord,
    WhoAmIResponse,
    AirtableBasesResponse,
} from './types.js';
import {
    OAUTH_ACCOUNT_FIELD,
    AIRTABLE_PAGE_SIZE,
    AIRTABLE_DELETE_BATCH_SIZE,
    AIRTABLE_WRITE_BATCH_SIZE,
    AIRTABLE_BASE_DELAY_MS,
    AIRTABLE_RETRY_DELAYS_MS,
} from './constants.js';
import { normalizeCellValue } from './utils.js';
import {
    AirtableOAuthAccountResponseSchema,
    AirtableSchemaResponseSchema,
    WhoAmIResponseSchema,
    AirtableBasesResponseSchema,
    AirtableCreateTableResponseSchema,
    validateResponse,
} from './schemas.js';

/**
 * Sleep utility for delays
 */
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Fetch wrapper with exponential backoff retry logic for rate limiting and errors
 */
const fetchWithRetry = async (url: string, opts: RequestInit = {}): Promise<Response> => {
    const maxAttempts = AIRTABLE_RETRY_DELAYS_MS.length + 1;
    let attemptCount = 0;

    while (attemptCount < maxAttempts) {
        attemptCount++;

        try {
            const res = await fetch(url, opts);

            // Handle rate limiting (HTTP 429)
            if (res.status === 429) {
                if (attemptCount < maxAttempts) {
                    const retryDelay = AIRTABLE_RETRY_DELAYS_MS[attemptCount - 1];
                    console.log(`⏳ Rate limited (429), retrying in ${retryDelay}ms (attempt ${attemptCount}/${maxAttempts})...`);
                    await sleep(retryDelay);
                    continue;
                } else {
                    throw new Error('Rate limit exceeded after all retry attempts');
                }
            }

            // If response is not OK and it's a server error (5xx), retry
            if (!res.ok && res.status >= 500 && res.status < 600) {
                if (attemptCount < maxAttempts) {
                    const retryDelay = AIRTABLE_RETRY_DELAYS_MS[attemptCount - 1];
                    console.log(`⏳ Server error (${res.status}), retrying in ${retryDelay}ms (attempt ${attemptCount}/${maxAttempts})...`);
                    await sleep(retryDelay);
                    continue;
                } else {
                    // Return the response even on last attempt so caller can handle the error
                    return res;
                }
            }

            // Success or client error (4xx) - return response
            return res;
        } catch (error) {
            // Network error or other fetch failure
            if (attemptCount < maxAttempts) {
                const retryDelay = AIRTABLE_RETRY_DELAYS_MS[attemptCount - 1];
                console.log(`⏳ Network error, retrying in ${retryDelay}ms (attempt ${attemptCount}/${maxAttempts})...`);
                await sleep(retryDelay);
            } else {
                throw error;
            }
        }
    }

    // Should never reach here, but TypeScript needs a return
    throw new Error('Unexpected error in fetchWithRetry');
};

/**
 * Creates an Airtable client with OAuth authentication from Apify Actor input
 * Uses the official Airtable.js SDK with automatic retry logic for rate limiting and errors
 */
export const getAirtableClient = async (input: ActorInput): Promise<AirtableClient> => {
    const accountId = input[OAUTH_ACCOUNT_FIELD];

    const headers = { Authorization: `Bearer ${process.env.APIFY_TOKEN}` };
    const res = await fetch(`${process.env.APIFY_API_BASE_URL}v2/actor-oauth-accounts/${accountId}`, { headers });
    const rawData = await res.json();
    const account = validateResponse(AirtableOAuthAccountResponseSchema, rawData, 'OAuth account');

    const { access_token } = account.data.data;

    // Configure Airtable.js client with retry logic
    const airtableSDK = new Airtable({
        apiKey: access_token,
        requestTimeout: 30000, // 30 seconds
    });

    return {
        token: access_token,
        sdk: airtableSDK,
        fetch: (url: string, opts: RequestInit = {}) =>
            fetchWithRetry(url, {
                ...opts,
                headers: {
                    Authorization: `Bearer ${access_token}`,
                    ...(opts.headers || {}),
                },
            }),
    };
};

/**
 * Fetches the complete schema (tables and fields) for an Airtable base
 */
export const fetchBaseSchema = async (airtable: AirtableClient, baseId: string): Promise<AirtableTable[]> => {
    const url = `https://api.airtable.com/v0/meta/bases/${baseId}/tables`;
    const res = await airtable.fetch(url);
    const rawData = await res.json();
    const validated = validateResponse(AirtableSchemaResponseSchema, rawData, 'base schema');
    return validated.tables || [];
};

/**
 * Finds a table in the schema by ID or name (case-insensitive)
 */
export const findTable = (tables: AirtableTable[], identifier: string | undefined): AirtableTable | null => {
    if (!identifier) return null;

    const idLower = identifier.trim().toLowerCase();
    return tables.find((t) => t.id.toLowerCase() === idLower || t.name.trim().toLowerCase() === idLower) || null;
};

/**
 * Creates a new table in Airtable with the specified fields
 * If table name conflicts, retries with a timestamped name
 */
export const createTable = async (
    airtable: AirtableClient,
    baseId: string,
    tableName: string,
    fields: Array<{ name: string; type: string }>,
): Promise<void> => {
    const path = `https://api.airtable.com/v0/meta/bases/${baseId}/tables`;

    // Map field types to include required options
    const mappedFields = fields.map((f) => {
        const field: any = { name: f.name, type: f.type };

        // Add empty options object for types that require it
        if (f.type === 'number') {
            field.options = { precision: 0 };
        } else if (f.type === 'singleLineText' || f.type === 'multilineText') {
            // Text fields don't require options
        } else if (f.type === 'checkbox') {
            field.options = { icon: 'check', color: 'greenBright' };
        } else if (f.type === 'email' || f.type === 'url' || f.type === 'phoneNumber') {
            // These don't require options
        } else if (f.type === 'date') {
            field.options = { dateFormat: { name: 'iso' } };
        }

        return field;
    });

    const payload = {
        name: tableName,
        fields: mappedFields,
    };

    const res = await airtable.fetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });

    const rawData = await res.json();
    const validated = validateResponse(AirtableCreateTableResponseSchema, rawData, 'create table');

    if (!res.ok || validated.error) {
        throw new Error(validated.error?.message || 'Failed to create table');
    }

    console.log(`✓ Created table "${tableName}"`);
};

/**
 * Fetches the authenticated user's information from Airtable
 */
export const fetchWhoAmI = async (airtable: AirtableClient): Promise<WhoAmIResponse> => {
    const res = await airtable.fetch('https://api.airtable.com/v0/meta/whoami');
    const rawData = await res.json();
    return validateResponse(WhoAmIResponseSchema, rawData, 'whoami');
};

/**
 * Lists all bases accessible to the authenticated user
 */
export const listBases = async (airtable: AirtableClient): Promise<AirtableBasesResponse> => {
    const res = await airtable.fetch('https://api.airtable.com/v0/meta/bases');

    if (!res.ok) {
        const errorText = await res.text();
        throw new Error(`Failed to list bases: ${errorText}`);
    }

    const rawData = await res.json();
    return validateResponse(AirtableBasesResponseSchema, rawData, 'bases list');
};

/**
 * Resolves a base identifier (name or ID) to a base ID and name
 * If the input is already a base ID (starts with 'app'), returns it as-is with an optional name lookup
 * Otherwise, searches for a base with the matching name (case-insensitive)
 */
export const resolveBaseId = async (airtable: AirtableClient, baseIdentifier: string): Promise<{ id: string; name?: string }> => {
    const trimmed = baseIdentifier.trim();

    // If it looks like a base ID (starts with 'app'), try to fetch the name
    if (trimmed.startsWith('app')) {
        try {
            const basesResponse = await listBases(airtable);
            const matchingBase = basesResponse.bases.find((base) => base.id === trimmed);
            return { id: trimmed, name: matchingBase?.name };
        } catch (err) {
            // If we can't fetch the name, just return the ID
            return { id: trimmed };
        }
    }

    // Otherwise, fetch all bases and search by name
    console.log(`🔍 Resolving base name "${trimmed}" to base ID...`);
    const basesResponse = await listBases(airtable);

    const normalizedName = trimmed.toLowerCase();
    const matchingBase = basesResponse.bases.find((base) => base.name.trim().toLowerCase() === normalizedName);

    if (!matchingBase) {
        throw new Error(
            `Base "${trimmed}" not found. Available bases: ${basesResponse.bases.map((b) => b.name).join(', ')}`,
        );
    }

    console.log(`✓ Resolved "${trimmed}" to base ID: ${matchingBase.id}`);
    return { id: matchingBase.id, name: matchingBase.name };
};

/**
 * Fetches all existing values for a unique identifier field from an Airtable table
 * Used for duplicate detection during imports
 * Uses Airtable.js SDK for efficient pagination
 */
export const fetchExistingUniqueIds = async (
    airtable: AirtableClient,
    baseId: string,
    tableName: string,
    uniqueTargetField: string,
): Promise<Set<string>> => {
    if (!uniqueTargetField) return new Set();

    const values = new Set<string>();
    const base = airtable.sdk.base(baseId);
    const table = base(tableName);

    await table
        .select({
            fields: [uniqueTargetField],
            pageSize: AIRTABLE_PAGE_SIZE,
        })
        .eachPage((records, fetchNextPage) => {
            for (const record of records) {
                const v = record.get(uniqueTargetField);
                if (v !== undefined && v !== null) {
                    const normalized = String(v).trim().toLowerCase();
                    if (normalized) values.add(normalized);
                }
            }
            fetchNextPage();
        });

    return values;
};

/**
 * Deletes all records from an Airtable table in batches
 * Uses Airtable.js SDK with automatic batch handling
 */
export const deleteAllRecords = async (
    airtable: AirtableClient,
    baseId: string,
    tableName: string,
): Promise<number> => {
    const base = airtable.sdk.base(baseId);
    const table = base(tableName);
    let totalDeleted = 0;

    const recordIds: string[] = [];

    // Fetch all record IDs
    await table
        .select({
            pageSize: AIRTABLE_PAGE_SIZE,
        })
        .eachPage((records, fetchNextPage) => {
            recordIds.push(...records.map((r) => r.id));
            fetchNextPage();
        });

    // Delete in batches
    for (let i = 0; i < recordIds.length; i += AIRTABLE_DELETE_BATCH_SIZE) {
        const batch = recordIds.slice(i, i + AIRTABLE_DELETE_BATCH_SIZE);

        try {
            await table.destroy(batch);
            totalDeleted += batch.length;
        } catch (error: any) {
            throw new Error(`Failed to delete records: ${error.message}`);
        }
    }

    return totalDeleted;
};

/**
 * Writes records to Airtable in batches with rate limiting
 * Uses Airtable.js SDK with automatic error handling and retries
 */
export const batchWriteRecords = async (
    airtable: AirtableClient,
    baseId: string,
    tableName: string,
    records: AirtableRecord[],
    schemaMap: Record<string, string>,
): Promise<number> => {
    const base = airtable.sdk.base(baseId);
    const table = base(tableName);

    let created = 0;
    let skipped = 0;

    for (let i = 0; i < records.length; i += AIRTABLE_WRITE_BATCH_SIZE) {
        const rawBatch = records.slice(i, i + AIRTABLE_WRITE_BATCH_SIZE);
        const batch = rawBatch.map((r) => ({
            fields: normalizeRecordFields(r.fields, schemaMap),
        }));

        // Check if the record has any non-null fields
        const hasValidFields = batch.some((record) => {
            const fieldValues = Object.values(record.fields);
            return fieldValues.some((v) => v !== null && v !== undefined);
        });

        if (!hasValidFields) {
            skipped++;
            continue;
        }

        try {
            const createdRecords = await table.create(batch);
            created += createdRecords.length;
        } catch (error: any) {
            const errorMessage = error.message || String(error);
            throw new Error(`Failed to create records: ${errorMessage}`);
        }

        // Rate limiting: wait before next request (except for the last batch)
        if (i + AIRTABLE_WRITE_BATCH_SIZE < records.length) {
            await sleep(AIRTABLE_BASE_DELAY_MS);
        }
    }

    return created;
};

/**
 * Normalizes record fields to match the Airtable schema types
 */
const normalizeRecordFields = (fields: Record<string, any>, schemaMap: Record<string, string>): Record<string, any> => {
    const out: Record<string, any> = {};

    for (const [fieldName, value] of Object.entries(fields)) {
        const targetType = schemaMap[fieldName];

        if (!targetType) {
            continue;
        }

        const valueType = typeof value;
        out[fieldName] = normalizeCellValue(value, valueType, targetType);
    }

    return out;
};
