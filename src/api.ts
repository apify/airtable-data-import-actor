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
    AirtablePaginatedRecordsSchema,
    AirtableDeleteResponseSchema,
    AirtableRecordsResponseSchema,
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
 * All fetch calls automatically include exponential backoff retry logic for rate limiting and errors
 */
export const getAirtableClient = async (input: ActorInput): Promise<AirtableClient> => {
    const accountId = input[OAUTH_ACCOUNT_FIELD];

    const headers = { Authorization: `Bearer ${process.env.APIFY_TOKEN}` };
    const res = await fetch(`${process.env.APIFY_API_BASE_URL}v2/actor-oauth-accounts/${accountId}`, { headers });
    const rawData = await res.json();
    const account = validateResponse(AirtableOAuthAccountResponseSchema, rawData, 'OAuth account');

    const { access_token } = account.data.data;

    return {
        token: access_token,
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
 */
export const fetchExistingUniqueIds = async (
    airtable: AirtableClient,
    baseId: string,
    tableName: string,
    uniqueTargetField: string,
): Promise<Set<string>> => {
    if (!uniqueTargetField) return new Set();

    const baseUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
    let offset: string | undefined;
    const values = new Set<string>();

    do {
        const url = new URL(baseUrl);
        url.searchParams.set('pageSize', String(AIRTABLE_PAGE_SIZE));
        if (offset) url.searchParams.set('offset', offset);
        url.searchParams.set('fields[]', uniqueTargetField);

        const res = await airtable.fetch(url.toString(), { method: 'GET' });
        const rawData = await res.json();
        const validated = validateResponse(AirtablePaginatedRecordsSchema, rawData, 'paginated records');

        for (const record of validated.records || []) {
            const v = record.fields?.[uniqueTargetField];
            if (v !== undefined && v !== null) {
                const normalized = String(v).trim().toLowerCase();
                if (normalized) values.add(normalized);
            }
        }

        offset = validated.offset;
    } while (offset);

    return values;
};

/**
 * Deletes all records from an Airtable table in batches
 * Respects Airtable's batch size limits (retry logic handled by client)
 */
export const deleteAllRecords = async (
    airtable: AirtableClient,
    baseId: string,
    tableName: string,
): Promise<number> => {
    const baseUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
    let offset: string | undefined;
    let totalDeleted = 0;

    do {
        const url = new URL(baseUrl);
        url.searchParams.set('pageSize', String(AIRTABLE_PAGE_SIZE));
        if (offset) url.searchParams.set('offset', offset);

        const res = await airtable.fetch(url.toString(), { method: 'GET' });
        const rawData = await res.json();
        const validated = validateResponse(AirtablePaginatedRecordsSchema, rawData, 'paginated records');

        const ids = (validated.records || []).map((r) => r.id).filter((id): id is string => id !== undefined);
        if (!ids.length) break;

        for (let i = 0; i < ids.length; i += AIRTABLE_DELETE_BATCH_SIZE) {
            const batch = ids.slice(i, i + AIRTABLE_DELETE_BATCH_SIZE);

            const deleteUrl = new URL(baseUrl);
            batch.forEach((id: string) => {
                deleteUrl.searchParams.append('records[]', id);
            });

            const deleteRes = await airtable.fetch(deleteUrl.toString(), {
                method: 'DELETE',
            });

            if (!deleteRes.ok) {
                const errorText = await deleteRes.text();
                try {
                    const errorJson = JSON.parse(errorText);
                    throw new Error(errorJson.error?.message || JSON.stringify(errorJson.error) || errorText);
                } catch {
                    throw new Error(`HTTP ${deleteRes.status}: ${errorText}`);
                }
            }

            const deleteRawData = await deleteRes.json();
            const deleteValidated = validateResponse(AirtableDeleteResponseSchema, deleteRawData, 'delete records');

            if (deleteValidated.error) {
                throw new Error(deleteValidated.error.message);
            }

            const deletedThisBatch = deleteValidated.records.length;
            totalDeleted += deletedThisBatch;
        }

        offset = validated.offset;
    } while (offset);

    return totalDeleted;
};

/**
 * Writes records to Airtable in batches with rate limiting
 * Retry logic and error handling are automatically handled by the client
 */
export const batchWriteRecords = async (
    airtable: AirtableClient,
    baseId: string,
    tableName: string,
    records: AirtableRecord[],
    schemaMap: Record<string, string>,
): Promise<number> => {
    const baseUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;

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

        const res = await airtable.fetch(baseUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ records: batch }),
        });

        if (!res.ok) {
            const errorText = await res.text();
            try {
                const errorJson = JSON.parse(errorText);
                throw new Error(errorJson.error?.message || JSON.stringify(errorJson.error) || errorText);
            } catch {
                throw new Error(`HTTP ${res.status}: ${errorText}`);
            }
        }

        const rawData = await res.json();
        const validated = validateResponse(AirtableRecordsResponseSchema, rawData, 'create records');

        if (validated.error) {
            throw new Error(validated.error.message);
        }

        const recordsCreated = validated.records.length;

        if (recordsCreated === 0) {
            skipped++;
        } else {
            created += recordsCreated;
        }

        // Rate limiting: wait before next request (except for the last record)
        if (i < records.length - 1) {
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
