import type {
    ActorInput,
    AirtableClient,
    AirtableOAuthAccountResponse,
    AirtableSchemaResponse,
    AirtableTable,
    AirtableRecord,
    WhoAmIResponse,
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

/**
 * Creates an Airtable client with OAuth authentication from Apify Actor input
 */
export const getAirtableClient = async (input: ActorInput): Promise<AirtableClient> => {
    const accountId = input[OAUTH_ACCOUNT_FIELD];

    const headers = { Authorization: `Bearer ${process.env.APIFY_TOKEN}` };
    const res = await fetch(`${process.env.APIFY_API_BASE_URL}v2/actor-oauth-accounts/${accountId}`, { headers });
    const account = (await res.json()) as AirtableOAuthAccountResponse;

    const { access_token } = account.data.data;

    return {
        token: access_token,
        fetch: (url: string, opts: RequestInit = {}) =>
            fetch(url, {
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
    const json = (await res.json()) as AirtableSchemaResponse;
    return json.tables || [];
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
export const createTableIfSupported = async (
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

    try {
        const res = await airtable.fetch(path, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });

        const json = await res.json();

        if (!res.ok || json.error) {
            throw new Error(json.error?.message || 'Failed to create table');
        }

        console.log(`✓ Created table "${tableName}"`);
    } catch (err) {
        // If there's a conflict (table name already exists), try with a timestamped name
        if (err instanceof Error && err.message.includes('name')) {
            const uniqueTableName = `${tableName} ${new Date().toISOString()}`;
            const patchedPayload = { ...payload, name: uniqueTableName };

            const retryRes = await airtable.fetch(path, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(patchedPayload),
            });

            const retryJson = await retryRes.json();

            if (!retryRes.ok || retryJson.error) {
                throw new Error(retryJson.error?.message || 'Failed to create table with unique name');
            }

            console.log(`✓ Created table "${uniqueTableName}"`);
        } else {
            throw err;
        }
    }
};

/**
 * Fetches the authenticated user's information from Airtable
 */
export const fetchWhoAmI = async (airtable: AirtableClient): Promise<WhoAmIResponse> => {
    const res = await airtable.fetch('https://api.airtable.com/v0/meta/whoami');
    return (await res.json()) as WhoAmIResponse;
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
        const json = await res.json();

        for (const record of json.records || []) {
            const v = record.fields?.[uniqueTargetField];
            if (v !== undefined && v !== null) {
                const normalized = String(v).trim().toLowerCase();
                if (normalized) values.add(normalized);
            }
        }

        offset = json.offset;
    } while (offset);

    return values;
};

/**
 * Deletes all records from an Airtable table in batches
 * Respects Airtable's batch size limits and implements retry logic with exponential backoff
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
        const json = await res.json();

        const ids = (json.records || []).map((r: any) => r.id);
        if (!ids.length) break;

        for (let i = 0; i < ids.length; i += AIRTABLE_DELETE_BATCH_SIZE) {
            const batch = ids.slice(i, i + AIRTABLE_DELETE_BATCH_SIZE);

            const deleteUrl = new URL(baseUrl);
            batch.forEach((id: string) => {
                deleteUrl.searchParams.append('records[]', id);
            });

            // Retry logic with exponential backoff
            let attemptCount = 0;
            let deleted = false;
            const maxAttempts = AIRTABLE_RETRY_DELAYS_MS.length + 1;

            while (attemptCount < maxAttempts && !deleted) {
                attemptCount++;

                try {
                    const deleteRes = await airtable.fetch(deleteUrl.toString(), {
                        method: 'DELETE',
                    });

                    if (!deleteRes.ok) {
                        if (deleteRes.status === 429) {
                            // Rate limited
                            if (attemptCount < maxAttempts) {
                                const retryDelay = AIRTABLE_RETRY_DELAYS_MS[attemptCount - 1];
                                await sleep(retryDelay);
                                continue;
                            } else {
                                throw new Error('Rate limit exceeded after all retry attempts');
                            }
                        }

                        const errorText = await deleteRes.text();
                        try {
                            const errorJson = JSON.parse(errorText);
                            throw new Error(errorJson.error?.message || JSON.stringify(errorJson.error) || errorText);
                        } catch {
                            throw new Error(`HTTP ${deleteRes.status}: ${errorText}`);
                        }
                    }

                    const deleteJson = await deleteRes.json();

                    if (deleteJson.error) {
                        throw new Error(deleteJson.error.message || JSON.stringify(deleteJson.error));
                    }

                    const deletedThisBatch = (deleteJson.records || []).length;
                    totalDeleted += deletedThisBatch;
                    deleted = true;
                } catch (error) {
                    if (attemptCount < maxAttempts) {
                        const retryDelay = AIRTABLE_RETRY_DELAYS_MS[attemptCount - 1];
                        await sleep(retryDelay);
                    } else {
                        throw error;
                    }
                }
            }
        }

        offset = json.offset;
    } while (offset);

    return totalDeleted;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Writes records to Airtable one at a time with rate limiting and retry logic
 * Handles rate limits (429) and implements exponential backoff for failed requests
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

        // Try to create the record with retries
        let attemptCount = 0;
        let recordCreated = false;
        const maxAttempts = AIRTABLE_RETRY_DELAYS_MS.length + 1; // Initial attempt + retries

        while (attemptCount < maxAttempts && !recordCreated) {
            attemptCount++;

            try {
                const res = await airtable.fetch(baseUrl, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ records: batch }),
                });

                if (!res.ok) {
                    // Handle rate limiting (HTTP 429)
                    if (res.status === 429) {
                        if (attemptCount < maxAttempts) {
                            const retryDelay = AIRTABLE_RETRY_DELAYS_MS[attemptCount - 1];
                            await sleep(retryDelay);
                            continue;
                        } else {
                            throw new Error('Rate limit exceeded after all retry attempts');
                        }
                    }

                    // Handle other HTTP errors
                    const errorText = await res.text();
                    try {
                        const errorJson = JSON.parse(errorText);
                        throw new Error(errorJson.error?.message || JSON.stringify(errorJson.error) || errorText);
                    } catch {
                        throw new Error(`HTTP ${res.status}: ${errorText}`);
                    }
                }

                const json = await res.json();

                if (json.error) {
                    throw new Error(json.error.message || JSON.stringify(json.error));
                }

                const recordsCreated = (json.records || []).length;

                if (recordsCreated === 0) {
                    // No records created, might be rate limiting or silent rejection
                    if (attemptCount < maxAttempts) {
                        const retryDelay = AIRTABLE_RETRY_DELAYS_MS[attemptCount - 1];
                        await sleep(retryDelay);
                        continue;
                    } else {
                        skipped++;
                        recordCreated = true; // Exit retry loop
                    }
                } else {
                    created += recordsCreated;
                    recordCreated = true; // Success, exit retry loop
                }
            } catch (error) {
                // On error, retry if attempts remain
                if (attemptCount < maxAttempts) {
                    const retryDelay = AIRTABLE_RETRY_DELAYS_MS[attemptCount - 1];
                    await sleep(retryDelay);
                } else {
                    // Max attempts reached, rethrow error
                    throw error;
                }
            }
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
