import { Actor } from 'apify';

const DATASET_BATCH_SIZE = 50; // as requested

// Very small helper to call Airtable
const getAirtableClient = async (input) => {
    const accountId = input['oAuthAccount.PxEIzE8praQReTn24'];

    const headers = { Authorization: `Bearer ${process.env.APIFY_TOKEN}` };
    const res = await fetch(`${process.env.APIFY_API_BASE_URL}v2/actor-oauth-accounts/${accountId}`, { headers });
    const account = await res.json();

    const { access_token } = account.data.data;  // OAuth tokens stored by Apify

    return {
        token: access_token,
        fetch: (url, opts = {}) =>
            fetch(url, {
                ...opts,
                headers: {
                    Authorization: `Bearer ${access_token}`,
                    ...(opts.headers || {}),
                }
            })
    };
};

// Helper: read Airtable base schema (tables + fields)
const fetchBaseSchema = async (airtable, baseId) => {
    const url = `https://api.airtable.com/v0/meta/bases/${baseId}/tables`;
    const res = await airtable.fetch(url);
    const json = await res.json();
    return json.tables || [];
};

// Find table by name (case-insensitive)
const findTable = (tables, identifier) => {
    if (!identifier) return null;

    const idLower = identifier.trim().toLowerCase();
    return tables.find((t) =>
        t.id.toLowerCase() === idLower ||
        t.name.trim().toLowerCase() === idLower
    ) || null;
};

// NOTE: REST Airtable as of mid-2024 doesn’t have GA support for creating tables/fields,
// only metadata read APIs. Extensions SDK does (createTableAsync, etc.), but here we are
// on the REST API. So this function is written as a placeholder / example of where you’d
// call the schema-mutation API if your account has access to it.
const createTableIfSupported = async (airtable, baseId, tableName) => {
    // TODO: Implement using Airtable schema mutation API if enabled for your account.
    // For now we just throw a friendly error so behavior is explicit.
    throw new Error(
        `Table "${tableName}" does not exist in base "${baseId}". ` +
        `Creating tables via the Airtable REST API is not generally available. ` +
        `Please create the table manually in Airtable, or enable the schema mutation API ` +
        `and implement createTableIfSupported().`,
    );
};

// Ensure table exists according to operation
const ensureTable = async (airtable, baseId, tableNameOrId, operation) => {
    const tables = await fetchBaseSchema(airtable, baseId);

    let table = findTable(tables, tableNameOrId);

    if (!table && operation === 'create') {
        // FIX IS HERE
        await createTableIfSupported(airtable, baseId, tableNameOrId);

        const newTables = await fetchBaseSchema(airtable, baseId);
        table = findTable(newTables, tableNameOrId);
    }

    if (!table) {
        // FIX IS HERE TOO
        throw new Error(
            `Table "${tableNameOrId}" was not found in base "${baseId}". ` +
            `If you intend to create it, use operation "create" and implement createTableIfSupported().`,
        );
    }

    return table;
};

// Create missing fields requested in dataMappings
const ensureFieldsExist = async (airtable, baseId, table, dataMappings) => {
    const existingFieldsByName = new Map();
    for (const f of table.fields) {
        existingFieldsByName.set(f.name, f);
    }

    const missingFields = [];

    for (const mapping of dataMappings) {
        const { target, targetType, fieldType } = mapping;
        const exists = existingFieldsByName.has(target);

        if (!exists) {
            // If targetType=existing but field not found -> hard error (schema mismatch)
            if (targetType === 'existing') {
                throw new Error(
                    `Mapping expects existing Airtable field "${target}" in table "${table.name}", ` +
                    `but it does not exist. Please create it in Airtable or mark the mapping as "new".`,
                );
            }

            // targetType=new and field not found -> schedule field creation
            missingFields.push({ name: target, type: fieldType });
        }
    }

    if (!missingFields.length) return;

    // Place where you’d call Airtable schema mutation API, if available
    // (PATCH /v0/meta/bases/{baseId}/tables or similar).
    // For now we surface a clear error so users understand why fields are missing.
    throw new Error(
        `The following fields are missing in table "${table.name}": ` +
        missingFields.map((f) => `"${f.name}" (${f.type})`).join(', ') +
        `. Airtable REST API does not generally allow creating fields. ` +
        `Please create these fields manually OR enable the schema mutation API and ` +
        `implement field creation logic in ensureFieldsExist().`,
    );
};

// Fetch all existing values in the Airtable column that corresponds to uniqueId mapping target
const fetchExistingUniqueIds = async (airtable, baseId, tableName, uniqueTargetField) => {
    if (!uniqueTargetField) return new Set();

    const baseUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
    let offset;
    const values = new Set();

    do {
        const url = new URL(baseUrl);
        url.searchParams.set('pageSize', '100');
        if (offset) url.searchParams.set('offset', offset);
        // Select only that one field to minimize payload
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

const deleteAllRecords = async (airtable, baseId, tableName) => {
    const baseUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
    let offset;
    let totalDeleted = 0;

    console.log(`🗑️ Starting full delete for "${tableName}"...`);

    do {
        // 1) Fetch a page of records
        const url = new URL(baseUrl);
        url.searchParams.set('pageSize', '100');
        if (offset) url.searchParams.set('offset', offset);

        const res = await airtable.fetch(url.toString(), { method: 'GET' });
        const json = await res.json();

        const ids = (json.records || []).map((r) => r.id);
        if (!ids.length) {
            console.log("No more records found, delete complete.");
            break;
        }

        console.log(`Found ${ids.length} records in this page to delete...`);

        // 2) Delete in batches of 10
        for (let i = 0; i < ids.length; i += 10) {
            const batch = ids.slice(i, i + 10);

            console.log(`➡️ Deleting batch of ${batch.length} records…`, batch);

            const deleteRes = await airtable.fetch(baseUrl, {
                method: 'DELETE',
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    records: batch.map(id => ({ id })),
                }),
            });

            const deleteJson = await deleteRes.json();

            // Validate the response
            if (deleteJson.error) {
                console.error("❌ Airtable delete error:", JSON.stringify(deleteJson, null, 2));
                throw new Error(deleteJson.error.message || JSON.stringify(deleteJson.error));
            }

            const deletedThisBatch = (deleteJson.records || []).length;

            console.log(`✅ Deleted ${deletedThisBatch}/${batch.length} records in this batch.`);

            if (deletedThisBatch !== batch.length) {
                console.warn(
                    `⚠️ WARNING: Airtable deleted fewer records than expected. ` +
                    `Expected ${batch.length}, deleted ${deletedThisBatch}.`
                );
            }

            totalDeleted += deletedThisBatch;
        }

        offset = json.offset;
    } while (offset);

    console.log(`🧹 Total deleted from "${tableName}": ${totalDeleted} records.`);
    return totalDeleted;
};



// Map Apify dataset value to a coarse Airtable type name (string union)
const mapApifyOutputToAirtableType = (value) => {
    if (value === null || value === undefined) return 'singleLineText';

    if (typeof value === 'number') return 'number';
    if (typeof value === 'boolean') return 'checkbox';

    if (typeof value === 'string') {
        return value.length > 255 ? 'multilineText' : 'singleLineText';
    }

    if (Array.isArray(value) || typeof value === 'object') {
        return 'multilineText';
    }

    return 'singleLineText';
};

const MAX_STRING_LENGTH = 10000;

// Normalize JSON value to match desired Airtable field type
const normalizeCellValue = (value, valueType, targetFieldType) => {
    if (value === undefined || value === null) return null;

    if (valueType === targetFieldType) return value;

    switch (targetFieldType) {
        case 'number': {
            if (typeof value === 'string') {
                const parsed = Number(value.trim());
                return Number.isNaN(parsed) ? null : parsed;
            }
            if (typeof value === 'number') return value;
            if (typeof value === 'boolean') return value ? 1 : 0;
            return null;
        }
        case 'checkbox': {
            if (typeof value === 'boolean') return value;
            if (typeof value === 'string') {
                const lower = value.toLowerCase().trim();
                if (['true', 'yes', '1', 'on'].includes(lower)) return true;
                if (['false', 'no', '0', 'off', ''].includes(lower)) return false;
            }
            return Boolean(value);
        }
        case 'singleLineText':
        case 'multilineText': {
            if (typeof value === 'object') {
                const s = JSON.stringify(value, null, 2);
                return s.length > MAX_STRING_LENGTH ? s.slice(0, MAX_STRING_LENGTH) : s;
            }
            const s = String(value);
            return s.length > MAX_STRING_LENGTH ? s.slice(0, MAX_STRING_LENGTH) : s;
        }
        default:
            return null;
    }
};

const getValueAtPath = (obj, path) => {
    return path.split('.').reduce((acc, key) => {
        if (acc == null) return undefined;
        return acc[key];
    }, obj);
};

// Convert dataset items into Airtable record payloads
const mapItemsToAirtableRecords = (items, dataMappings, uniqueId, uniqueIdSet) => {
    const records = [];
    let duplicateCount = 0;

    for (const item of items) {
        let rowId = null;
        if (uniqueId) {
            const idVal = getValueAtPath(item, uniqueId);
            rowId = String(idVal ?? '').trim().toLowerCase();
        }

        if (rowId && uniqueIdSet && uniqueIdSet.has(rowId)) {
            duplicateCount++;
            continue;
        }

        if (rowId && uniqueIdSet) {
            uniqueIdSet.add(rowId);
        }

        const fields = {};
        for (const mapping of dataMappings) {
            const value = getValueAtPath(item, mapping.source);
            const valueType = mapApifyOutputToAirtableType(value);
            const normalized = normalizeCellValue(value, valueType, mapping.fieldType);
            if (normalized !== null && normalized !== undefined) {
                fields[mapping.target] = normalized;
            }
        }

        if (Object.keys(fields).length > 0) {
            records.push({ fields });
        }
    }

    return { records, duplicateCount };
};

const normalizeRecordFields = (fields, schemaMap) => {
    const out = {};

    for (const [fieldName, value] of Object.entries(fields)) {
        const targetType = schemaMap[fieldName];

        if (!targetType) {
            // Airtable does not have this column → skip it automatically
            continue;
        }

        const valueType = typeof value;
        out[fieldName] = normalizeCellValue(value, valueType, targetType);
    }

    return out;
};

// Write records to Airtable in batches (max 10 per request)
const batchWriteRecords = async (airtable, baseId, tableName, records) => {
    const baseUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
    let created = 0;

    for (let i = 0; i < records.length; i += 1) {
            const rawBatch = records.slice(i, i + 1);
            const batch = rawBatch.map(r => ({
                fields: normalizeRecordFields(r.fields, schemaMap)
            }));        
            const res = await airtable.fetch(baseUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ records: batch }),
        });
        const json = await res.json();

        // 🔥 LOG ERRORS
        if (json.error) {
            console.log(batch)
            console.error("Airtable error:", JSON.stringify(json, null, 2));
            throw new Error(json.error.message || JSON.stringify(json.error));
        }

        created += (json.records || []).length;
    }

    return created;
};

await Actor.init();

let schemaMap;

try {
    const input = await Actor.getInput() || {};
    const {
        operation,
        base: baseId,
        table: tableName,
        datasetId,
        uniqueId,
        dataMappings = [],
    } = input;

    const cleanedMappings = dataMappings.filter(m => m.target && m.target.trim() !== "");

    if (!operation || !['append', 'override', 'create'].includes(operation)) {
        throw new Error('Input "operation" must be one of: append | override | create.');
    }
    if (!baseId) throw new Error('Input "base" (Airtable base ID) is required.');
    if (!tableName) throw new Error('Input "table" (Airtable table name) is required.');
    if (!datasetId) throw new Error('Input "datasetId" is required.');
    if (!Array.isArray(cleanedMappings) || cleanedMappings.length === 0) {
        throw new Error('Input "dataMappings" must be a non-empty array.');
    }

    const airtable = await getAirtableClient(input);

    // Optional: log whoami so user can confirm which Airtable account is used
    const whoamiRes = await airtable.fetch('https://api.airtable.com/v0/meta/whoami');
    const whoami = await whoamiRes.json();
    console.log('Airtable user:', whoami);

    // 1) Get or create table depending on operation
    const tableMeta = await ensureTable(airtable, baseId, tableName, operation);

    // 2) Ensure fields from dataMappings exist (or at least fail explicitly)
    await ensureFieldsExist(airtable, baseId, tableMeta, cleanedMappings);

    schemaMap = {};
    for (const f of tableMeta.fields) {
        schemaMap[f.name] = f.type;   // e.g. "number", "singleLineText", "multilineText", "checkbox"
    }

    // 3) For override: delete all records
    if (operation === 'override') {
        console.log(`Operation=override: deleting all records in "${tableName}"...`);
        await deleteAllRecords(airtable, baseId, tableName);
        console.log('All records deleted.');
    }

    // 4) Duplicate detection setup
    let uniqueIdSet = new Set();
    let uniqueTargetField = null;

    if (uniqueId) {
        const uniqueMapping = cleanedMappings.find((m) => m.source === uniqueId);
        if (uniqueMapping && uniqueMapping.target && uniqueMapping.targetType !== 'new') {
            uniqueTargetField = uniqueMapping.target;
            console.log(
                `Unique ID enabled. Reading existing values from Airtable field "${uniqueTargetField}"...`,
            );
            uniqueIdSet = await fetchExistingUniqueIds(
                airtable,
                baseId,
                tableName,
                uniqueTargetField,
            );
            console.log(`Found ${uniqueIdSet.size} existing unique IDs in Airtable.`);
        } else {
            console.log(
                'uniqueId provided but mapping not found or marked as "new". ' +
                'Duplicate check will only consider new records within this run.',
            );
        }
    }

    // 5) Read dataset in 50-item batches and import
    const dataset = await Actor.openDataset(datasetId);
    const datasetInfo = await dataset.getInfo();
    const totalItems = datasetInfo.itemCount || 0;

    console.log(`Dataset "${datasetId}" contains ${totalItems} items.`);

    let importedCount = 0;
    let skippedDuplicates = 0;

    for (let offset = 0; offset < totalItems; offset += DATASET_BATCH_SIZE) {
        const limit = Math.min(DATASET_BATCH_SIZE, totalItems - offset);
        console.log(`Processing dataset batch offset=${offset} limit=${limit}...`);

        const { items } = await dataset.getData({ offset, limit });

        if (!items || !items.length) continue;

        const { records, duplicateCount } = mapItemsToAirtableRecords(
            items,
            cleanedMappings,
            uniqueId,
            uniqueIdSet,
        );

        skippedDuplicates += duplicateCount;

        if (!records.length) {
            console.log('No non-duplicate records to import in this batch.');
            continue;
        }

        console.log(`Writing ${records.length} records to Airtable...`);
        const created = await batchWriteRecords(airtable, baseId, tableName, records);
        importedCount += created;
        console.log(`Batch written: ${created} records created.`);
    }

    console.log(
        `Import finished. Imported ${importedCount} records. ` +
        `Skipped ${skippedDuplicates} duplicates.`,
    );

    await Actor.pushData({
        importedCount,
        skippedDuplicates,
        baseId,
        tableName,
        operation,
        airtableUser: whoami,
    });
} catch (err) {
    console.error('Actor failed:', err);
    await Actor.fail(err);
} finally {
    await Actor.exit();
}