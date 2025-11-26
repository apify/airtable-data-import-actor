export const getAirtableClient = async (input) => {
    const accountId = input['oAuthAccount.PxEIzE8praQReTn24'];
    const headers = { Authorization: `Bearer ${process.env.APIFY_TOKEN}` };
    const res = await fetch(`${process.env.APIFY_API_BASE_URL}v2/actor-oauth-accounts/${accountId}`, { headers });
    const account = (await res.json());
    const { access_token } = account.data.data;
    return {
        token: access_token,
        fetch: (url, opts = {}) => fetch(url, {
            ...opts,
            headers: {
                Authorization: `Bearer ${access_token}`,
                ...(opts.headers || {}),
            },
        }),
    };
};
export const fetchBaseSchema = async (airtable, baseId) => {
    const url = `https://api.airtable.com/v0/meta/bases/${baseId}/tables`;
    const res = await airtable.fetch(url);
    const json = (await res.json());
    return json.tables || [];
};
export const findTable = (tables, identifier) => {
    if (!identifier)
        return null;
    const idLower = identifier.trim().toLowerCase();
    return (tables.find((t) => t.id.toLowerCase() === idLower ||
        t.name.trim().toLowerCase() === idLower) || null);
};
export const createTableIfSupported = async (_airtable, baseId, tableName) => {
    throw new Error(`Table "${tableName}" does not exist in base "${baseId}". ` +
        `Creating tables via the Airtable REST API is not generally available. ` +
        `Please create the table manually in Airtable, or enable the schema mutation API ` +
        `and implement createTableIfSupported().`);
};
export const fetchWhoAmI = async (airtable) => {
    const res = await airtable.fetch('https://api.airtable.com/v0/meta/whoami');
    return (await res.json());
};
export const fetchExistingUniqueIds = async (airtable, baseId, tableName, uniqueTargetField) => {
    if (!uniqueTargetField)
        return new Set();
    const baseUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
    let offset;
    const values = new Set();
    do {
        const url = new URL(baseUrl);
        url.searchParams.set('pageSize', '100');
        if (offset)
            url.searchParams.set('offset', offset);
        url.searchParams.set('fields[]', uniqueTargetField);
        const res = await airtable.fetch(url.toString(), { method: 'GET' });
        const json = await res.json();
        for (const record of json.records || []) {
            const v = record.fields?.[uniqueTargetField];
            if (v !== undefined && v !== null) {
                const normalized = String(v).trim().toLowerCase();
                if (normalized)
                    values.add(normalized);
            }
        }
        offset = json.offset;
    } while (offset);
    return values;
};
export const deleteAllRecords = async (airtable, baseId, tableName) => {
    const baseUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
    let offset;
    let totalDeleted = 0;
    console.log(`🗑️ Starting full delete for "${tableName}"...`);
    do {
        const url = new URL(baseUrl);
        url.searchParams.set('pageSize', '100');
        if (offset)
            url.searchParams.set('offset', offset);
        const res = await airtable.fetch(url.toString(), { method: 'GET' });
        const json = await res.json();
        const ids = (json.records || []).map((r) => r.id);
        if (!ids.length) {
            console.log('No more records found, delete complete.');
            break;
        }
        console.log(`Found ${ids.length} records in this page to delete...`);
        for (let i = 0; i < ids.length; i += 10) {
            const batch = ids.slice(i, i + 10);
            console.log(`➡️ Deleting batch of ${batch.length} records…`, batch);
            const deleteRes = await airtable.fetch(baseUrl, {
                method: 'DELETE',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    records: batch.map((id) => ({ id })),
                }),
            });
            const deleteJson = await deleteRes.json();
            if (deleteJson.error) {
                console.error('❌ Airtable delete error:', JSON.stringify(deleteJson, null, 2));
                throw new Error(deleteJson.error.message || JSON.stringify(deleteJson.error));
            }
            const deletedThisBatch = (deleteJson.records || []).length;
            console.log(`✅ Deleted ${deletedThisBatch}/${batch.length} records in this batch.`);
            if (deletedThisBatch !== batch.length) {
                console.warn(`⚠️ WARNING: Airtable deleted fewer records than expected. ` +
                    `Expected ${batch.length}, deleted ${deletedThisBatch}.`);
            }
            totalDeleted += deletedThisBatch;
        }
        offset = json.offset;
    } while (offset);
    console.log(`🧹 Total deleted from "${tableName}": ${totalDeleted} records.`);
    return totalDeleted;
};
export const batchWriteRecords = async (airtable, baseId, tableName, records, schemaMap) => {
    const baseUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
    let created = 0;
    for (let i = 0; i < records.length; i += 1) {
        const rawBatch = records.slice(i, i + 1);
        const batch = rawBatch.map((r) => ({
            fields: normalizeRecordFields(r.fields, schemaMap),
        }));
        const res = await airtable.fetch(baseUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ records: batch }),
        });
        const json = await res.json();
        if (json.error) {
            console.log(batch);
            console.error('Airtable error:', JSON.stringify(json, null, 2));
            throw new Error(json.error.message || JSON.stringify(json.error));
        }
        created += (json.records || []).length;
    }
    return created;
};
const normalizeRecordFields = (fields, schemaMap) => {
    const out = {};
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
const normalizeCellValue = (value, valueType, targetFieldType) => {
    const MAX_STRING_LENGTH = 10000;
    if (value === undefined || value === null)
        return null;
    if (valueType === targetFieldType)
        return value;
    switch (targetFieldType) {
        case 'number': {
            if (typeof value === 'string') {
                const parsed = Number(value.trim());
                return Number.isNaN(parsed) ? null : parsed;
            }
            if (typeof value === 'number')
                return value;
            if (typeof value === 'boolean')
                return value ? 1 : 0;
            return null;
        }
        case 'checkbox': {
            if (typeof value === 'boolean')
                return value;
            if (typeof value === 'string') {
                const lower = value.toLowerCase().trim();
                if (['true', 'yes', '1', 'on'].includes(lower))
                    return true;
                if (['false', 'no', '0', 'off', ''].includes(lower))
                    return false;
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
//# sourceMappingURL=api.js.map