import { Actor } from 'apify';
import type { ActorInput } from './types.js';
import {
    getAirtableClient,
    fetchWhoAmI,
    resolveBaseId,
    deleteAllRecords,
    fetchExistingUniqueIds,
    batchWriteRecords,
} from './api.js';
import { validateInput, ensureTable, ensureFieldsExist } from './validation.js';
import { mapItemsToAirtableRecords } from './utils.js';
import { DATASET_BATCH_SIZE } from './constants.js';

await Actor.init();

try {
    const input = (await Actor.getInput()) as ActorInput | null;

    if (!input) {
        throw new Error('No input provided');
    }

    validateInput(input);

    const {
        operation,
        base: baseIdentifier,
        table: tableName,
        datasetId,
        uniqueId,
        dataMappings,
        clearOnCreate,
    } = input;

    const cleanedMappings = dataMappings.filter((m) => m.target && m.target.trim() !== '');

    console.log(`🚀 Starting import: ${operation} → ${tableName}`);

    const airtable = await getAirtableClient(input);

    const whoami = await fetchWhoAmI(airtable);
    console.log(`✓ Authenticated as ${whoami.id}`);

    // Resolve base name to ID if necessary
    const baseId = await resolveBaseId(airtable, baseIdentifier);

    const tableMeta = await ensureTable(airtable, baseId, tableName, operation, cleanedMappings, clearOnCreate);
    console.log(`✓ Table ready: ${tableMeta.fields.length} fields`);

    await ensureFieldsExist(airtable, baseId, tableMeta, cleanedMappings);

    const schemaMap: Record<string, string> = {};
    for (const f of tableMeta.fields) {
        schemaMap[f.name] = f.type;
    }

    if (operation === 'override') {
        console.log(`🗑️  Clearing existing records...`);
        const deletedCount = await deleteAllRecords(airtable, baseId, tableName);
        console.log(`✓ Cleared ${deletedCount} records`);
    } else if (operation === 'create' && clearOnCreate === true) {
        console.log(`🗑️  Clearing existing records...`);
        const deletedCount = await deleteAllRecords(airtable, baseId, tableName);
        console.log(`✓ Cleared ${deletedCount} records`);
    }

    let uniqueIdSet: Set<string> | null = new Set();
    let uniqueTargetField: string | null = null;

    if (uniqueId) {
        const uniqueMapping = cleanedMappings.find((mapping) => mapping.source === uniqueId);
        if (uniqueMapping && uniqueMapping.target) {
            uniqueTargetField = uniqueMapping.target;
            console.log(`🔍 Checking duplicates via "${uniqueTargetField}"...`);
            uniqueIdSet = await fetchExistingUniqueIds(airtable, baseId, tableName, uniqueTargetField);
            console.log(`✓ Found ${uniqueIdSet.size} existing records`);
        } else {
            console.log(`⚠️  No mapping for uniqueId "${uniqueId}" - skipping duplicate check`);
        }
    }

    const dataset = await Actor.openDataset(datasetId);
    const datasetInfo = await dataset.getInfo();
    const totalItems = datasetInfo?.itemCount || 0;

    console.log(`📦 Processing ${totalItems} items with ${cleanedMappings.length} mappings`);

    let importedCount = 0;
    let skippedDuplicates = 0;

    for (let offset = 0; offset < totalItems; offset += DATASET_BATCH_SIZE) {
        const limit = Math.min(DATASET_BATCH_SIZE, totalItems - offset);
        const batchEnd = Math.min(offset + limit, totalItems);
        console.log(`Processing ${offset + 1}-${batchEnd}/${totalItems}...`);

        const { items } = await dataset.getData({ offset, limit });

        if (!items || !items.length) continue;

        const { records, duplicateCount } = mapItemsToAirtableRecords(items, cleanedMappings, uniqueId, uniqueIdSet);

        skippedDuplicates += duplicateCount;

        if (!records.length) {
            console.log(`↷ Skipped (all duplicates)`);
            continue;
        }

        const created = await batchWriteRecords(airtable, baseId, tableName, records, schemaMap);
        importedCount += created;

        // Charge for each record imported to Airtable
        if (created > 0) {
            await Actor.charge({ eventName: 'import-to-airtable', count: created });
        }

        const msg = duplicateCount > 0 ? ` (${duplicateCount} duplicates skipped)` : '';
        console.log(`✓ Created ${created} records${msg}`);
    }

    console.log(`\n✅ Import complete!`);
    console.log(`   Imported: ${importedCount}`);
    console.log(`   Skipped: ${skippedDuplicates}`);
    console.log(`   Total: ${totalItems}`);

    await Actor.pushData({
        importedCount,
        skippedDuplicates,
        baseId,
        tableName,
        operation,
        airtableUser: whoami,
    });
} catch (err) {
    console.error('\n❌ Import failed');
    console.error(err instanceof Error ? err.message : String(err));
    if (err instanceof Error && err.stack) {
        console.error('\nStack trace:', err.stack);
    }
    const errorMessage = err instanceof Error ? err.message : String(err);
    await Actor.fail(errorMessage);
} finally {
    await Actor.exit();
}
