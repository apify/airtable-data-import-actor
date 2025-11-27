import { Actor } from 'apify';
import type { ActorInput } from './types.js';
import { getAirtableClient, fetchWhoAmI, deleteAllRecords, fetchExistingUniqueIds, batchWriteRecords } from './api.js';
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

    const { operation, base: baseId, table: tableName, datasetId, uniqueId, dataMappings, clearOnCreate } = input;

    const cleanedMappings = dataMappings.filter((m) => m.target && m.target.trim() !== '');

    console.log(`Starting Airtable import - Operation: ${operation}, Base: ${baseId}, Table: ${tableName}`);

    const airtable = await getAirtableClient(input);

    const whoami = await fetchWhoAmI(airtable);
    console.log(`Authenticated as Airtable user: ${whoami.id} (scopes: ${whoami.scopes.join(', ')})`);

    const tableMeta = await ensureTable(airtable, baseId, tableName, operation, cleanedMappings, clearOnCreate);
    console.log(`Table verified: "${tableMeta.name}" (ID: ${tableMeta.id}) with ${tableMeta.fields.length} fields`);

    await ensureFieldsExist(airtable, baseId, tableMeta, cleanedMappings);

    const schemaMap: Record<string, string> = {};
    for (const f of tableMeta.fields) {
        schemaMap[f.name] = f.type;
    }

    if (operation === 'override') {
        console.log(`Operation is "override" - clearing all existing records in table "${tableName}"`);
        const deletedCount = await deleteAllRecords(airtable, baseId, tableName);
        console.log(`Cleared ${deletedCount} existing records before import`);
    } else if (operation === 'create' && clearOnCreate === true) {
        console.log(
            `Operation is "create" with clearOnCreate=true - clearing all existing records in table "${tableName}"`,
        );
        const deletedCount = await deleteAllRecords(airtable, baseId, tableName);
        console.log(`Cleared ${deletedCount} existing records before import`);
    }

    let uniqueIdSet: Set<string> | null = new Set();
    let uniqueTargetField: string | null = null;

    if (uniqueId) {
        const uniqueMapping = cleanedMappings.find((mapping) => mapping.source === uniqueId);
        if (uniqueMapping && uniqueMapping.target) {
            uniqueTargetField = uniqueMapping.target;
            console.log(`Duplicate detection enabled using field "${uniqueTargetField}" (source: "${uniqueId}")`);
            console.log(`Fetching existing unique IDs from Airtable...`);
            uniqueIdSet = await fetchExistingUniqueIds(airtable, baseId, tableName, uniqueTargetField);
            console.log(`Loaded ${uniqueIdSet.size} existing unique IDs for duplicate detection`);
        } else {
            console.log(
                `Warning: uniqueId "${uniqueId}" specified but no matching mapping found. ` +
                    `Duplicate detection will only apply to records within this import run.`,
            );
        }
    }

    const dataset = await Actor.openDataset(datasetId);
    const datasetInfo = await dataset.getInfo();
    const totalItems = datasetInfo?.itemCount || 0;

    console.log(`Opening dataset "${datasetId}" with ${totalItems} items`);
    console.log(`Starting import process with ${cleanedMappings.length} field mappings`);

    let importedCount = 0;
    let skippedDuplicates = 0;

    for (let offset = 0; offset < totalItems; offset += DATASET_BATCH_SIZE) {
        const limit = Math.min(DATASET_BATCH_SIZE, totalItems - offset);
        const batchEnd = Math.min(offset + limit, totalItems);
        console.log(`Processing dataset batch: items ${offset + 1}-${batchEnd} of ${totalItems}`);

        const { items } = await dataset.getData({ offset, limit });

        if (!items || !items.length) continue;

        const { records, duplicateCount } = mapItemsToAirtableRecords(items, cleanedMappings, uniqueId, uniqueIdSet);

        skippedDuplicates += duplicateCount;

        if (!records.length) {
            console.log(`Batch ${offset + 1}-${batchEnd}: No records to import (all duplicates or empty)`);
            continue;
        }

        console.log(
            `Batch ${offset + 1}-${batchEnd}: Writing ${records.length} records to Airtable (${duplicateCount} duplicates skipped)`,
        );
        const created = await batchWriteRecords(airtable, baseId, tableName, records, schemaMap);
        importedCount += created;
        console.log(`Batch ${offset + 1}-${batchEnd}: Successfully created ${created} records`);
    }

    console.log(`\n=== Import Summary ===`);
    console.log(`Total records imported: ${importedCount}`);
    console.log(`Total duplicates skipped: ${skippedDuplicates}`);
    console.log(`Dataset items processed: ${totalItems}`);
    console.log(`Target: Base "${baseId}", Table "${tableName}"`);

    await Actor.pushData({
        importedCount,
        skippedDuplicates,
        baseId,
        tableName,
        operation,
        airtableUser: whoami,
    });
} catch (err) {
    console.error('\n=== Actor Failed ===');
    console.error('Error:', err instanceof Error ? err.message : String(err));
    if (err instanceof Error && err.stack) {
        console.error('Stack trace:', err.stack);
    }
    const errorMessage = err instanceof Error ? err.message : String(err);
    await Actor.fail(errorMessage);
} finally {
    await Actor.exit();
}
