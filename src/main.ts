import { Actor } from 'apify';
import type { ActorInput } from './types.js';
import { getAirtableClient, fetchWhoAmI, deleteAllRecords, fetchExistingUniqueIds, batchWriteRecords } from './api.js';
import { validateInput, ensureTable, ensureFieldsExist } from './validation.js';
import { mapItemsToAirtableRecords } from './utils.js';

const DATASET_BATCH_SIZE = 50;

await Actor.init();

try {
    const input = (await Actor.getInput()) as ActorInput | null;

    if (!input) {
        throw new Error('No input provided');
    }

    validateInput(input);

    const { operation, base: baseId, table: tableName, datasetId, uniqueId, dataMappings, clearOnCreate } = input;

    const cleanedMappings = dataMappings.filter((m) => m.target && m.target.trim() !== '');

    const airtable = await getAirtableClient(input);

    const whoami = await fetchWhoAmI(airtable);
    console.log('Airtable user:', whoami);

    const tableMeta = await ensureTable(airtable, baseId, tableName, operation, cleanedMappings, clearOnCreate);

    await ensureFieldsExist(airtable, baseId, tableMeta, cleanedMappings);

    const schemaMap: Record<string, string> = {};
    for (const f of tableMeta.fields) {
        schemaMap[f.name] = f.type;
    }

    if (operation === 'override') {
        console.log(`Operation=override: deleting all records in "${tableName}"...`);
        await deleteAllRecords(airtable, baseId, tableName);
        console.log('All records deleted.');
    } else if (operation === 'create' && clearOnCreate === true) {
        console.log(`Operation=create with clearOnCreate=true: deleting all records in "${tableName}"...`);
        await deleteAllRecords(airtable, baseId, tableName);
        console.log('All records deleted.');
    }

    let uniqueIdSet: Set<string> | null = new Set();
    let uniqueTargetField: string | null = null;

    if (uniqueId) {
        const uniqueMapping = cleanedMappings.find((m) => m.source === uniqueId);
        if (uniqueMapping && uniqueMapping.target && uniqueMapping.targetType !== 'new') {
            uniqueTargetField = uniqueMapping.target;
            console.log(
                `Unique ID enabled. Reading existing values from Airtable field "${uniqueTargetField}"...`
            );
            uniqueIdSet = await fetchExistingUniqueIds(airtable, baseId, tableName, uniqueTargetField);
            console.log(`Found ${uniqueIdSet.size} existing unique IDs in Airtable.`);
        } else {
            console.log(
                'uniqueId provided but mapping not found or marked as "new". ' +
                    'Duplicate check will only consider new records within this run.'
            );
        }
    }

    const dataset = await Actor.openDataset(datasetId);
    const datasetInfo = await dataset.getInfo();
    const totalItems = datasetInfo?.itemCount || 0;

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
            uniqueIdSet
        );

        skippedDuplicates += duplicateCount;

        if (!records.length) {
            console.log('No non-duplicate records to import in this batch.');
            continue;
        }

        console.log(`Writing ${records.length} records to Airtable...`);
        const created = await batchWriteRecords(airtable, baseId, tableName, records, schemaMap);
        importedCount += created;
        console.log(`Batch written: ${created} records created.`);
    }

    console.log(
        `Import finished. Imported ${importedCount} records. ` + `Skipped ${skippedDuplicates} duplicates.`
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
    const errorMessage = err instanceof Error ? err.message : String(err);
    await Actor.fail(errorMessage);
} finally {
    await Actor.exit();
}
