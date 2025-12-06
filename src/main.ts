import { Actor, log } from 'apify';
import type { ActorInput, ActorOutput } from './types.js';
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

const startTime = new Date().toISOString();

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

    log.info(`🚀 Starting import: ${operation} → ${tableName}`);

    const airtableClient = await getAirtableClient(input);

    const whoami = await fetchWhoAmI(airtableClient);
    log.info(`✓ Authenticated as ${whoami.id}`);

    // Resolve base name to ID if necessary
    const baseInfo = await resolveBaseId(airtableClient, baseIdentifier);
    const baseId = baseInfo.id;
    const baseName = baseInfo.name;

    const tableMeta = await ensureTable(airtableClient, baseId, tableName, operation, cleanedMappings, clearOnCreate);
    log.info(`✓ Table ready: ${tableMeta.fields.length} fields`);

    await ensureFieldsExist(airtableClient, baseId, tableMeta, cleanedMappings);

    const schemaMap: Record<string, string> = {};
    for (const f of tableMeta.fields) {
        schemaMap[f.name] = f.type;
    }

    let clearedRecords = 0;
    if (operation === 'Override' || (operation === 'Create' && clearOnCreate === true)) {
        log.info('🗑️  Clearing existing records...');
        clearedRecords = await deleteAllRecords(airtableClient, baseId, tableName);
        log.info(`✓ Cleared ${clearedRecords} records`);
    }

    let uniqueIdSet: Set<string> | null = new Set();
    let uniqueTargetField: string | null = null;

    if (uniqueId) {
        const uniqueMapping = cleanedMappings.find((mapping) => mapping.source === uniqueId);
        if (uniqueMapping && uniqueMapping.target) {
            uniqueTargetField = uniqueMapping.target;
            log.info(`🔍 Checking duplicates via "${uniqueTargetField}"...`);
            uniqueIdSet = await fetchExistingUniqueIds(airtableClient, baseId, tableName, uniqueTargetField);
            log.info(`✓ Found ${uniqueIdSet.size} existing records`);
        } else {
            log.warning(`⚠️  No mapping for uniqueId "${uniqueId}" - skipping duplicate check`);
        }
    }

    const dataset = await Actor.openDataset(datasetId);
    const datasetInfo = await dataset.getInfo();
    const totalItems = datasetInfo?.itemCount || 0;

    log.info(`📦 Processing ${totalItems} items with ${cleanedMappings.length} mappings`);

    let importedCount = 0;
    let skippedDuplicates = 0;

    for (let offset = 0; offset < totalItems; offset += DATASET_BATCH_SIZE) {
        const limit = Math.min(DATASET_BATCH_SIZE, totalItems - offset);
        const batchEnd = Math.min(offset + limit, totalItems);
        log.info(`Processing ${offset + 1}-${batchEnd}/${totalItems}...`);

        const { items } = await dataset.getData({ offset, limit });

        if (!items || !items.length) continue;

        const { records, duplicateCount } = mapItemsToAirtableRecords(items, cleanedMappings, uniqueId, uniqueIdSet);

        skippedDuplicates += duplicateCount;

        if (!records.length) {
            log.info('↷ Skipped (all duplicates)');
            continue;
        }

        const created = await batchWriteRecords(airtableClient, baseId, tableName, records, schemaMap);
        importedCount += created;

        // Charge for each record imported to Airtable
        if (created > 0) {
            await Actor.charge({ eventName: 'import-to-airtable', count: created });
        }

        const msg = duplicateCount > 0 ? ` (${duplicateCount} duplicates skipped)` : '';
        log.info(`✓ Created ${created} records${msg}`);
    }

    log.info('✅ Import complete!', {
        imported: importedCount,
        skipped: skippedDuplicates,
        total: totalItems,
    });

    const endTime = new Date().toISOString();
    const duration = (new Date(endTime).getTime() - new Date(startTime).getTime()) / 1000;

    const output: ActorOutput = {
        success: true,
        operation,
        baseId,
        baseName,
        tableName,
        datasetId,
        totalItems,
        importedCount,
        skippedDuplicates,
        clearedRecords: clearedRecords > 0 ? clearedRecords : undefined,
        uniqueIdField: uniqueId,
        uniqueIdTargetField: uniqueTargetField || undefined,
        mappingsCount: cleanedMappings.length,
        clearOnCreate,
        airtableUser: whoami,
        startTime,
        endTime,
        duration,
    };

    await Actor.pushData(output);
} catch (err) {
    log.error('❌ Import failed', {
        error: err instanceof Error ? err.message : String(err),
        stack: err instanceof Error ? err.stack : undefined,
    });
    const errorMessage = err instanceof Error ? err.message : String(err);
    await Actor.fail(errorMessage);
} finally {
    await Actor.exit();
}
