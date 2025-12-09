import { log } from 'apify';
import type { ActorInput, AirtableClient, AirtableTable, DataMapping, OperationType } from './types.js';
import { AIRTABLE_API_BASE_URL } from './constants.js';
import { fetchBaseSchema, findTable, createTable, fetchWithRetry } from './api.js';

/**
 * Validates the actor input configuration
 * Throws descriptive errors if any required fields are missing or invalid
 */
export const validateInput = (input: ActorInput): void => {
    const { operation, base, table, datasetId, dataMappings } = input;

    const cleanedMappings = dataMappings.filter((mapping) => mapping.target && mapping.target.trim() !== '');

    if (!operation || !['Append', 'Override', 'Create'].includes(operation)) {
        throw new Error(
            `Invalid operation "${operation || '(empty)'}". Must be one of: "Append", "Override", or "Create".`,
        );
    }
    if (!base) {
        throw new Error('Missing required input "base". Please provide an Airtable base ID.');
    }
    if (!table) {
        throw new Error('Missing required input "table". Please provide an Airtable table name or ID.');
    }
    if (!datasetId) {
        throw new Error('Missing required input "datasetId". Please provide an Apify dataset ID to import from.');
    }
    if (!Array.isArray(cleanedMappings) || cleanedMappings.length === 0) {
        throw new Error(
            'Input "dataMappings" must be a non-empty array with at least one valid mapping. ' +
                'Each mapping must have a non-empty "target" field.',
        );
    }
};

/**
 * Ensures a table exists in the Airtable base, creating it if necessary based on operation type
 * Handles the "create" operation logic including table creation and clearOnCreate behavior
 */
export const ensureTable = async (
    airtable: AirtableClient,
    baseId: string,
    tableNameOrId: string,
    operation: OperationType,
    dataMappings: DataMapping[],
    clearOnCreate?: boolean,
): Promise<AirtableTable> => {
    const tables = await fetchBaseSchema(airtable, baseId);

    let table = findTable(tables, tableNameOrId);

    if (!table && operation === 'Create') {
        log.info(`📋 Creating table with ${dataMappings.length} fields...`);
        const fields = dataMappings.map((mapping) => ({
            name: mapping.target,
            type: mapping.fieldType,
        }));

        await createTable(airtable, baseId, tableNameOrId, fields);

        const newTables = await fetchBaseSchema(airtable, baseId);
        table = findTable(newTables, tableNameOrId);

        if (!table) {
            throw new Error(
                `Failed to create or find table "${tableNameOrId}" in base "${baseId}" after creation attempt. ` +
                    `Please verify your Airtable permissions and base configuration.`,
            );
        }
    } else if (table && operation === 'Create') {
        // Table already exists with 'Create' operation
        if (clearOnCreate === false) {
            throw new Error(
                `Table "${tableNameOrId}" already exists in base "${baseId}". ` +
                    `Operation is set to "Create" and "clearOnCreate" is false. ` +
                    `Either set "clearOnCreate" to true to clear existing data, or use "Append" operation instead.`,
            );
        }
        // If clearOnCreate is true or undefined (default behavior), allow it to proceed
        // The data clearing will be handled in main.ts
    }

    if (!table) {
        throw new Error(
            `Table "${tableNameOrId}" was not found in base "${baseId}". ` +
                `Available operations: Use "Create" to create a new table, or verify the table name/ID is correct.`,
        );
    }

    return table;
};

/**
 * Validates that all required Airtable fields exist in the table
 * Creates missing fields if they are marked as "new", throws error if "existing" fields are not found
 */
export const ensureFieldsExist = async (
    airtable: AirtableClient,
    baseId: string,
    table: AirtableTable,
    dataMappings: DataMapping[],
): Promise<void> => {
    const existingFieldsByName = new Map<string, boolean>();
    for (const field of table.fields) {
        existingFieldsByName.set(field.name, true);
    }

    const missingFields: Array<{ name: string; type: string }> = [];

    for (const mapping of dataMappings) {
        const { target, targetType, fieldType } = mapping;
        const exists = existingFieldsByName.has(target);

        if (!exists) {
            if (targetType === 'existing') {
                throw new Error(
                    `Field mapping error: Expected existing Airtable field "${target}" in table "${table.name}", ` +
                        `but it does not exist. Available fields: ${table.fields.map((f) => f.name).join(', ')}. ` +
                        `Solution: Either create the field "${target}" in Airtable, or change the mapping targetType to "new".`,
                );
            }

            missingFields.push({ name: target, type: fieldType });
        }
    }

    if (!missingFields.length) {
        return;
    }

    // Create missing fields using Airtable Meta API
    log.info(`➕ Creating ${missingFields.length} new field(s) in table "${table.name}"...`);

    for (const field of missingFields) {
        const res = await fetchWithRetry(`${AIRTABLE_API_BASE_URL}/v0/meta/bases/${baseId}/tables/${table.id}/fields`, {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${airtable.token}`,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                name: field.name,
                type: field.type,
            }),
        });

        if (!res.ok) {
            const errorText = await res.text();
            throw new Error(
                `Failed to create field "${field.name}" (type: ${field.type}) in table "${table.name}" ` +
                    `(${res.status} ${res.statusText}): ${errorText}`,
            );
        }

        log.info(`✓ Created field "${field.name}" (${field.type})`);
    }
};
