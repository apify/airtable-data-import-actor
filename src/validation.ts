import type { ActorInput, AirtableClient, AirtableTable, DataMapping, OperationType } from './types.js';
import { fetchBaseSchema, findTable, createTableIfSupported } from './api.js';

/**
 * Validates the actor input configuration
 * Throws descriptive errors if any required fields are missing or invalid
 */
export const validateInput = (input: ActorInput): void => {
    const { operation, base, table, datasetId, dataMappings } = input;

    const cleanedMappings = dataMappings.filter((mapping) => mapping.target && mapping.target.trim() !== '');

    if (!operation || !['append', 'override', 'create'].includes(operation)) {
        throw new Error(
            `Invalid operation "${operation || '(empty)'}". Must be one of: "append", "override", or "create".`,
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

    if (!table && operation === 'create') {
        console.log(`Creating table with ${dataMappings.length} fields...`);
        const fields = dataMappings.map((mapping) => ({
            name: mapping.target,
            type: mapping.fieldType,
        }));

        await createTableIfSupported(airtable, baseId, tableNameOrId, fields);

        const newTables = await fetchBaseSchema(airtable, baseId);
        table = findTable(newTables, tableNameOrId);

        if (!table) {
            throw new Error(
                `Failed to create or find table "${tableNameOrId}" in base "${baseId}" after creation attempt. ` +
                    `Please verify your Airtable permissions and base configuration.`,
            );
        }
    } else if (table && operation === 'create') {
        // Table already exists with 'create' operation
        if (clearOnCreate === false) {
            throw new Error(
                `Table "${tableNameOrId}" already exists in base "${baseId}". ` +
                    `Operation is set to "create" and "clearOnCreate" is false. ` +
                    `Either set "clearOnCreate" to true to clear existing data, or use "append" operation instead.`,
            );
        }
        // If clearOnCreate is true or undefined (default behavior), allow it to proceed
        // The data clearing will be handled in main.ts
    }

    if (!table) {
        throw new Error(
            `Table "${tableNameOrId}" was not found in base "${baseId}". ` +
                `Available operations: Use "create" to create a new table, or verify the table name/ID is correct.`,
        );
    }

    return table;
};

/**
 * Validates that all required Airtable fields exist in the table
 * Throws an error if fields marked as "existing" are not found or if new fields cannot be created
 */
export const ensureFieldsExist = async (
    _airtable: AirtableClient,
    _baseId: string,
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

    const fieldsList = missingFields.map((field) => `"${field.name}" (type: ${field.type})`).join(', ');
    throw new Error(
        `Cannot create ${missingFields.length} missing field(s) in table "${table.name}": ${fieldsList}. ` +
            `The Airtable REST API does not generally allow creating fields via API. ` +
            `Solution: Please create these fields manually in Airtable, OR enable the Enterprise schema mutation API ` +
            `and implement field creation logic in this function.`,
    );
};
