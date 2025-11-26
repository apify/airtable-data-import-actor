import type { ActorInput, AirtableClient, AirtableTable, DataMapping, OperationType } from './types.js';
import { fetchBaseSchema, findTable, createTableIfSupported } from './api.js';

export const validateInput = (input: ActorInput): void => {
    const { operation, base, table, datasetId, dataMappings } = input;

    const cleanedMappings = dataMappings.filter((m) => m.target && m.target.trim() !== '');

    if (!operation || !['append', 'override', 'create'].includes(operation)) {
        throw new Error('Input "operation" must be one of: append | override | create.');
    }
    if (!base) throw new Error('Input "base" (Airtable base ID) is required.');
    if (!table) throw new Error('Input "table" (Airtable table name) is required.');
    if (!datasetId) throw new Error('Input "datasetId" is required.');
    if (!Array.isArray(cleanedMappings) || cleanedMappings.length === 0) {
        throw new Error('Input "dataMappings" must be a non-empty array.');
    }
};

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
        const fields = dataMappings.map((m) => ({
            name: m.target,
            type: m.fieldType,
        }));

        await createTableIfSupported(airtable, baseId, tableNameOrId, fields);

        const newTables = await fetchBaseSchema(airtable, baseId);
        table = findTable(newTables, tableNameOrId);
    } else if (table && operation === 'create') {
        // Table already exists with 'create' operation
        if (clearOnCreate === false) {
            throw new Error(
                `Table "${tableNameOrId}" already exists in base "${baseId}". ` +
                    `Operation is set to "create" and "clearOnCreate" is false. ` +
                    `Either set "clearOnCreate" to true to clear existing data, or use "append" operation.`,
            );
        }
        // If clearOnCreate is true or undefined (default behavior), allow it to proceed
        // The data clearing will be handled in main.ts
    }

    if (!table) {
        throw new Error(
            `Table "${tableNameOrId}" was not found in base "${baseId}". ` +
                `If you intend to create it, use operation "create".`,
        );
    }

    return table;
};

export const ensureFieldsExist = async (
    _airtable: AirtableClient,
    _baseId: string,
    table: AirtableTable,
    dataMappings: DataMapping[],
): Promise<void> => {
    const existingFieldsByName = new Map<string, boolean>();
    for (const f of table.fields) {
        existingFieldsByName.set(f.name, true);
    }

    const missingFields: Array<{ name: string; type: string }> = [];

    for (const mapping of dataMappings) {
        const { target, targetType, fieldType } = mapping;
        const exists = existingFieldsByName.has(target);

        if (!exists) {
            if (targetType === 'existing') {
                throw new Error(
                    `Mapping expects existing Airtable field "${target}" in table "${table.name}", ` +
                        `but it does not exist. Please create it in Airtable or mark the mapping as "new".`,
                );
            }

            missingFields.push({ name: target, type: fieldType });
        }
    }

    if (!missingFields.length) return;

    throw new Error(
        `The following fields are missing in table "${table.name}": ` +
            missingFields.map((f) => `"${f.name}" (${f.type})`).join(', ') +
            `. Airtable REST API does not generally allow creating fields. ` +
            `Please create these fields manually OR enable the schema mutation API and ` +
            `implement field creation logic in ensureFieldsExist().`,
    );
};
