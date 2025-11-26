import type { ActorInput, AirtableClient, AirtableTable, DataMapping, OperationType } from './types.js';
export declare const validateInput: (input: ActorInput) => void;
export declare const ensureTable: (airtable: AirtableClient, baseId: string, tableNameOrId: string, operation: OperationType) => Promise<AirtableTable>;
export declare const ensureFieldsExist: (_airtable: AirtableClient, _baseId: string, table: AirtableTable, dataMappings: DataMapping[]) => Promise<void>;
//# sourceMappingURL=validation.d.ts.map