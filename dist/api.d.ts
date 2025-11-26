import type { ActorInput, AirtableClient, AirtableTable, AirtableRecord, WhoAmIResponse } from './types.js';
export declare const getAirtableClient: (input: ActorInput) => Promise<AirtableClient>;
export declare const fetchBaseSchema: (airtable: AirtableClient, baseId: string) => Promise<AirtableTable[]>;
export declare const findTable: (tables: AirtableTable[], identifier: string | undefined) => AirtableTable | null;
export declare const createTableIfSupported: (_airtable: AirtableClient, baseId: string, tableName: string) => Promise<void>;
export declare const fetchWhoAmI: (airtable: AirtableClient) => Promise<WhoAmIResponse>;
export declare const fetchExistingUniqueIds: (airtable: AirtableClient, baseId: string, tableName: string, uniqueTargetField: string) => Promise<Set<string>>;
export declare const deleteAllRecords: (airtable: AirtableClient, baseId: string, tableName: string) => Promise<number>;
export declare const batchWriteRecords: (airtable: AirtableClient, baseId: string, tableName: string, records: AirtableRecord[], schemaMap: Record<string, string>) => Promise<number>;
//# sourceMappingURL=api.d.ts.map