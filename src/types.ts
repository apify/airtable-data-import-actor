import type Airtable from 'airtable';

export type OperationType = 'Append' | 'Override' | 'Create';

export type AirtableFieldType =
    | 'singleLineText'
    | 'multilineText'
    | 'number'
    | 'checkbox'
    | 'email'
    | 'url'
    | 'date'
    | 'phoneNumber';

export type TargetType = 'new' | 'existing';

export interface DataMapping {
    source: string;
    target: string;
    targetType: TargetType;
    fieldType: AirtableFieldType;
}

export interface ActorInput {
    'oAuthAccount.BpW1howJtlI9fdEck': string;
    operation: OperationType;
    clearOnCreate?: boolean;
    base: string;
    table: string;
    datasetId: string;
    uniqueId?: string;
    dataMappings: DataMapping[];
}

export interface AirtableClient {
    token: string;
    sdk: Airtable; // Airtable SDK instance - handles retries for record operations
}

export interface AirtableField {
    id: string;
    name: string;
    type: string;
}

export interface AirtableTable {
    id: string;
    name: string;
    fields: AirtableField[];
}

export interface AirtableRecord {
    id?: string;
    fields: Record<string, any>;
}

export interface AirtableSchemaResponse {
    tables: AirtableTable[];
}

export interface AirtableOAuthAccountResponse {
    data: {
        data: {
            access_token: string;
        };
    };
}

export interface WhoAmIResponse {
    id?: string;
    scopes?: string[];
}

export interface AirtableBase {
    id: string;
    name: string;
    permissionLevel: string;
}

export interface AirtableBasesResponse {
    bases: AirtableBase[];
    offset?: string;
}

export interface MappingResult {
    records: AirtableRecord[];
    duplicateCount: number;
}

export interface ActorOutput {
    success: boolean;
    operation: OperationType;
    baseId: string;
    baseName?: string;
    tableName: string;
    datasetId: string;
    totalItems: number;
    importedCount: number;
    skippedDuplicates: number;
    clearedRecords?: number;
    uniqueIdField?: string;
    uniqueIdTargetField?: string;
    mappingsCount: number;
    clearOnCreate?: boolean;
    airtableUser: WhoAmIResponse;
    startTime: string;
    endTime: string;
    duration: number;
}
