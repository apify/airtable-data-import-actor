export type OperationType = 'append' | 'override' | 'create';

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
    'oAuthAccount.4NisUztj4uOTblL9i': string;
    operation: OperationType;
    base: string;
    table: string;
    datasetId: string;
    uniqueId?: string;
    dataMappings: DataMapping[];
}

export interface AirtableClient {
    token: string;
    fetch: (url: string, options?: RequestInit) => Promise<Response>;
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
    id: string;
    scopes: string[];
}

export interface MappingResult {
    records: AirtableRecord[];
    duplicateCount: number;
}
