import type { AirtableFieldType, DataMapping, MappingResult } from './types.js';
export declare const mapApifyOutputToAirtableType: (value: any) => AirtableFieldType;
export declare const getValueAtPath: (obj: any, path: string) => any;
export declare const mapItemsToAirtableRecords: (items: any[], dataMappings: DataMapping[], uniqueId: string | undefined, uniqueIdSet: Set<string> | null) => MappingResult;
//# sourceMappingURL=utils.d.ts.map