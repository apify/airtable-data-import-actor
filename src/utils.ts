import type { AirtableFieldType, DataMapping, MappingResult, AirtableRecord } from './types.js';

export const mapApifyOutputToAirtableType = (value: any): AirtableFieldType => {
    if (value === null || value === undefined) return 'singleLineText';

    if (typeof value === 'number') return 'number';
    if (typeof value === 'boolean') return 'checkbox';

    if (typeof value === 'string') {
        return value.length > 255 ? 'multilineText' : 'singleLineText';
    }

    if (Array.isArray(value) || typeof value === 'object') {
        return 'multilineText';
    }

    return 'singleLineText';
};

export const getValueAtPath = (obj: any, path: string): any => {
    return path.split('.').reduce((acc, key) => {
        if (acc == null) return undefined;
        return acc[key];
    }, obj);
};

export const mapItemsToAirtableRecords = (
    items: any[],
    dataMappings: DataMapping[],
    uniqueId: string | undefined,
    uniqueIdSet: Set<string> | null,
): MappingResult => {
    const records: AirtableRecord[] = [];
    let duplicateCount = 0;

    for (const item of items) {
        let rowId: string | null = null;
        if (uniqueId) {
            const idVal = getValueAtPath(item, uniqueId);
            rowId = String(idVal ?? '')
                .trim()
                .toLowerCase();
        }

        if (rowId && uniqueIdSet && uniqueIdSet.has(rowId)) {
            duplicateCount++;
            continue;
        }

        if (rowId && uniqueIdSet) {
            uniqueIdSet.add(rowId);
        }

        const fields: Record<string, any> = {};
        for (const mapping of dataMappings) {
            const value = getValueAtPath(item, mapping.source);
            const valueType = mapApifyOutputToAirtableType(value);
            const normalized = normalizeCellValue(value, valueType, mapping.fieldType);
            if (normalized !== null && normalized !== undefined) {
                fields[mapping.target] = normalized;
            }
        }

        if (Object.keys(fields).length > 0) {
            records.push({ fields });
        }
    }

    return { records, duplicateCount };
};

const normalizeCellValue = (value: any, valueType: string, targetFieldType: string): any => {
    const MAX_STRING_LENGTH = 10000;

    if (value === undefined || value === null) return null;

    if (valueType === targetFieldType) return value;

    switch (targetFieldType) {
        case 'number': {
            if (typeof value === 'string') {
                const parsed = Number(value.trim());
                return Number.isNaN(parsed) ? null : parsed;
            }
            if (typeof value === 'number') return value;
            if (typeof value === 'boolean') return value ? 1 : 0;
            return null;
        }
        case 'checkbox': {
            if (typeof value === 'boolean') return value;
            if (typeof value === 'string') {
                const lower = value.toLowerCase().trim();
                if (['true', 'yes', '1', 'on'].includes(lower)) return true;
                if (['false', 'no', '0', 'off', ''].includes(lower)) return false;
            }
            return Boolean(value);
        }
        case 'singleLineText':
        case 'multilineText': {
            if (typeof value === 'object') {
                const s = JSON.stringify(value, null, 2);
                return s.length > MAX_STRING_LENGTH ? s.slice(0, MAX_STRING_LENGTH) : s;
            }
            const s = String(value);
            return s.length > MAX_STRING_LENGTH ? s.slice(0, MAX_STRING_LENGTH) : s;
        }
        default:
            return null;
    }
};
