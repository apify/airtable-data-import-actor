import { z } from 'zod';
import { log } from 'apify';

/**
 * Zod schemas for validating API responses
 * Ensures type safety at runtime by validating that API responses match expected structure
 */

// Airtable field schema
export const AirtableFieldSchema = z.object({
    id: z.string(),
    name: z.string(),
    type: z.string(),
});

// Airtable table schema
export const AirtableTableSchema = z.object({
    id: z.string(),
    name: z.string(),
    fields: z.array(AirtableFieldSchema),
});

// Airtable schema response (base tables)
export const AirtableSchemaResponseSchema = z.object({
    tables: z.array(AirtableTableSchema),
});

// OAuth account response
export const AirtableOAuthAccountResponseSchema = z.object({
    data: z.object({
        data: z.object({
            access_token: z.string(),
        }),
    }),
});

// WhoAmI response - Airtable API may return different structures
// Making id optional since it might not always be present
export const WhoAmIResponseSchema = z.object({
    id: z.string().optional(),
    scopes: z.array(z.string()).optional(),
});

// Airtable base schema
export const AirtableBaseSchema = z.object({
    id: z.string(),
    name: z.string(),
    permissionLevel: z.string(),
});

// Bases list response
export const AirtableBasesResponseSchema = z.object({
    bases: z.array(AirtableBaseSchema),
    offset: z.string().optional(),
});

// Airtable record response
export const AirtableRecordSchema = z.object({
    id: z.string().optional(),
    fields: z.record(z.string(), z.any()),
});

// Paginated records response (for fetching existing data)
export const AirtablePaginatedRecordsSchema = z.object({
    records: z.array(AirtableRecordSchema),
    offset: z.string().optional(),
});

// Create/update records response
export const AirtableRecordsResponseSchema = z.object({
    records: z.array(AirtableRecordSchema),
    error: z
        .object({
            message: z.string(),
        })
        .optional(),
});

// Delete records response
export const AirtableDeleteResponseSchema = z.object({
    records: z.array(
        z.object({
            id: z.string(),
            deleted: z.boolean(),
        }),
    ),
    error: z
        .object({
            message: z.string(),
        })
        .optional(),
});

// Create table response
export const AirtableCreateTableResponseSchema = z.object({
    id: z.string().optional(),
    name: z.string().optional(),
    error: z
        .object({
            message: z.string(),
        })
        .optional(),
});

/**
 * Helper function to safely parse and validate API responses
 * Throws a descriptive error if validation fails
 */
export function validateResponse<T>(schema: z.ZodSchema<T>, data: unknown, context: string): T {
    const result = schema.safeParse(data);

    if (!result.success) {
        const errorDetails = result.error.issues.map((e: z.ZodIssue) => `${e.path.join('.')}: ${e.message}`).join(', ');
        log.error(`Validation failed for ${context}`, {
            context,
            receivedData: JSON.stringify(data, null, 2),
            validationErrors: errorDetails,
        });
        throw new Error(`Invalid ${context} response structure: ${errorDetails}`);
    }

    return result.data;
}
