/**
 * Configuration constants for Airtable data import operations
 */

// Dataset processing
export const DATASET_BATCH_SIZE = 50;

// Airtable API rate limiting and batch sizes
export const AIRTABLE_BASE_DELAY_MS = 200; // 5 req/sec = 200ms between requests
export const AIRTABLE_RETRY_DELAYS_MS = [250, 500, 1000]; // Exponential backoff delays
export const AIRTABLE_DELETE_BATCH_SIZE = 10; // Max records to delete per API call
export const AIRTABLE_WRITE_BATCH_SIZE = 10; // Records to write per API call (1 for better error handling)
export const AIRTABLE_PAGE_SIZE = 100; // Max records to fetch per page (Airtable's limit)

// Field value constraints
export const MAX_STRING_LENGTH = 10000;

// OAuth account field name
export const OAUTH_ACCOUNT_FIELD = 'oAuthAccount.4NisUztj4uOTblL9i';
