# Airtable Data Import Actor

Import data from Apify datasets directly into Airtable with flexible field mapping, duplicate detection, and automatic table/field creation.

## Use the Apify Extension for Airtable

**For the best experience, we recommend using the [Apify Extension for Airtable](https://docs.apify.com/platform/integrations/airtable)**. The extension provides:

- **Visual field mapping UI**: Map dataset fields directly within Airtable
- **Automatic field matching**: Matches source and target fields
- **Run Actors from Airtable**: Execute Actors and tasks without leaving your base
- **OAuth integration**: Simple authentication flow

Install it directly in Airtable: **Tools > Extensions > Search "Apify"**

[Learn more about the Apify-Airtable integration →](https://docs.apify.com/platform/integrations/airtable)

---

## What does this Actor do?

This Actor imports data from any Apify dataset into your Airtable base. Perfect for storing web scraping results, building automated data pipelines, and creating structured databases from scraped data.

## Key features

- **Flexible import modes**: Append, override, or create tables automatically
- **Smart field mapping**: Map dataset fields to Airtable columns with dot notation support (e.g., `product.details.price`)
- **Duplicate detection**: Skip records that already exist based on a unique identifier
- **Automatic field creation**: Create new Airtable fields on the fly
- **Batch processing**: Handles large datasets with automatic batching

## Setup

### 1. Connect Airtable account

Authenticate with Airtable using OAuth in the Apify integration settings.

### 2. Configure import settings

#### Required fields

- **Airtable base**: Base ID (e.g., `appXXXXXXXXXXXXXX`) or base name (e.g., `My Base`)
    - **By ID**: Find the base ID in your Airtable URL
    - **By name**: Use the exact base name (case-insensitive matching)
- **Airtable table name**: Name of the target table (e.g., `Products`, `Contacts`)
- **Apify dataset ID**: Source dataset ID

    **⚠️ IMPORTANT FOR INTEGRATIONS:** When setting up this Actor as an integration to run after another Actor, use the following variable for the Dataset ID field:

    ```
    {{resource.defaultDatasetId}}
    ```

    This automatically uses the output dataset from the previous Actor in your workflow.

- **Import operation**:
    - `Append` - Add new records (keeps existing data)
    - `Override` - Delete all records first, then import
    - `Create` - Create table if it doesn't exist

- **Field mappings**: Map source fields to Airtable columns
    ```json
    [
        {
            "source": "title",
            "target": "Product Name",
            "targetType": "existing",
            "fieldType": "singleLineText"
        },
        {
            "source": "price",
            "target": "Price",
            "targetType": "new",
            "fieldType": "number"
        }
    ]
    ```

#### Optional fields

- **Unique ID source field**: Field name for duplicate detection (e.g., `url`, `productId`)
- **Clear existing table data**: Clear existing data when table already exists in `Create` mode

### Field mapping guide

Each mapping requires:

- **source**: Dataset field name (supports dot notation: `contact.email`)
- **target**: Airtable column name
- **targetType**: `existing` (field exists) or `new` (create if missing)
- **fieldType**: `singleLineText`, `multilineText`, `number`, or `checkbox`

## Example: E-commerce product import

```json
{
    "operation": "Append",
    "base": "appABC123456789",
    "table": "Products",
    "datasetId": "{{resource.defaultDatasetId}}",
    "uniqueId": "url",
    "dataMappings": [
        {
            "source": "title",
            "target": "Product Name",
            "targetType": "existing",
            "fieldType": "singleLineText"
        },
        {
            "source": "price",
            "target": "Price",
            "targetType": "existing",
            "fieldType": "number"
        },
        {
            "source": "url",
            "target": "URL",
            "targetType": "existing",
            "fieldType": "singleLineText"
        }
    ]
}
```

## Tips

- Always use a unique identifier field (`uniqueId`) when appending to prevent duplicates
- Use `singleLineText` for most text fields, `multilineText` for descriptions
- The Actor processes datasets in batches of 1000 items automatically
- Check Actor logs for detailed progress and error information

## Output

Returns a summary with imported count, duplicates skipped, and operation details.

## Integration workflow example

1. Run a web scraper Actor to collect data
2. Set this Actor as an integration with `datasetId: {{resource.defaultDatasetId}}`
3. Data automatically flows from scraper → Airtable
