# Airtable Data Import Actor

Import data from Apify datasets directly into Airtable with flexible field mapping, duplicate detection, and automatic table/field creation.

## What does this Actor do?

This Actor imports data from any Apify dataset into your Airtable base. Perfect for storing web scraping results, building automated data pipelines, and creating structured databases from scraped data.

## Key Features

- **Flexible Import Modes**: Append, override, or create tables automatically
- **Smart Field Mapping**: Map dataset fields to Airtable columns with dot notation support (e.g., `product.details.price`)
- **Duplicate Detection**: Skip records that already exist based on a unique identifier
- **Automatic Field Creation**: Create new Airtable fields on the fly
- **Batch Processing**: Efficiently handles large datasets with automatic batching

## Setup

### 1. Connect Airtable Account

Authenticate with Airtable using OAuth in the Apify integration settings.

### 2. Configure Import Settings

#### Required Fields

- **Airtable Base**: Base ID (e.g., `appXXXXXXXXXXXXXX`) or base name (e.g., `My Base`)
    - **By ID**: Find the base ID in your Airtable URL
    - **By Name**: Use the exact base name (case-insensitive matching)
- **Airtable Table Name**: Name of the target table (e.g., `Products`, `Contacts`)
- **Apify Dataset ID**: Source dataset ID

    **⚠️ IMPORTANT FOR INTEGRATIONS:** When setting up this Actor as an integration to run after another Actor, use the following variable for the Dataset ID field:

    ```
    {{resource.defaultDatasetId}}
    ```

    This automatically uses the output dataset from the previous Actor in your workflow.

- **Import Operation**:
    - `Append` - Add new records (keeps existing data)
    - `Override` - Delete all records first, then import
    - `Create` - Create table if it doesn't exist

- **Field Mappings**: Map source fields to Airtable columns
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

#### Optional Fields

- **Unique ID Source Field**: Field name for duplicate detection (e.g., `url`, `productId`)
- **Clear on Create**: Clear existing data when table already exists in `Create` mode

### Field Mapping Guide

Each mapping requires:

- **source**: Dataset field name (supports dot notation: `contact.email`)
- **target**: Airtable column name
- **targetType**: `existing` (field exists) or `new` (create if missing)
- **fieldType**: `singleLineText`, `multilineText`, `number`, or `checkbox`

## Example: E-commerce Product Import

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

## Common Use Cases

- **E-commerce Monitoring**: Store competitor prices and product data
- **Lead Generation**: Import contact information with duplicate prevention
- **Real Estate**: Maintain property listings from multiple sources
- **Content Aggregation**: Collect articles, posts, or reviews

## Tips

- Always use a unique identifier field (`uniqueId`) when appending to prevent duplicates
- Use `singleLineText` for most text fields, `multilineText` for descriptions
- The Actor processes datasets in batches of 1000 items automatically
- Check Actor logs for detailed progress and error information

## Output

Returns a summary with imported count, duplicates skipped, and operation details.

## Integration Workflow Example

1. Run a web scraper Actor to collect data
2. Set this Actor as an integration with `datasetId: {{resource.defaultDatasetId}}`
3. Data automatically flows from scraper → Airtable

## Technical Details

- **Runtime**: Node.js 18+
- **Batch Size**: 1000 items
- **API**: Uses official Airtable Web API with rate limiting
