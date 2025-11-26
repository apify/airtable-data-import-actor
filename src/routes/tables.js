/**
 * Route handler to fetch available tables for a selected Airtable base
 * This is used to populate the "table" dropdown dynamically in the Actor input UI
 */
export default async function handler(req, res) {
    try {
        // Get the OAuth account ID and base ID from query parameters
        const oAuthAccountId = req.query['oAuthAccount.4NisUztj4uOTblL9i'];
        const baseId = req.query.base;

        if (!oAuthAccountId) {
            return res.status(400).json({
                error: 'Missing oAuthAccount.4NisUztj4uOTblL9i parameter',
            });
        }

        if (!baseId) {
            return res.status(400).json({
                error: 'Missing base parameter',
            });
        }

        // Fetch the OAuth account details from Apify
        const apifyToken = process.env.APIFY_TOKEN;
        if (!apifyToken) {
            return res.status(500).json({
                error: 'APIFY_TOKEN not configured',
            });
        }

        const accountResponse = await fetch(
            `${process.env.APIFY_API_BASE_URL}v2/actor-oauth-accounts/${oAuthAccountId}`,
            {
                headers: { Authorization: `Bearer ${apifyToken}` },
            },
        );

        if (!accountResponse.ok) {
            return res.status(accountResponse.status).json({
                error: 'Failed to fetch OAuth account',
            });
        }

        const accountData = await accountResponse.json();
        const accessToken = accountData.data.data.access_token;

        // Fetch tables from Airtable
        const tablesResponse = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
            headers: {
                Authorization: `Bearer ${accessToken}`,
            },
        });

        if (!tablesResponse.ok) {
            return res.status(tablesResponse.status).json({
                error: 'Failed to fetch tables from Airtable',
            });
        }

        const tablesData = await tablesResponse.json();
        const tables = tablesData.tables || [];

        // Format response for Apify select field
        // Return array of objects with "value" and "label" properties
        const options = tables.map((table) => ({
            value: table.id,
            label: table.name,
        }));

        return res.json(options);
    } catch (error) {
        console.error('Error in /tables route:', error);
        return res.status(500).json({
            error: error.message || 'Internal server error',
        });
    }
}
