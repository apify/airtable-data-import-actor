/**
 * Route handler to fetch available Airtable bases for the authenticated OAuth account
 * This is used to populate the "base" dropdown dynamically in the Actor input UI
 */
export default async function handler(req, res) {
    try {
        // Get the OAuth account ID from the query parameters
        const oAuthAccountId = req.query['oAuthAccount.4NisUztj4uOTblL9i'];

        if (!oAuthAccountId) {
            return res.status(400).json({
                error: 'Missing oAuthAccount.4NisUztj4uOTblL9i parameter',
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

        // Fetch bases from Airtable
        const basesResponse = await fetch('https://api.airtable.com/v0/meta/bases', {
            headers: {
                Authorization: `Bearer ${accessToken}`,
            },
        });

        if (!basesResponse.ok) {
            return res.status(basesResponse.status).json({
                error: 'Failed to fetch bases from Airtable',
            });
        }

        const basesData = await basesResponse.json();
        const bases = basesData.bases || [];

        // Format response for Apify select field
        // Return array of objects with "value" and "label" properties
        const options = bases.map((base) => ({
            value: base.id,
            label: base.name,
        }));

        return res.json(options);
    } catch (error) {
        console.error('Error in /bases route:', error);
        return res.status(500).json({
            error: error.message || 'Internal server error',
        });
    }
}
