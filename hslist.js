const hubspot = require('@hubspot/api-client');

exports.main = async (event, callback) => {
    const listId = '1383'; // List ID
    const accessToken = event.secrets.DedupSecret; // Secret

    const url = `https://api.hubapi.com/contacts/v1/lists/${listId}/contacts/all`;
    const params = new URLSearchParams({
        count: 25, // Max 100
        property: 'firstname,lastname,email,phone', // Specified properties needed for dedup
        propertyMode: 'value_only', // NOTE Can be 'value_only' or 'value_and_history'
        formSubmissionMode: 'newest', // NOTE Can be 'all', 'none', 'newest', 'oldest'
        showListMemberships: false // Indicates whether current list memberships should be fetched
    }).toString();

    try {
        const response = await fetch(`${url}?${params}`, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json'
            }
        });

        if (response.ok) {
            const data = await response.json();
            console.log(`Fetched ${data.contacts.length} contacts from list ID ${listId}.`);
            callback(null, { message: "Fetched contacts successfully.", contacts: data.contacts });
        } else {
            console.error('API request failed:', response.statusText);
            callback(new Error('API request failed'));
        }
    } catch (error) {
        console.error('Failed to fetch contacts:', error);
        callback(error);
    }
};
