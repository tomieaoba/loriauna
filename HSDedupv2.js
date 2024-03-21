const hubspot = require('@hubspot/api-client');

const hubspotClient = new hubspot.Client({ accessToken: process.env.DedupSecret });

let rateLimit = 'NO'; // Initialize outside of the main function to update dynamically

exports.main = async (event, callback) => {
    let hs_execution_state = 'SUCCESS';
    let duplicateFound = 'NO';
    let error = '';
    let mergedContactIDs = [];

    try {
        const triggeringContactId = event.object.objectId.toString();
        const contactProperties = ['phone', 'firstname', 'lastname', 'jobtitle'];
        const contactResult = await handleRateLimit(() => hubspotClient.crm.contacts.basicApi.getById(triggeringContactId, contactProperties));
        
        if (!contactResult || !contactResult.properties) {
            throw new Error('Failed to fetch contact details.');
        }

        const { firstname: firstName, lastname: lastName, phone, jobtitle: jobTitle } = contactResult.properties;

        // Normalize phone number by removing all symbols but keeping the leading "1" if present
        const normalizedPhoneWith1 = phone ? phone.replace(/\D/g, '').replace(/^1?/, '1') : '';
        // Fully normalize phone number by removing all symbols and any leading "1"
        const fullyNormalizedPhone = phone ? phone.replace(/\D/g, '').replace(/^1/, '') : '';

        // Original phone number search
        if (phone) {
            let searchResults = await handleRateLimit(() => searchForDuplicates(firstName, lastName, phone, 'phone', triggeringContactId));
            console.log(`Search results for phone "${phone}":`, JSON.stringify(searchResults, null, 2));
            await processSearchResults(searchResults, triggeringContactId, mergedContactIDs);
        }

        // Normalized phone number with leading "1"
        if (normalizedPhoneWith1 && normalizedPhoneWith1 !== phone) {
            let searchResults = await handleRateLimit(() => searchForDuplicates(firstName, lastName, normalizedPhoneWith1, 'phone', triggeringContactId));
            console.log(`Search results for phone with '1' "${normalizedPhoneWith1}":`, JSON.stringify(searchResults, null, 2));
            await processSearchResults(searchResults, triggeringContactId, mergedContactIDs);
        }

        // Fully normalized phone number
        if (fullyNormalizedPhone && fullyNormalizedPhone !== phone && fullyNormalizedPhone !== normalizedPhoneWith1) {
            let searchResults = await handleRateLimit(() => searchForDuplicates(firstName, lastName, fullyNormalizedPhone, 'phone', triggeringContactId));
            console.log(`Search results for fully normalized phone "${fullyNormalizedPhone}":`, JSON.stringify(searchResults, null, 2));
            await processSearchResults(searchResults, triggeringContactId, mergedContactIDs);
        }

        // Job title search
        if (jobTitle) {
            const searchResults = await handleRateLimit(() => searchForDuplicates(firstName, lastName, jobTitle, 'jobtitle', triggeringContactId));
            console.log(`Search results for job title "${jobTitle}":`, JSON.stringify(searchResults, null, 2));
            await processSearchResults(searchResults, triggeringContactId, mergedContactIDs);
        }

        duplicateFound = mergedContactIDs.length > 0 ? 'YES' : 'NO';

    } catch (e) {
        hs_execution_state = 'ERROR';
        error = e.message || 'An error occurred during execution.';
    } finally {
        callback({
            outputFields: {
                'hs_execution_state': hs_execution_state,
                'DuplicateFound': duplicateFound,
                'Error': error,
                'RateLimit': rateLimit,
                'Merged_Contact_IDs': mergedContactIDs.join(', ')
            }
        });
    }
};

async function searchForDuplicates(firstName, lastName, searchValue, valueType, excludingId) {
    let filters = [
        { propertyName: 'firstname', operator: 'EQ', value: firstName },
        { propertyName: 'lastname', operator: 'EQ', value: lastName },
        { propertyName: valueType, operator: 'EQ', value: searchValue }
    ];

    const searchQuery = {
        filterGroups: [{ filters }],
        properties: ['firstname', 'lastname', 'phone', 'jobtitle']
    };

    try {
        const searchResults = await hubspotClient.crm.contacts.searchApi.doSearch(searchQuery);
        return searchResults;
    } catch (error) {
        console.error('Search Error:', error);
        throw error;
    }
}

async function processSearchResults(searchResults, triggeringContactId, mergedContactIDs) {
    if (searchResults.total > 0) {
        for (const result of searchResults.results) {
            if (result.id !== triggeringContactId) {
                try {
                    const mergeInput = { primaryObjectId: triggeringContactId, objectIdToMerge: result.id };
                        await handleRateLimit(() => hubspotClient.crm.contacts.publicObjectApi.merge(mergeInput));
                        console.log(`Successfully merged contact ID ${result.id} into triggering contact ID ${triggeringContactId}.`);
                        mergedContactIDs.push(result.id);
                    } catch (error) {
                        console.error(`Failed to merge contact ID ${result.id}:`, error.message);
                    }
                }
            }
        }
    }

    async function handleRateLimit(apiCall, attempt = 1, maxAttempts = 8) {
        try {
            const response = await apiCall();
            rateLimit = 'NO'; // Reset rate limit status on successful call
            console.log('API Response:', response); // Log HubSpot's response
            return response;
        } catch (error) {
            console.log('API Error Response:', error.response || error); // Log error response
            if (error.response && error.response.status === 429) {
                rateLimit = 'YES'; // Update rate limit status
                if (attempt > maxAttempts) {
                    console.log(`Max retries exceeded for API call.`);
                    throw new Error('Rate limit exceeded. Max retries attempted.');
                }
                const waitTime = Math.pow(2, attempt) * 1000; // Exponential backoff
                console.log(`Rate limit exceeded. Waiting for ${waitTime / 1000} seconds before retrying...`);
                await new Promise(resolve => setTimeout(resolve, waitTime));
                console.log('Retry after waiting...');
                return await handleRateLimit(apiCall, attempt + 1, maxAttempts); // Increment attempt and retry
            } else {
                throw error; // Rethrow for non-rate limit errors
            }
        }
    }
    
