import express from 'express';
import admin from 'firebase-admin';
import cors from 'cors';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5002;

let serviceAccount;
try {
    const base64EncodedKey = process.env.GOOGLE_APPLICATION_CREDENTIALS_BASE64;
    if (base64EncodedKey) {
        const decodedServiceAccount = Buffer.from(base64EncodedKey, 'base64').toString('utf8');
        serviceAccount = JSON.parse(decodedServiceAccount);
    } else {
        throw new Error("GOOGLE_APPLICATION_CREDENTIALS_BASE64 environment variable is not set.");
    }
} catch (error) {
    console.error("Failed to parse service account key from environment variable:", error);
    process.exit(1);
}

// Initialize Firebase Admin SDK
admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

// Middleware
app.use(cors());
app.use(express.json());

// --- Helper: Format date (existing, still useful for some logging/display) ---
const formatDate = (date) => date.toISOString().split('T')[0];

// --- Helper: Get individual float balance (existing, unchanged) ---
const getIndividualFloatBalance = async (floatType) => {
    try {
        const floatDoc = await db.collection(floatType).doc('current').get();
        if (floatDoc.exists) {
            return floatDoc.data().balance || 0;
        } else {
            console.warn(`Backend: ${floatType}/current document does not exist. Returning 0 float balance.`);
            return 0;
        }
    } catch (error) {
        console.error(`Error fetching ${floatType} float balance (backend):`, error);
        return 0;
    }
};

// --- Helper: Map telco to its float balance document/collection ID (existing, unchanged) ---
function getFloatCollectionId(telco) {
    if (telco === 'Safaricom') {
        return 'Saf_float';
    } else if (telco === 'Airtel' || telco === 'Telkom' || telco === 'Africastalking') {
        return 'AT_Float';
    }
    return null;
}

// --- NEW EAT Date Utility Functions (CRITICAL for accurate sales aggregation) ---
// These functions convert UTC Date objects to Firestore Timestamps representing EAT boundaries.
// Firestore queries will then correctly filter based on EAT days/months.
const getStartOfDayEAT = (date) => {
    const d = new Date(date);
    d.setUTCHours(0, 0, 0, 0); // Start with UTC midnight
    d.setUTCHours(d.getUTCHours() - 3); // Subtract 3 hours for EAT (UTC+3) midnight
    return admin.firestore.Timestamp.fromDate(d);
};

const getEndOfDayEAT = (date) => {
    const d = new Date(date);
    d.setUTCHours(23, 59, 59, 999); // End with UTC end of day
    d.setUTCHours(d.getUTCHours() - 3); // Subtract 3 hours for EAT (UTC+3) end of day
    return admin.firestore.Timestamp.fromDate(d);
};

const getStartOfMonthEAT = (date) => {
    const d = new Date(date.getFullYear(), date.getMonth(), 1); // Get first day of month in local time
    d.setUTCHours(0, 0, 0, 0); // Set to UTC midnight for that day
    d.setUTCHours(d.getUTCHours() - 3); // Adjust for EAT
    return admin.firestore.Timestamp.fromDate(d);
};

// --- NEW / REFACTORED: Centralized Sales Calculation Function ---
// This function will be called by both /dashboard and /sales-overview
const getSalesOverviewData = async () => {
    const telcos = ['Safaricom', 'Airtel', 'Telkom'];
    const sales = {};
    const topPurchasers = {}; // Moved here to be part of the sales overview data

    const today = new Date();
    const yesterday = new Date();
    yesterday.setDate(today.getDate() - 1); // Adjust for yesterday

    // EAT boundaries
    const startOfTodayEAT = getStartOfDayEAT(today);
    const endOfTodayEAT = getEndOfDayEAT(today);
    const startOfYesterdayEAT = getStartOfDayEAT(yesterday);
    const endOfYesterdayEAT = getEndOfDayEAT(yesterday);
    const startOfThisMonthEAT = getStartOfMonthEAT(today);

    for (const telco of telcos) {
        // --- Today's Sales (using createdAt and EAT timestamps) ---
        const todaySalesQuery = db.collection('sales')
            .where('status', '==', 'COMPLETED') // Ensure only completed sales are counted
            .where('carrier', '==', telco) // Assuming 'carrier' field for telco
            .where('createdAt', '>=', startOfTodayEAT)
            .where('createdAt', '<=', endOfTodayEAT);
        const todaySalesSnapshot = await todaySalesQuery.get();
        const todayTotal = todaySalesSnapshot.docs.reduce((sum, doc) => sum + (doc.data().amount || 0), 0);

        // --- Yesterday's Sales (using createdAt and EAT timestamps for trend) ---
        const yesterdaySalesQuery = db.collection('sales')
            .where('status', '==', 'COMPLETED')
            .where('carrier', '==', telco)
            .where('createdAt', '>=', startOfYesterdayEAT)
            .where('createdAt', '<=', endOfYesterdayEAT);
        const yesterdaySalesSnapshot = await yesterdaySalesQuery.get();
        const yesterdayTotal = yesterdaySalesSnapshot.docs.reduce((sum, doc) => sum + (doc.data().amount || 0), 0);

        // --- This Month's Sales (using createdAt and EAT timestamps) ---
        const thisMonthSalesQuery = db.collection('sales')
            .where('status', '==', 'COMPLETED')
            .where('carrier', '==', telco)
            .where('createdAt', '>=', startOfThisMonthEAT);
        const thisMonthSalesSnapshot = await thisMonthSalesQuery.get();
        const monthTotal = thisMonthSalesSnapshot.docs.reduce((sum, doc) => sum + (doc.data().amount || 0), 0);

        // Calculate Trend (Today vs. Yesterday)
        const calculateTrend = (todayVal, yesterdayVal) => {
            if (yesterdayVal === 0) {
                return todayVal > 0 ? 'up' : 'neutral'; // If yesterday was 0, any sales today is 'up'
            }
            return todayVal >= yesterdayVal ? 'up' : 'down';
        };
        const trend = calculateTrend(todayTotal, yesterdayTotal);

        sales[telco] = {
            today: todayTotal,
            month: monthTotal,
            trend,
        };

        // --- Top Purchasers (assuming 'sales' collection still has 'name' and 'amount' for purchasers) ---
        // You'll need to adapt this if your top purchasers are tracked differently.
        // This calculates top purchasers *overall* not just for the month/day.
        const allSalesForTelcoSnap = await db.collection('sales')
            .where('carrier', '==', telco) // Use 'carrier' for consistency
            .where('status', '==', 'COMPLETED')
            .get();
        const purchasers = {};
        allSalesForTelcoSnap.docs.forEach(doc => {
            const { name, amount } = doc.data(); // Ensure 'name' field exists for the purchaser
            if (name && typeof amount === 'number') {
                purchasers[name] = (purchasers[name] || 0) + amount;
            }
        });
        const sortedTop = Object.entries(purchasers)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 3)
            .map(([name, amount]) => ({ name, Amount: amount })); // Ensure 'Amount' matches frontend expectation

        topPurchasers[telco] = sortedTop;
    }

    return { sales, topPurchasers };
};


// --- NEW ENDPOINT: GET /api/analytics/sales-overview ---
app.get('/api/analytics/sales-overview', async (req, res) => {
    try {
        const { sales, topPurchasers } = await getSalesOverviewData(); // Get the sales data
        res.json(sales); // Only return the 'sales' part for this endpoint
    } catch (error) {
        console.error('Error fetching sales overview:', error);
        res.status(500).json({ error: 'Failed to fetch sales overview.' });
    }
});


// Existing Endpoint: POST /api/process-airtime-purchase (unchanged)
app.post('/api/process-airtime-purchase', async (req, res) => {
    const {
        amount,
        status,
        telco, // Renamed from 'carrier' in the frontend to 'telco' here for clarity as per your code
        transactionId,
    } = req.body;

    console.log(`[${new Date().toISOString()}] Received purchase request:`, { amount, status, telco, transactionId });

    if (typeof amount !== 'number' || amount <= 0 || !status || !telco || !transactionId) {
        console.warn(`[${new Date().toISOString()}] Invalid request body for float deduction:`, req.body);
        return res.status(400).json({ success: false, message: 'Invalid purchase data provided. Missing amount, status, telco, or transactionId.' });
    }

    if (status.toUpperCase() !== 'COMPLETED' && status.toUpperCase() !== 'SUCCESS') { // Ensure COMPLETED or SUCCESS
        console.log(`[${new Date().toISOString()}] Transaction ${transactionId} status is ${status}. No float deduction needed.`);
        return res.status(200).json({ success: true, message: `Transaction ${transactionId} was ${status}. No float deduction needed.` });
    }

    const floatCollectionId = getFloatCollectionId(telco);
    if (!floatCollectionId) {
        console.error(`[${new Date().toISOString()}] Unknown or unmapped telco "${telco}" for transaction ${transactionId}. Cannot deduct float.`);
        return res.status(400).json({ success: false, message: `Unknown telco "${telco}". Cannot process float deduction.` });
    }

    const floatDocRef = db.collection(floatCollectionId).doc('current');

    try {
        await db.runTransaction(async (transaction) => {
            const doc = await transaction.get(floatDocRef);

            if (!doc.exists) {
                console.error(`[${new Date().toISOString()}] Float balance document "${floatCollectionId}/current" does not exist!`);
                throw new Error(`Float balance for ${telco} (${floatCollectionId}) not found.`);
            }

            const currentBalance = doc.data().balance;
            if (typeof currentBalance !== 'number') {
                console.error(`[${new Date().toISOString()}] Float balance for ${floatCollectionId} is not a number. Value: ${currentBalance}`);
                throw new Error(`Float balance for ${floatCollectionId} is corrupted or not a number.`);
            }

            const newBalance = currentBalance - amount;

            console.log(`[${new Date().toISOString()}] Processing ${telco} float deduction for transaction ${transactionId}:`);
            console.log(`  Current balance (${floatCollectionId}): ${currentBalance}`);
            console.log(`  Amount to deduct: ${amount}`);
            console.log(`  New balance will be: ${newBalance}`);

            if (newBalance < 0) {
                console.error(`[${new Date().toISOString()}] Insufficient float for ${telco}! Current: ${currentBalance}, Deduction: ${amount}`);
                throw new Error(`Insufficient float balance for ${telco}. Current: ${currentBalance.toLocaleString()}, Needed: ${amount.toLocaleString()}.`);
            }

            transaction.update(floatDocRef, { balance: newBalance });
            console.log(`[${new Date().toISOString()}] Float balance for ${telco} updated successfully to ${newBalance}.`);

            // Optional: Log this float deduction to a 'floatDeductionLogs' or similar collection
            // if (status.toUpperCase() === 'COMPLETED' || status.toUpperCase() === 'SUCCESS') {
            //     transaction.set(db.collection('floatLogs').doc(), {
            //         type: 'Usage', // Using 'Usage' as per your existing floatLogs type
            //         Amount: amount,
            //         description: `Airtime purchase for ${telco} (ID: ${transactionId})`,
            //         timestamp: admin.firestore.FieldValue.serverTimestamp(),
            //         transactionId: transactionId,
            //         telco: telco,
            //         newBalance: newBalance // Log the new balance after deduction
            //     });
            // }
        });

        res.status(200).json({ success: true, message: `Float balance for ${telco} deducted successfully. New balance is updated.` });

    } catch (error) {
        console.error(`[${new Date().toISOString()}] Error processing float deduction for ${telco} transaction ${transactionId}:`, error.message);
        res.status(500).json({ success: false, message: `Failed to deduct float: ${error.message}` });
    }
});


// --- MODIFIED Existing Endpoint: GET /api/analytics/dashboard ---
// This endpoint now reuses the getSalesOverviewData function
app.get('/api/analytics/dashboard', async (req, res) => {
    try {
        const { sales, topPurchasers } = await getSalesOverviewData(); // Get the sales data

        // Fetching and summing Saf_float and AT_Float balances
        const [safBalance, atBalance] = await Promise.all([
            getIndividualFloatBalance('Saf_float'),
            getIndividualFloatBalance('AT_Float')
        ]);
        const totalCombinedFloatBalance = safBalance + atBalance;

        // Fetch floatLogs for historical viewing
        const floatLogsData = [];
        const floatSnap = await db.collection('floatLogs').orderBy('timestamp', 'desc').limit(50).get();
        floatSnap.docs.forEach(doc => {
            const data = doc.data();
            const dateToFormat = data.timestamp && typeof data.timestamp.toDate === 'function'
                ? data.timestamp.toDate()
                : new Date(); // Fallback

            floatLogsData.push({
                date: formatDate(dateToFormat),
                type: data.type,
                Amount: data.Amount, // Use 'Amount' as per your frontend expects
                description: data.description,
            });
        });

        res.json({
            sales, // Now includes today, month, and trend
            floatBalance: totalCombinedFloatBalance,
            safFloatBalance: safBalance,
            atFloatBalance: atBalance,
            floatLogs: floatLogsData,
            topPurchasers,
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Failed to fetch analytics data.' });
    }
});


// Helper: Get sales for a specific telco and date (kept for /overview, if needed, but consider using new EAT functions)
const getSalesForDate = async (telco, date) => {
    const startOfDayEAT = getStartOfDayEAT(date);
    const endOfDayEAT = getEndOfDayEAT(date);
    const snap = await db.collection('sales')
        .where('carrier', '==', telco) // Using 'carrier' for consistency
        .where('status', '==', 'COMPLETED')
        .where('createdAt', '>=', startOfDayEAT)
        .where('createdAt', '<=', endOfDayEAT)
        .get();

    return snap.docs.reduce((sum, doc) => sum + (doc.data().amount || 0), 0);
};

// Helper: Get sales for a specific telco and month (kept for /overview, if needed, but consider using new EAT functions)
const getSalesForMonth = async (telco, date) => {
    const startOfMonthEAT = getStartOfMonthEAT(date);
    // For month end, it's easier to query ">= startOfMonth" to the current date if only looking up to "now"
    // or calculate end of month if you need full past months.
    // For simplicity in monthly breakdown, we'll just query >= start of month.
    const snap = await db.collection('sales')
        .where('carrier', '==', telco) // Using 'carrier' for consistency
        .where('status', '==', 'COMPLETED')
        .where('createdAt', '>=', startOfMonthEAT)
        .get();

    return snap.docs.reduce((sum, doc) => sum + (doc.data().amount || 0), 0);
};


// Existing Endpoint: GET /api/analytics/overview (slightly modified to use new date helpers and float)
app.get('/api/analytics/overview', async (req, res) => {
    try {
        // Generate summaryData
        const telcos = ['Safaricom', 'Airtel', 'Telkom'];
        const colors = { Safaricom: 'emerald', Airtel: 'cyber', Telkom: 'sky' };

        const summaryData = await Promise.all(
            telcos.map(async (telco) => {
                const todaySales = await getSalesForDate(telco, new Date());
                const monthSales = await getSalesForMonth(telco, new Date());

                // Use the combined float balance for the 'overview' float as well
                const [safBalance, atBalance] = await Promise.all([
                    getIndividualFloatBalance('Saf_float'),
                    getIndividualFloatBalance('AT_Float')
                ]);
                const combinedFloatForOverview = safBalance + atBalance;

                return {
                    company: telco,
                    color: colors[telco],
                    today: todaySales,
                    month: monthSales,
                    float: combinedFloatForOverview, // Use the new combined float
                };
            })
        );

        // Generate monthly breakdown for the past 6 months (unchanged logic, still uses getSalesForMonth helper)
        const now = new Date();
        const months = Array.from({ length: 6 }, (_, i) => {
            const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
            return d.toLocaleString('default', { month: 'short' });
        }).reverse();

        const monthlyBreakdown = await Promise.all(
            months.map(async (monthLabel, i) => {
                const d = new Date(now.getFullYear(), now.getMonth() - (5 - i), 1);
                const entries = await Promise.all(telcos.map(async telco => {
                    const amount = await getSalesForMonth(telco, d); // Still using getSalesForMonth
                    return [telco, amount];
                }));
                const entryObject = Object.fromEntries(entries);
                return { month: monthLabel, ...entryObject };
            })
        );

        res.json({ summaryData, monthlyBreakdown });
    } catch (err) {
        console.error('Overview Error:', err.message);
        res.status(500).json({ error: 'Failed to load sales overview.' });
    }
});


app.listen(PORT, () => console.log(`Analytics server running on port ${PORT}`));
