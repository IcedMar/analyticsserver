import express from 'express';
import admin from 'firebase-admin';
import cors from 'cors';


const app = express();
const PORT = process.env.PORT || 5002;

let serviceAccount;
try {
    if (process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON) {
        serviceAccount = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON);
    } else {
        throw new Error("GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable is not set.");
    }
} catch (error) {
    console.error("Failed to parse service account key from environment variable:", error);
    process.exit(1);
}

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

app.use(cors());
app.use(express.json());

// Helper: format date (already exists)
const formatDate = (date) => date.toISOString().split('T')[0];

// Helper: get individual float balance (already exists)
const getIndividualFloatBalance = async (floatType) => {
    try {
        const floatDoc = await db.collection(floatType).doc('current').get();
        if (floatDoc.exists) {
            // Return the 'balance' field from the 'current' document
            return floatDoc.data().balance || 0;
        } else {
            console.warn(`Backend: ${floatType}/current document does not exist. Returning 0 float balance.`);
            return 0; // Default to 0 if the document doesn't exist
        }
    } catch (error) {
        console.error(`Error fetching ${floatType} float balance (backend):`, error);
        return 0; // Default to 0 on error
    }
};

// --- NEW HELPER: Map telco to its float balance document/collection ID ---
// You mentioned "Africastalking" and "Safaricom" as telcos for float deduction.
// This mapping assumes:
// 'Safaricom' airtime deduction impacts 'Saf_float' collection.
// 'Airtel'/'Telkom' airtime deduction impacts 'AT_Float' (Africastalking) collection.
// If you have separate floats for Airtel/Telkom, adjust this.
function getFloatCollectionId(telco) {
  if (telco === 'Safaricom') {
    return 'Saf_float';
  } else if (telco === 'Airtel' || telco === 'Telkom' || telco === 'Africastalking') {
    // Assuming Airtel and Telkom deductions come from an "Africastalking" float.
    // If 'Africastalking' is itself a telco for deduction (not just an aggregator),
    // ensure your purchase records accurately reflect that.
    return 'AT_Float';
  }
  return null; // Invalid or unmapped telco
}


// --- NEW ENDPOINT: Process Airtime Purchase and Deduct Float ---
// This endpoint should be called by your frontend (or another backend service)
// AFTER a purchase has been successfully completed and its status is 'SUCCESS'.
app.post('/api/process-airtime-purchase', async (req, res) => {
  const {
    amount,        // The amount of airtime purchased (number)
    status,        // 'SUCCESS', 'FAILED', 'PENDING', etc. (string)
    telco,         // 'Safaricom', 'Airtel', 'Telkom' (string)
    transactionId, // A unique ID for the transaction (e.g., MPESA code, internal ID)
    // You might also send: purchaserInfo, txCode, dispatch etc. if you want to log them here
  } = req.body;

  console.log(`[${new Date().toISOString()}] Received purchase request:`, { amount, status, telco, transactionId });

  // 1. Basic validation of incoming data
  if (typeof amount !== 'number' || amount <= 0 || !status || !telco || !transactionId) {
    console.warn(`[${new Date().toISOString()}] Invalid request body for float deduction:`, req.body);
    return res.status(400).json({ success: false, message: 'Invalid purchase data provided. Missing amount, status, telco, or transactionId.' });
  }

  // 2. Only proceed if the transaction was genuinely successful
  if (status.toUpperCase() !== 'SUCCESS') { // Using .toUpperCase() for robustness
    console.log(`[${new Date().toISOString()}] Transaction ${transactionId} status is ${status}. No float deduction needed.`);
    // Respond successfully, as no error occurred from this service's perspective
    return res.status(200).json({ success: true, message: `Transaction ${transactionId} was ${status}. No float deduction needed.` });
  }

  // Determine which float balance to deduct from
  const floatCollectionId = getFloatCollectionId(telco);
  if (!floatCollectionId) {
    console.error(`[${new Date().toISOString()}] Unknown or unmapped telco "${telco}" for transaction ${transactionId}. Cannot deduct float.`);
    return res.status(400).json({ success: false, message: `Unknown telco "${telco}". Cannot process float deduction.` });
  }

  // Reference to the specific float balance document
  const floatDocRef = db.collection(floatCollectionId).doc('current');

  try {
    // 3. Use a Firestore Transaction for atomic updates
    await db.runTransaction(async (transaction) => {
      const doc = await transaction.get(floatDocRef);

      if (!doc.exists) {
        console.error(`[${new Date().toISOString()}] Float balance document "${floatCollectionId}/current" does not exist!`);
        throw new Error(`Float balance for ${telco} (${floatCollectionId}) not found.`);
      }

      const currentBalance = doc.data().balance; // Assuming the float field is named 'balance'
      if (typeof currentBalance !== 'number') {
          console.error(`[${new Date().toISOString()}] Float balance for ${floatCollectionId} is not a number. Value: ${currentBalance}`);
          throw new Error(`Float balance for ${floatCollectionId} is corrupted or not a number.`);
      }

      const newBalance = currentBalance - amount;

      console.log(`[${new Date().toISOString()}] Processing ${telco} float deduction for transaction ${transactionId}:`);
      console.log(`  Current balance (${floatCollectionId}): ${currentBalance}`);
      console.log(`  Amount to deduct: ${amount}`);
      console.log(`  New balance will be: ${newBalance}`);

      // Optional: Prevent negative float (CRITICAL for financial integrity)
      if (newBalance < 0) {
        console.error(`[${new Date().toISOString()}] Insufficient float for ${telco}! Current: ${currentBalance}, Deduction: ${amount}`);
        // IMPORTANT: In a real-world scenario, you might log this heavily, trigger an alert,
        // and potentially even reverse the user's purchase if float goes negative.
        throw new Error(`Insufficient float balance for ${telco}. Current: ${currentBalance.toLocaleString()}, Needed: ${amount.toLocaleString()}.`);
      }

      // Update the float balance in Firestore
      transaction.update(floatDocRef, { balance: newBalance });
      console.log(`[${new Date().toISOString()}] Float balance for ${telco} updated successfully to ${newBalance}.`);

      // OPTIONAL: Log this float deduction to a 'floatDeductionLogs' or similar collection
      // for auditing, distinct from 'floatLogs' which seems to track top-ups too.
      // transaction.set(db.collection('floatDeductionLogs').doc(), {
      //   transactionId,
      //   telco,
      //   deductedAmount: amount,
      //   newBalance,
      //   timestamp: admin.firestore.FieldValue.serverTimestamp(),
      //   type: 'SaleDeduction'
      // });
    });

    res.status(200).json({ success: true, message: `Float balance for ${telco} deducted successfully. New balance is updated.` });

  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error processing float deduction for ${telco} transaction ${transactionId}:`, error.message);
    res.status(500).json({ success: false, message: `Failed to deduct float: ${error.message}` });
  }
});


// Existing Endpoint: GET /api/analytics/dashboard (unchanged)
app.get('/api/analytics/dashboard', async (req, res) => {
  try {
    const today = new Date();
    const yesterday = new Date();
    yesterday.setDate(today.getDate() - 1);

    const todayStr = formatDate(today);
    const yesterdayStr = formatDate(yesterday);

    const telcos = ['Safaricom', 'Airtel', 'Telkom'];
    const sales = {};
    const topPurchasers = {};

    for (const telco of telcos) {
      // Today's sales
      const todaySnap = await db.collection('sales')
        .where('telco', '==', telco)
        .where('date', '==', todayStr)
        .get();
      const todayTotal = todaySnap.docs.reduce((sum, doc) => sum + doc.data().amount, 0);

      // Yesterday's sales
      const yesterdaySnap = await db.collection('sales')
        .where('telco', '==', telco)
        .where('date', '==', yesterdayStr)
        .get();
      const yesterdayTotal = yesterdaySnap.docs.reduce((sum, doc) => sum + doc.data().amount, 0);

      // Month's sales
      const startOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);
      const monthSalesSnap = await db.collection('sales')
        .where('telco', '==', telco)
        .where('timestamp', '>=', startOfMonth) // Note: Using 'timestamp' here, ensure consistency with 'date' if both exist.
        .get();
      const monthTotal = monthSalesSnap.docs.reduce((sum, doc) => sum + doc.data().amount, 0);

      // Determine trend
      const trend = todayTotal >= yesterdayTotal ? 'up' : 'down';

      sales[telco] = {
        today: todayTotal,
        month: monthTotal,
        trend,
      };

      // Top Purchasers
      const topSnap = await db.collection('sales')
        .where('telco', '==', telco)
        .get();
      const purchasers = {};
      topSnap.docs.forEach(doc => {
        const { name, amount } = doc.data();
        purchasers[name] = (purchasers[name] || 0) + amount;
      });
      const sortedTop = Object.entries(purchasers)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([name, amount]) => ({ name, amount }));

      topPurchasers[telco] = sortedTop;
    }

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
        amount: data.amount,
        description: data.description,
      });
    });

    res.json({
      sales,
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


// Helper: Get sales for a specific telco and date (Keep as is)
const getSalesForDate = async (telco, date) => {
  const dateStr = formatDate(date);
  const snap = await db.collection('sales')
    .where('telco', '==', telco)
    .where('date', '==', dateStr)
    .get();

  return snap.docs.reduce((sum, doc) => sum + doc.data().amount, 0);
};

// Helper: Get sales for a specific telco and month (Keep as is)
const getSalesForMonth = async (telco, date) => {
  const start = new Date(date.getFullYear(), date.getMonth(), 1);
  const end = new Date(date.getFullYear(), date.getMonth() + 1, 0, 23, 59, 59, 999);

  const snap = await db.collection('sales')
    .where('telco', '==', telco)
    .where('timestamp', '>=', start) // Note: Using 'timestamp' here, ensure consistency with 'date' if both exist.
    .where('timestamp', '<=', end)
    .get();

  return snap.docs.reduce((sum, doc) => sum + doc.data().amount, 0);
};

// This helper function is now largely redundant for the dashboard's main floatBalance
// It still *could* be used for other purposes if you need to calculate a historical
// balance from floatLogs, but the primary floatBalance now comes from 'current' docs.
const getFloatBalance = async () => {
    const floatSnap = await db.collection('floatLogs').get();
    return floatSnap.docs.reduce((sum, doc) => {
        const data = doc.data();
        if (data.type === 'Top-up') {
            return sum + data.amount;
        } else if (data.type === 'Sale' || data.type === 'Withdrawal' || data.type === 'Payout') {
            return sum - data.amount;
        } else {
            console.warn(`Unexpected floatLog type encountered: ${data.type} for doc ID: ${doc.id}`);
            return sum;
        }
    }, 0);
};


// Existing Endpoint: GET /api/analytics/overview (unchanged)
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

    // Generate dummy monthly breakdown for the past 6 months
    const now = new Date();
    const months = Array.from({ length: 6 }, (_, i) => {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      return d.toLocaleString('default', { month: 'short' });
    }).reverse();

    const monthlyBreakdown = await Promise.all(
      months.map(async (monthLabel, i) => {
        const d = new Date(now.getFullYear(), now.getMonth() - (5 - i), 1);
        const entries = await Promise.all(telcos.map(async telco => {
          const amount = await getSalesForMonth(telco, d);
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