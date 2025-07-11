import admin from 'firebase-admin';
import cors from 'cors';
import dotenv from 'dotenv';
import express from 'express';

dotenv.config();
const app = express();
const PORT = process.env.PORT || 5002;

// --- Initialize Firebase Admin ---
let serviceAccount;
try {
  const base64EncodedKey = process.env.GOOGLE_APPLICATION_CREDENTIALS_BASE64;
  if (base64EncodedKey) {
    const decodedServiceAccount = Buffer.from(base64EncodedKey, 'base64').toString('utf8');
    serviceAccount = JSON.parse(decodedServiceAccount);
  } else {
    throw new Error("GOOGLE_APPLICATION_CREDENTIALS_BASE64 env var not set.");
  }
} catch (err) {
  console.error("Service account decode failed:", err);
  process.exit(1);
}

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

app.use(cors());
app.use(express.json());

// --- Date Helpers ---
const formatDate = (date) => date.toISOString().split('T')[0];

const getStartOfDayEAT = (date) => {
  const d = new Date(date);
  d.setUTCHours(0, 0, 0, 0);
  d.setUTCHours(d.getUTCHours() - 3);
  return admin.firestore.Timestamp.fromDate(d);
};

const getEndOfDayEAT = (date) => {
  const d = new Date(date);
  d.setUTCHours(23, 59, 59, 999);
  d.setUTCHours(d.getUTCHours() - 3);
  return admin.firestore.Timestamp.fromDate(d);
};

const getStartOfMonthEAT = (date) => {
  const d = new Date(date.getFullYear(), date.getMonth(), 1);
  d.setUTCHours(0, 0, 0, 0);
  d.setUTCHours(d.getUTCHours() - 3);
  return admin.firestore.Timestamp.fromDate(d);
};

// --- Float Helpers ---
const getIndividualFloatBalance = async (floatType) => {
  try {
    const doc = await db.collection(floatType).doc('current').get();
    if (doc.exists) return doc.data().balance || 0;
    return 0;
  } catch (err) {
    console.error(`Error fetching ${floatType}:`, err);
    return 0;
  }
};

function getFloatCollectionId(telco) {
  if (telco === 'Safaricom') return 'Saf_float';
  if (['Airtel', 'Telkom', 'Africastalking'].includes(telco)) return 'AT_Float';
  return null;
}

// --- NEW: Aggregated Sales Overview ---
const getSalesOverviewData = async () => {
  const telcos = ['Safaricom', 'Airtel', 'Telkom'];
  const sales = {};
  const topPurchasers = {};
  const today = new Date();
  const yesterday = new Date();
  yesterday.setDate(today.getDate() - 1);

  const startOfTodayEAT = getStartOfDayEAT(today);
  const endOfTodayEAT = getEndOfDayEAT(today);
  const startOfYesterdayEAT = getStartOfDayEAT(yesterday);
  const endOfYesterdayEAT = getEndOfDayEAT(yesterday);
  const startOfThisMonthEAT = getStartOfMonthEAT(today);

  for (const telco of telcos) {
    const todayAgg = await db.collection('sales')
      .where('status', 'in', ['COMPLETED', 'SUCCESS'])
      .where('carrier', '==', telco)
      .where('createdAt', '>=', startOfTodayEAT)
      .where('createdAt', '<=', endOfTodayEAT)
      .aggregate({ totalAmount: admin.firestore.aggregate.sum('amount') });
    const todayTotal = todayAgg.data().totalAmount || 0;

    const yesterdayAgg = await db.collection('sales')
      .where('status', 'in', ['COMPLETED', 'SUCCESS'])
      .where('carrier', '==', telco)
      .where('createdAt', '>=', startOfYesterdayEAT)
      .where('createdAt', '<=', endOfYesterdayEAT)
      .aggregate({ totalAmount: admin.firestore.aggregate.sum('amount') });
    const yesterdayTotal = yesterdayAgg.data().totalAmount || 0;

    const monthAgg = await db.collection('sales')
      .where('status', 'in', ['COMPLETED', 'SUCCESS'])
      .where('carrier', '==', telco)
      .where('createdAt', '>=', startOfThisMonthEAT)
      .aggregate({ totalAmount: admin.firestore.aggregate.sum('amount') });
    const monthTotal = monthAgg.data().totalAmount || 0;

    const trend = yesterdayTotal === 0
      ? todayTotal > 0 ? 'up' : 'neutral'
      : todayTotal >= yesterdayTotal ? 'up' : 'down';

    sales[telco] = {
      today: todayTotal,
      month: monthTotal,
      trend,
    };

    // --- Top Purchasers still uses .get() ---
    const snap = await db.collection('sales')
      .where('carrier', '==', telco)
      .where('status', 'in', ['COMPLETED', 'SUCCESS'])
      .get();
    const purchasers = {};
    snap.forEach(doc => {
      const { topupNumber, amount } = doc.data();
      if (topupNumber && typeof amount === 'number') {
        purchasers[topupNumber] = (purchasers[topupNumber] || 0) + amount;
      }
    });
    const sortedTop = Object.entries(purchasers)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([name, Amount]) => ({ name, Amount }));
    topPurchasers[telco] = sortedTop;
  }

  return { sales, topPurchasers };
};

// --- Endpoint: /api/analytics/sales-overview ---
app.get('/api/analytics/sales-overview', async (req, res) => {
  try {
    const { sales } = await getSalesOverviewData();
    res.json(sales);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to fetch sales overview.' });
  }
});

// --- Endpoint: /api/analytics/dashboard ---
app.get('/api/analytics/dashboard', async (req, res) => {
  try {
    const { sales, topPurchasers } = await getSalesOverviewData();
    const [safBalance, atBalance] = await Promise.all([
      getIndividualFloatBalance('Saf_float'),
      getIndividualFloatBalance('AT_Float'),
    ]);
    const floatLogsData = [];
    const floatSnap = await db.collection('floatLogs').orderBy('timestamp', 'desc').limit(50).get();
    floatSnap.forEach(doc => {
      const data = doc.data();
      const dateToFormat = data.timestamp?.toDate?.() || new Date();
      floatLogsData.push({
        date: formatDate(dateToFormat),
        type: data.type,
        Amount: data.Amount,
        description: data.description,
      });
    });
    res.json({
      sales,
      floatBalance: safBalance + atBalance,
      safFloatBalance: safBalance,
      atFloatBalance: atBalance,
      floatLogs: floatLogsData,
      topPurchasers,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to fetch dashboard.' });
  }
});

// --- Aggregated Helpers for /overview ---
const getSalesForDate = async (telco, date) => {
  const start = getStartOfDayEAT(date);
  const end = getEndOfDayEAT(date);
  const agg = await db.collection('sales')
    .where('carrier', '==', telco)
    .where('status', 'in', ['COMPLETED', 'SUCCESS'])
    .where('createdAt', '>=', start)
    .where('createdAt', '<=', end)
    .aggregate({ totalAmount: admin.firestore.aggregate.sum('amount') });
  return agg.data().totalAmount || 0;
};

const getSalesForMonth = async (telco, date) => {
  const start = getStartOfMonthEAT(date);
  const agg = await db.collection('sales')
    .where('carrier', '==', telco)
    .where('status', 'in', ['COMPLETED', 'SUCCESS'])
    .where('createdAt', '>=', start)
    .aggregate({ totalAmount: admin.firestore.aggregate.sum('amount') });
  return agg.data().totalAmount || 0;
};

// --- Endpoint: /api/analytics/overview ---
app.get('/api/analytics/overview', async (req, res) => {
  try {
    const telcos = ['Safaricom', 'Airtel', 'Telkom'];
    const colors = { Safaricom: 'emerald', Airtel: 'cyber', Telkom: 'sky' };
    const summaryData = await Promise.all(telcos.map(async telco => {
      const todaySales = await getSalesForDate(telco, new Date());
      const monthSales = await getSalesForMonth(telco, new Date());
      const [safBalance, atBalance] = await Promise.all([
        getIndividualFloatBalance('Saf_float'),
        getIndividualFloatBalance('AT_Float'),
      ]);
      return {
        company: telco,
        color: colors[telco],
        today: todaySales,
        month: monthSales,
        float: safBalance + atBalance,
      };
    }));

    const now = new Date();
    const months = Array.from({ length: 6 }, (_, i) => {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      return d.toLocaleString('default', { month: 'short' });
    }).reverse();

    const monthlyBreakdown = await Promise.all(months.map(async (label, i) => {
      const d = new Date(now.getFullYear(), now.getMonth() - (5 - i), 1);
      const entries = await Promise.all(telcos.map(async telco => {
        const amount = await getSalesForMonth(telco, d);
        return [telco, amount];
      }));
      return { month: label, ...Object.fromEntries(entries) };
    }));

    res.json({ summaryData, monthlyBreakdown });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to fetch overview.' });
  }
});

// --- Your airtime POST stays unchanged ---
app.post('/api/process-airtime-purchase', async (req, res) => {
  const { amount, status, telco, transactionId } = req.body;
  if (!amount || !status || !telco || !transactionId) {
    return res.status(400).json({ success: false, message: 'Invalid purchase data.' });
  }
  if (!['COMPLETED', 'SUCCESS'].includes(status.toUpperCase())) {
    return res.status(200).json({ success: true, message: 'No float deduction needed.' });
  }
  const floatCollectionId = getFloatCollectionId(telco);
  if (!floatCollectionId) {
    return res.status(400).json({ success: false, message: 'Unknown telco.' });
  }
  const floatDocRef = db.collection(floatCollectionId).doc('current');
  try {
    await db.runTransaction(async tx => {
      const doc = await tx.get(floatDocRef);
      const current = doc.data().balance || 0;
      const newBalance = current - amount;
      if (newBalance < 0) throw new Error('Insufficient float.');
      tx.update(floatDocRef, { balance: newBalance });
    });
    res.status(200).json({ success: true, message: 'Float deducted.' });
  } catch (err) {
    res.status(500).json({ success: false, message: `Float deduction failed: ${err.message}` });
  }
});

app.listen(PORT, () => console.log(`✅ Analytics server running on ${PORT}`));
