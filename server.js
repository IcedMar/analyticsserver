import admin from 'firebase-admin';
import cors from 'cors';
import dotenv from 'dotenv';
import express from 'express';

dotenv.config();
const app = express();
const PORT = process.env.PORT || 5050;

let serviceAccount;
try {
  const base64EncodedKey = process.env.GOOGLE_APPLICATION_CREDENTIALS_BASE64;
  if (base64EncodedKey) {
    const decoded = Buffer.from(base64EncodedKey, 'base64').toString('utf8');
    serviceAccount = JSON.parse(decoded);
  } else {
    throw new Error('GOOGLE_APPLICATION_CREDENTIALS_BASE64 is not set.');
  }
} catch (err) {
  console.error('Failed to parse service account key:', err);
  process.exit(1);
}

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();
app.use(cors());
app.use(express.json());

const formatDate = (date) => date.toISOString().split('T')[0];

const getFloatCollectionId = (telco) => {
  if (telco === 'Safaricom') return 'Saf_float';
  if (['Airtel', 'Telkom', 'Africastalking'].includes(telco)) return 'AT_Float';
  return null;
};

const getIndividualFloatBalance = async (floatType) => {
  try {
    const doc = await db.collection(floatType).doc('current').get();
    return doc.exists ? doc.data().balance || 0 : 0;
  } catch (err) {
    console.error(`Error fetching ${floatType} float:`, err);
    return 0;
  }
};

// --- Time helpers ---
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

// --- Fallback-safe aggregation ---
async function sumSales(collectionRef) {
  if (collectionRef.aggregate) {
    const agg = await collectionRef.aggregate({
      totalAmount: admin.firestore.aggregate.sum('amount')
    });
    return agg.data().totalAmount || 0;
  } else {
    const snap = await collectionRef.get();
    return snap.docs.reduce((sum, doc) => sum + (doc.data().amount || 0), 0);
  }
}

// --- Main Sales Data Function ---
const getSalesOverviewData = async () => {
  const telcos = ['Safaricom', 'Airtel', 'Telkom'];
  const sales = {};
  const topPurchasers = {};

  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);

  const startToday = getStartOfDayEAT(today);
  const endToday = getEndOfDayEAT(today);
  const startYesterday = getStartOfDayEAT(yesterday);
  const endYesterday = getEndOfDayEAT(yesterday);
  const startMonth = getStartOfMonthEAT(today);

  for (const telco of telcos) {
    // Today
    const todayRef = db.collection('sales')
      .where('status', 'in', ['COMPLETED', 'SUCCESS'])
      .where('carrier', '==', telco)
      .where('createdAt', '>=', startToday)
      .where('createdAt', '<=', endToday);
    const todayTotal = await sumSales(todayRef);

    // Yesterday
    const yestRef = db.collection('sales')
      .where('status', 'in', ['COMPLETED', 'SUCCESS'])
      .where('carrier', '==', telco)
      .where('createdAt', '>=', startYesterday)
      .where('createdAt', '<=', endYesterday);
    const yestTotal = await sumSales(yestRef);

    // This month
    const monthRef = db.collection('sales')
      .where('status', 'in', ['COMPLETED', 'SUCCESS'])
      .where('carrier', '==', telco)
      .where('createdAt', '>=', startMonth);
    const monthTotal = await sumSales(monthRef);

    const trend = yestTotal === 0
      ? (todayTotal > 0 ? 'up' : 'neutral')
      : (todayTotal >= yestTotal ? 'up' : 'down');

    sales[telco] = { today: todayTotal, month: monthTotal, trend };

    // Top purchasers
    const allRef = db.collection('sales')
      .where('carrier', '==', telco)
      .where('status', 'in', ['COMPLETED', 'SUCCESS']);
    const allSnap = await allRef.get();
    const buyers = {};
    allSnap.forEach(doc => {
      const { topupNumber, amount } = doc.data();
      if (topupNumber) buyers[topupNumber] = (buyers[topupNumber] || 0) + (amount || 0);
    });
    const top = Object.entries(buyers)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([name, Amount]) => ({ name, Amount }));
    topPurchasers[telco] = top;
  }

  return { sales, topPurchasers };
};

// --- Endpoints ---
app.get('/api/analytics/sales-overview', async (req, res) => {
  try {
    const { sales } = await getSalesOverviewData();
    res.json(sales);
  } catch (err) {
    console.error('Error:', err);
    res.status(500).json({ error: 'Failed to load sales overview.' });
  }
});

app.post('/api/process-airtime-purchase', async (req, res) => {
  const { amount, status, telco, transactionId } = req.body;

  if (!amount || !status || !telco || !transactionId) {
    return res.status(400).json({ error: 'Missing fields.' });
  }

  if (!['COMPLETED', 'SUCCESS'].includes(status.toUpperCase())) {
    return res.json({ ok: true, note: 'No float deduction needed.' });
  }

  const floatCollectionId = getFloatCollectionId(telco);
  if (!floatCollectionId) {
    return res.status(400).json({ error: 'Unknown telco.' });
  }

  const floatRef = db.collection(floatCollectionId).doc('current');

  try {
    await db.runTransaction(async (tx) => {
      const doc = await tx.get(floatRef);
      if (!doc.exists) throw new Error('Float doc missing.');
      const current = doc.data().balance || 0;
      const newBal = current - amount;
      if (newBal < 0) throw new Error('Insufficient float.');
      tx.update(floatRef, { balance: newBal });
    });
    res.json({ ok: true, note: 'Float deducted.' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/analytics/dashboard', async (req, res) => {
  try {
    const { sales, topPurchasers } = await getSalesOverviewData();
    const saf = await getIndividualFloatBalance('Saf_float');
    const at = await getIndividualFloatBalance('AT_Float');

    const floatLogsSnap = await db.collection('floatLogs')
      .orderBy('timestamp', 'desc')
      .limit(50)
      .get();

    const floatLogs = floatLogsSnap.docs.map(doc => ({
      date: formatDate(doc.data().timestamp?.toDate?.() || new Date()),
      type: doc.data().type,
      Amount: doc.data().Amount,
      description: doc.data().description,
    }));

    res.json({
      sales,
      safFloatBalance: saf,
      atFloatBalance: at,
      floatBalance: saf + at,
      floatLogs,
      topPurchasers
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to load dashboard.' });
  }
});

app.listen(PORT, () => console.log(`✅ Analytics server running on ${PORT}`));
