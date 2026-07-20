const admin = require("firebase-admin");

// Securely initialize Firebase via GitHub Secrets
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DATABASE_URL
});

const db = admin.database();

async function runThinning() {
  console.log("Starting Nightly Firebase Data Thinning...");
  const ref = db.ref("rooftop/history");
  const snapshot = await ref.once("value");
  const data = snapshot.val();
  
  if (!data) {
    console.log("No data found to process.");
    process.exit(0);
  }

  const now = Date.now() / 1000; // Current time in Unix Seconds
  const DAY = 86400;
  const HOUR = 3600;

  let updatedHistory = {};
  let hourlyBuckets = {};
  let dailyBuckets = {};

  // 1. Sort historical nodes into time buckets
  for (const [key, entry] of Object.entries(data)) {
    if (!entry.ts) continue;
    
    const age = now - entry.ts;

    if (age <= DAY) {
      // < 24 Hours: Keep granular 5-minute data exactly as is
      updatedHistory[key] = entry;
    } else if (age > DAY && age <= 7 * DAY) {
      // 1 to 7 Days: Group timestamps into strict 1-Hour buckets
      const hourBucket = Math.floor(entry.ts / HOUR) * HOUR;
      if (!hourlyBuckets[hourBucket]) hourlyBuckets[hourBucket] = [];
      hourlyBuckets[hourBucket].push(entry);
    } else {
      // > 7 Days: Group timestamps into strict 24-Hour (Daily) buckets
      const dayBucket = Math.floor(entry.ts / DAY) * DAY;
      if (!dailyBuckets[dayBucket]) dailyBuckets[dayBucket] = [];
      dailyBuckets[dayBucket].push(entry);
    }
  }

  // 2. Mathematical Aggregation Engine
  const aggregateBucket = (entries, bucketTs) => {
    let count = entries.length;
    let sumTw = 0, sumTo = 0, sumRmt = 0, sumRmh = 0, sumTds = 0, sumVol = 0;
    let maxLeak = 0, maxRain = 0, maxSol = 0;

    entries.forEach(e => {
      sumTw += (e.tw || 0);
      sumTo += (e.to || 0);
      sumRmt += (e.rmt || 0);
      sumRmh += (e.rmh || 0);
      sumTds += (e.tds || 0);
      
      // Volumes are cumulative within the hour/day
      sumVol += (e.vol || 0); 
      
      // Discrete traps & daily totals keep the highest recorded peak
      if ((e.leak || 0) > maxLeak) maxLeak = e.leak;
      if ((e.rain || 0) > maxRain) maxRain = e.rain;
      if ((e.sol_kwh || 0) > maxSol) maxSol = e.sol_kwh;
    });

    return {
      ts: bucketTs,
      tw: parseFloat((sumTw / count).toFixed(1)),
      to: parseFloat((sumTo / count).toFixed(1)),
      rmt: parseFloat((sumRmt / count).toFixed(1)),
      rmh: parseFloat((sumRmh / count).toFixed(1)),
      tds: Math.round(sumTds / count),
      vol: parseFloat(sumVol.toFixed(3)),
      leak: maxLeak,
      rain: maxRain,
      sol_kwh: parseFloat(maxSol.toFixed(3))
    };
  };

  // 3. Process the Hourly Buckets
  for (const [ts, entries] of Object.entries(hourlyBuckets)) {
    const aggId = `agg_h_${ts}`; // Custom key for hourly rollups
    updatedHistory[aggId] = aggregateBucket(entries, parseInt(ts));
  }

  // 4. Process the Daily Buckets
  for (const [ts, entries] of Object.entries(dailyBuckets)) {
    const aggId = `agg_d_${ts}`; // Custom key for daily rollups
    updatedHistory[aggId] = aggregateBucket(entries, parseInt(ts));
  }

  // 5. Overwrite the Firebase History array with the fully compressed dataset
  await ref.set(updatedHistory);
  console.log(`Thinning complete. Database optimized down to ${Object.keys(updatedHistory).length} core nodes.`);
  process.exit(0);
}

runThinning().catch(err => {
  console.error("Critical Failure in processing pipeline:", err);
  process.exit(1);
});