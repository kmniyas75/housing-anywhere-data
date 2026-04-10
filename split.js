const fs = require('fs');
const path = require('path');
const https = require('https');
const { chain } = require('stream-chain');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');

const FILE_URL = "https://housinganywhere.com/feeds/Stayforall/Stayforall.json";
const OUTPUT_DIR = path.join(__dirname, 'docs');
const TEMP_FILE = path.join(__dirname, 'full-data.json');

// Ensure docs folder exists
if (!fs.existsSync(OUTPUT_DIR)) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

async function downloadFile(url, dest) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    https.get(url, (response) => {
      if (response.statusCode !== 200) {
        reject(new Error(`Failed to download: ${response.statusCode}`));
        return;
      }
      response.pipe(file);
      file.on('finish', () => {
        file.close();
        resolve();
      });
    }).on('error', (err) => {
      fs.unlink(dest, () => reject(err));
    });
  });
}

const cityData = {};

async function processData() {
  console.log("🚀 Starting data processing...");
  
  try {
    await downloadFile(FILE_URL, TEMP_FILE);
    console.log("✅ Download complete.");

    const pipeline = chain([
      fs.createReadStream(TEMP_FILE),
      parser(),
      streamArray()
    ]);

    pipeline.on('data', ({ value }) => {
      // Sanitize city name: lowercase, trim, replace spaces/special chars with hyphens
      const rawCity = value.city || 'unknown';
      const city = rawCity.toLowerCase()
        .trim()
        .normalize("NFD").replace(/[\u0300-\u036f]/g, "") // Remove accents
        .replace(/[^a-z0-9]/g, '-') // Replace non-alphanumeric with hyphens
        .replace(/-+/g, '-') // Remove double hyphens
        .replace(/^-|-$/g, ''); // Remove leading/trailing hyphens
      
      if (!cityData[city]) {
        cityData[city] = [];
      }
      cityData[city].push(value);
    });

    pipeline.on('end', () => {
      console.log("📦 Splitting into city files...");
      
      const cities = Object.keys(cityData);
      cities.forEach(city => {
        fs.writeFileSync(
          path.join(OUTPUT_DIR, `${city}.json`),
          JSON.stringify(cityData[city])
        );
      });

      // Create a cities index for the frontend
      fs.writeFileSync(
        path.join(OUTPUT_DIR, 'cities.json'),
        JSON.stringify(cities.sort())
      );

      console.log(`✨ Done! Processed ${cities.length} cities.`);
      
      // Clean up temp file
      if (fs.existsSync(TEMP_FILE)) {
        fs.unlinkSync(TEMP_FILE);
      }
    });

    pipeline.on('error', (err) => {
      console.error("❌ Pipeline error:", err);
    });

  } catch (error) {
    console.error("❌ Error:", error);
  }
}

processData();
