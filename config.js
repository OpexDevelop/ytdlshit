import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

let config = {};

try {
  const configPath = path.resolve(__dirname, 'env.json');
  const configFile = await fs.readFile(configPath, 'utf-8');
  config = JSON.parse(configFile);
} catch (err) {
  console.error('CRITICAL: Error reading or parsing env.json', err);
  throw err;
}

export default config;
