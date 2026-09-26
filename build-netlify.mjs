import { mkdirSync, copyFileSync, writeFileSync } from 'node:fs';

const rawOrigin = process.env.STREAMLAB_API_ORIGIN;
if (!rawOrigin) {
  throw new Error('Set STREAMLAB_API_ORIGIN to your HTTPS Python backend URL in Netlify.');
}

const origin = new URL(rawOrigin);
if (origin.protocol !== 'https:' || origin.username || origin.password || origin.pathname !== '/' || origin.search || origin.hash) {
  throw new Error('STREAMLAB_API_ORIGIN must be an HTTPS origin, such as https://your-service.onrender.com');
}

mkdirSync('public', { recursive: true });
copyFileSync('index.html', 'public/index.html');
writeFileSync('public/_redirects', [
  `/api/* ${origin.origin}/api/:splat 200`,
  '/browse /index.html 200',
  '/watch /index.html 200',
  '',
].join('\n'));
console.log(`Netlify frontend ready; /api/* proxies to ${origin.origin}`);
