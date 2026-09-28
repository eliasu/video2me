// video2me — local ffmpeg web converter. Run: node server.js  →  http://localhost:4321
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';

const run = promisify(execFile);
const PORT = +process.env.PORT || 4321;
const VIDEO = /\.(mp4|m4v|mov|mkv|webm|avi|mts|m2ts|wmv|flv|mpg|mpeg|3gp)$/i;

const aac = ['-c:a', 'aac', '-b:a', '128k'];
const mp4 = ['-movflags', '+faststart'];
const CODECS = {
  h264: { ext: 'mp4', crf: 23, args: c => ['-c:v', 'libx264', '-preset', 'slow', '-crf', c, '-pix_fmt', 'yuv420p', '-profile:v', 'high', ...aac, ...mp4] },
  hevc: { ext: 'mp4', crf: 26, args: c => ['-c:v', 'libx265', '-preset', 'medium', '-crf', c, '-tag:v', 'hvc1', '-pix_fmt', 'yuv420p', '-x265-params', 'log-level=error', ...aac, ...mp4] },
  vp9:  { ext: 'webm', crf: 33, args: c => ['-c:v', 'libvpx-vp9', '-crf', c, '-b:v', '0', '-row-mt', '1', '-deadline', 'good', '-cpu-used', '2', '-pix_fmt', 'yuv420p', '-c:a', 'libopus', '-b:a', '96k'] },
  av1:  { ext: 'mp4', crf: 34, args: c => ['-c:v', 'libsvtav1', '-preset', '6', '-crf', c, '-pix_fmt', 'yuv420p', ...aac, ...mp4] },
};
const QUALITY = { high: -4, mid: 0, small: 5 };

async function probe(file) {
  const { stdout } = await run('ffprobe', ['-v', 'error', '-select_streams', 'v:0',
    '-show_entries', 'stream=width,height,codec_name:stream_side_data=rotation:format=duration', '-of', 'json', file]);
  const j = JSON.parse(stdout), s = j.streams?.[0] || {};
  let w = s.width, h = s.height;
  if (Math.abs(s.side_data_list?.find(d => 'rotation' in d)?.rotation) === 90) [w, h] = [h, w];
  return { w, h, codec: s.codec_name, duration: +j.format?.duration || 0 };
}

const cmp = (a, b) => a.localeCompare(b, 'de', { numeric: true });
async function list(dir) {
  // all subfolders; skips hidden files/folders (.git, ._AppleDouble …)
  const rels = fs.readdirSync(dir, { recursive: true })
    .filter(r => VIDEO.test(r) && !r.split(path.sep).some(s => s.startsWith('.')))
    .sort((a, b) => cmp(path.dirname(a), path.dirname(b)) || cmp(a, b));
  const out = [];
  let next = 0;
  // 8 ffprobes at a time so big trees don't fork hundreds of processes
  await Promise.all(Array.from({ length: 8 }, async () => {
    while (next < rels.length) {
      const k = next++, p = path.join(dir, rels[k]);
      const info = await probe(p).catch(() => null);
      if (info) out[k] = { name: path.basename(p), folder: path.dirname(rels[k]).replace(/^\.$/, ''), path: p, size: fs.statSync(p).size, ...info };
    }
  }));
  return out.filter(Boolean);
}

// ---------- job queue (sequential; ffmpeg already uses all cores) ----------
const jobs = [];
let current = null, nextId = 1;

function uniquePath(p) {
  const taken = q => fs.existsSync(q) || jobs.some(j => j.out === q && !['error', 'canceled'].includes(j.status));
  const { dir, name, ext } = path.parse(p);
  for (let i = 2, q = p; ; q = path.join(dir, `${name}-${i++}${ext}`)) if (!taken(q)) return q;
}

function pump() {
  if (current) return;
  const job = jobs.find(j => j.status === 'queued');
  if (!job) return;
  const { codec, quality, mute, w, h, inPoint, outPoint } = job.opts;
  const c = CODECS[codec];
  const args = ['-hide_banner', '-v', 'error', '-y', '-progress', 'pipe:1', '-nostats',
    '-ss', String(inPoint), '-i', job.src, '-t', String(outPoint - inPoint),
    '-map', '0:v:0', '-map', '0:a:0?', '-map_metadata', '-1',
    '-vf', `scale=${w}:${h}:flags=lanczos`, ...c.args(String(c.crf + QUALITY[quality])), ...(mute ? ['-an'] : []), job.out];
  fs.mkdirSync(path.dirname(job.out), { recursive: true });
  const p = spawn('ffmpeg', args);
  current = { job, p };
  job.status = 'running';
  let err = '';
  p.stdout.on('data', d => {
    const m = [...d.toString().matchAll(/out_time_us=(\d+)/g)].pop();
    if (m) job.progress = Math.min(1, m[1] / 1e6 / (outPoint - inPoint));
  });
  p.stderr.on('data', d => (err += d).length > 4000 && (err = err.slice(-4000)));
  p.on('close', code => {
    if (job.status === 'running') {
      if (code === 0) Object.assign(job, { status: 'done', progress: 1, outSize: fs.statSync(job.out).size });
      else Object.assign(job, { status: 'error', error: err.trim().split('\n').pop() || `ffmpeg exit ${code}` });
    }
    if (job.status !== 'done') fs.rmSync(job.out, { force: true }); // never leave half-written files
    current = null;
    pump();
  });
}

function enqueue({ items, codec, quality, mute, outDir }) {
  if (!CODECS[codec] || !(quality in QUALITY)) throw new Error('Ungültige Einstellungen');
  for (const it of items) {
    const w = it.w | 0, h = it.h | 0, inPoint = +it.inPoint, outPoint = +it.outPoint;
    if (!fs.existsSync(it.src) || w < 2 || h < 2 || w % 2 || h % 2 || !(outPoint > inPoint && inPoint >= 0)) throw new Error(`Ungültiger Auftrag: ${it.src}`);
    const name = path.basename(String(it.name)).replace(/\.[^.]*$/, '') + '.' + CODECS[codec].ext;
    const job = { id: nextId++, src: it.src, status: 'queued', progress: 0, srcSize: fs.statSync(it.src).size,
      opts: { codec, quality, mute: !!mute, w, h, inPoint, outPoint } };
    job.out = uniquePath(path.join(outDir || path.dirname(it.src), name));
    jobs.push(job);
  }
  pump();
}

function cancel() {
  for (const j of jobs) if (j.status === 'queued' || j.status === 'running') j.status = 'canceled';
  current?.p.kill('SIGKILL');
}

// ---------- http ----------
function sendFile(req, res, file) {
  const { size } = fs.statSync(file);
  // Chrome plays H.264/HEVC .mov fine when served as mp4
  const type = /\.webm$/i.test(file) ? 'video/webm' : 'video/mp4';
  const m = /bytes=(\d*)-(\d*)/.exec(req.headers.range || '');
  if (!m) { res.writeHead(200, { 'Content-Type': type, 'Content-Length': size, 'Accept-Ranges': 'bytes' }); return fs.createReadStream(file).pipe(res); }
  const start = +m[1] || 0, end = Math.min(m[2] ? +m[2] : size - 1, size - 1);
  res.writeHead(206, { 'Content-Type': type, 'Content-Length': end - start + 1, 'Content-Range': `bytes ${start}-${end}/${size}`, 'Accept-Ranges': 'bytes' });
  fs.createReadStream(file, { start, end }).pipe(res);
}

// 16 fast input seeks stitched side by side; works regardless of keyframe spacing
function filmstrip(res, file, dur) {
  const n = 16, ins = [], chains = [];
  for (let i = 0; i < n; i++) {
    ins.push('-ss', (dur * (i + 0.5) / n).toFixed(3), '-i', file);
    chains.push(`[${i}:v:0]scale=160:90:force_original_aspect_ratio=increase,crop=160:90,setsar=1[v${i}]`);
  }
  const graph = chains.join(';') + ';' + chains.map((_, i) => `[v${i}]`).join('') + `hstack=inputs=${n},format=yuvj420p`;
  const p = spawn('ffmpeg', ['-v', 'error', ...ins, '-filter_complex', graph, '-frames:v', '1', '-q:v', '6', '-f', 'image2', '-c:v', 'mjpeg', 'pipe:1']);
  res.writeHead(200, { 'Content-Type': 'image/jpeg', 'Cache-Control': 'max-age=3600' });
  p.stdout.pipe(res);
}

const json = (res, data, code = 200) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(data)); };
const body = req => new Promise(r => { let b = ''; req.on('data', d => b += d); req.on('end', () => r(JSON.parse(b || '{}'))); });

http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const q = Object.fromEntries(url.searchParams);
  // block DNS rebinding and cross-site form posts
  if (!/^(localhost|127\.0\.0\.1)(:\d+)?$/.test(req.headers.host || '')) return json(res, { error: 'forbidden' }, 403);
  if (req.method === 'POST' && req.headers['content-type'] !== 'application/json') return json(res, { error: 'forbidden' }, 403);
  try {
    switch (url.pathname) {
      case '/': res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); return fs.createReadStream(new URL('index.html', import.meta.url)).pipe(res);
      case '/icon.svg': res.writeHead(200, { 'Content-Type': 'image/svg+xml' }); return fs.createReadStream(new URL('icon.svg', import.meta.url)).pipe(res);
      case '/apple-touch-icon.png': res.writeHead(200, { 'Content-Type': 'image/png' }); return fs.createReadStream(new URL('apple-touch-icon.png', import.meta.url)).pipe(res);
      case '/api/list': return json(res, { dir: path.resolve(q.dir), files: await list(path.resolve(q.dir)) });
      case '/api/pick': {
        const { stdout } = await run('osascript', ['-e', 'POSIX path of (choose folder with prompt "Ordner wählen")']).catch(() => ({ stdout: '' }));
        return json(res, { dir: stdout.trim().replace(/(.)\/$/, '$1') || null });
      }
      case '/video': if (!VIDEO.test(q.path)) break; return sendFile(req, res, q.path);
      case '/api/strip': if (!VIDEO.test(q.path)) break; return filmstrip(res, q.path, +q.d);
      case '/api/jobs': return json(res, jobs.map(({ opts, ...j }) => j));
      case '/api/convert': enqueue(await body(req)); return json(res, { ok: true });
      case '/api/cancel': cancel(); return json(res, { ok: true });
      case '/api/reveal': await run('open', ['-R', (await body(req)).path]); return json(res, { ok: true });
    }
    json(res, { error: 'not found' }, 404);
  } catch (e) { json(res, { error: e.message }, 400); }
}).listen(PORT, '127.0.0.1', () => console.log(`video2me → http://localhost:${PORT}`));
