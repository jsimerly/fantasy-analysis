const fs = require('fs');
const { JSDOM, VirtualConsole } = require('jsdom');
const html = fs.readFileSync(process.argv[2], 'utf8'), data = fs.readFileSync(process.argv[3], 'utf8');
const errors = [];
const vc = new VirtualConsole();
vc.on('jsdomError', e => errors.push(String((e.detail && e.detail.stack) || e.message).slice(0, 300)));
const dom = new JSDOM(html, { runScripts: 'dangerously', virtualConsole: vc, pretendToBeVisual: true, url: 'https://example.org/',
  beforeParse(w) { w.fetch = () => Promise.resolve({ json: () => Promise.resolve(JSON.parse(data)) }); } });
setTimeout(() => {
  const d = dom.window.document;
  const heads = [...d.querySelectorAll('#hdr th')].map(t => t.textContent);
  console.log('range column:', heads.includes('WAR floor–ceiling'), '| headers:', heads.join(' | ').slice(0, 160));
  const row = [...d.querySelectorAll('#body tr')].find(tr => /Nabers/.test(tr.textContent));
  console.log('Nabers row:', row ? row.textContent.replace(/\s+/g, ' ').slice(0, 200) : 'missing');
  if (row) { row.querySelector('button.caret').click(); const det = d.querySelector('#body tr.detail'); const ths = det ? [...det.querySelectorAll('table.spans thead th')].map(t => t.textContent) : []; console.log('detail headers:', ths.join(' | ')); const r2 = det ? det.querySelectorAll('table.spans tbody tr')[2] : null; console.log('detail season 3:', r2 ? r2.textContent.replace(/\s+/g, ' ') : '-'); }
  console.log('errors:', errors.length ? errors : 'none');
}, 1500);
