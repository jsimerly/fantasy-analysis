const fs = require('fs'); const { JSDOM, VirtualConsole } = require('jsdom');
const html = fs.readFileSync(process.argv[2], 'utf8'), data = fs.readFileSync(process.argv[3], 'utf8');
const vc = new VirtualConsole(); const errors = []; vc.on('jsdomError', e => errors.push(String(e.message).slice(0, 200)));
const dom = new JSDOM(html, { runScripts: 'dangerously', virtualConsole: vc, pretendToBeVisual: true, url: 'https://example.org/', beforeParse(w) { w.fetch = () => Promise.resolve({ json: () => Promise.resolve(JSON.parse(data)) }); } });
setTimeout(() => {
  const d = dom.window.document;
  const hdr = [...d.querySelectorAll('#hdr th')].map(th => th.textContent.trim());
  console.log('columns:', hdr.join(' | '));
  const rows = [...d.querySelectorAll('#body tr')].filter(tr => /\d{4} (Early|Mid|Late) /.test(tr.textContent));
  console.log('pick rows shown:', rows.length);
  for (const tr of rows.slice(0, 12)) console.log([...tr.children].map(td => td.textContent.trim().replace(/\s+/g, ' ')).join(' | '));
  const j = JSON.parse(data); console.log('picks.now', j.picks.now, '| rows sample', j.picks.rows.slice(0, 4), '| meta', JSON.stringify(j.picks.meta).slice(0, 300));
  console.log('errors', errors.length);
}, 1500);
