const fs = require('fs'); const { JSDOM, VirtualConsole } = require('jsdom');
const html = fs.readFileSync(process.argv[2], 'utf8'), data = fs.readFileSync(process.argv[3], 'utf8');
const errors = []; const vc = new VirtualConsole(); vc.on('jsdomError', e => errors.push(String((e.detail && e.detail.stack) || e.message).slice(0, 300)));
const dom = new JSDOM(html, { runScripts: 'dangerously', virtualConsole: vc, pretendToBeVisual: true, url: 'https://example.org/', beforeParse(w) { w.fetch = () => Promise.resolve({ json: () => Promise.resolve(JSON.parse(data)) }); } });
setTimeout(() => { try {
  const d = dom.window.document; const hdr = [...d.querySelectorAll('#hdr th')].map(x => x.textContent);
  console.log('as_of:', (d.getElementById('meta').textContent || '').slice(0, 24), '| headers:', hdr.join(','));
  const i = hdr.indexOf('Season'); const rows = [...d.querySelectorAll('#body tr')].filter(r => r.children.length > 5);
  const show = r => r.children[1].textContent.replace('▸', '').trim().slice(0, 18) + ' ' + r.children[2].textContent + ' age ' + r.children[3].textContent + ' -> ' + r.children[i].textContent.trim() + ' [' + (r.children[i].querySelector('.mis') || {}).className + ']';
  console.log(rows.slice(0, 6).map(show).join('\n'));
  const pick = rows.find(r => r.children[2].textContent === 'PICK'); console.log('pick:', pick ? show(pick) : 'none in view');
  const old = rows.find(r => Number(r.children[3].textContent) >= 29); console.log('29+:', old ? show(old) : 'none');
  d.querySelector('#hdr th[data-key="season_pct"]').click(); console.log('sorted by season:', [...d.querySelectorAll('#body tr')].slice(0, 3).map(r => r.children[1].textContent.replace('▸', '').trim().slice(0, 16) + ' ' + r.children[i].textContent.trim()).join(' | '));
} catch (e) { errors.push('check: ' + e.stack.slice(0, 400)); } console.log('errors:', errors.length, errors.join(' || ')); process.exit(0); }, 1500);
