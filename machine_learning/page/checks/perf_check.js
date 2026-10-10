// Headless load with a projections.json that carries performance.market; click the perf tab and
// report the rendered market block.
process.on('unhandledRejection', e => { console.log('UNHANDLED:', ((e && e.stack) || String(e)).split('\n').slice(0, 6).join(' | ')); });
const fs = require('fs');
const { JSDOM, VirtualConsole } = require('jsdom');
const html = fs.readFileSync(process.argv[2], 'utf8');
const data = fs.readFileSync(process.argv[3], 'utf8');
const vc = new VirtualConsole();
const errors = [];
vc.on('jsdomError', e => errors.push('jsdomError: ' + ((e.detail && e.detail.stack) || e.message || e)));
vc.on('error', (...a) => errors.push('console.error: ' + a.join(' ')));
const dom = new JSDOM(html, {
  runScripts: 'dangerously', virtualConsole: vc, pretendToBeVisual: true, url: 'https://example.org/',
  beforeParse(window) { window.fetch = () => Promise.resolve({ json: () => Promise.resolve(JSON.parse(data)) }); },
});
setTimeout(() => {
  const d = dom.window.document;
  try {
    console.log('players rows rendered:', d.querySelectorAll('#t tbody tr').length);
    d.getElementById('tb-perf').click();
    const perf = d.getElementById('perf');
    const cards = [...perf.querySelectorAll('.card h3')].map(h => h.textContent);
    console.log('perf cards:', JSON.stringify(cards));
    const mk = [...perf.querySelectorAll('.card')].find(c => /3b\./.test(c.querySelector('h3').textContent));
    if (mk) {
      console.log('market block tables:', mk.querySelectorAll('table').length, '| h4:', [...mk.querySelectorAll('h4')].map(h => h.textContent).join(' / '));
      console.log('first table rows:', [...mk.querySelectorAll('table')][0].querySelectorAll('tbody tr').length, '| first row:', [...mk.querySelector('table tbody tr').children].map(td => td.textContent).join(' , '));
      const last = [...mk.querySelectorAll('table')].pop();
      console.log('last table first row:', [...last.querySelector('tbody tr').children].map(td => td.textContent).join(' , '));
    } else console.log('market block: MISSING');
    d.getElementById('tb-rosters').click();
    console.log('rosters html length:', d.getElementById('roster').innerHTML.length);
  } catch (e) { errors.push('click: ' + e.stack); }
  console.log('errors:', errors.length);
  errors.slice(0, 5).forEach(e => console.log(e.split('\n').slice(0, 4).join('\n')));
  process.exit(0);
}, 1500);
