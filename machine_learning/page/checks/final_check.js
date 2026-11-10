// Headless check of the Trades (winner-left cards) and Draft slots tabs.
const fs = require('fs');
const { JSDOM, VirtualConsole } = require('jsdom');
const html = fs.readFileSync(process.argv[2], 'utf8'), data = fs.readFileSync(process.argv[3], 'utf8');
const errors = [];
const vc = new VirtualConsole();
vc.on('jsdomError', e => errors.push(String((e.detail && e.detail.stack) || e.message).slice(0, 300)));
const dom = new JSDOM(html, { runScripts: 'dangerously', virtualConsole: vc, pretendToBeVisual: true, url: 'https://example.org/',
  beforeParse(w) { w.fetch = () => Promise.resolve({ json: () => Promise.resolve(JSON.parse(data)) }); } });
setTimeout(() => {
  try {
    const d = dom.window.document;
    d.getElementById('tb-trades').click();
    const so = d.getElementById('tr-sort'); so.value = 'lopsided'; so.dispatchEvent(new dom.window.Event('change'));
    const cs = [...d.querySelectorAll('#trades .trc')];
    const leftName = c => c.querySelector('.side.a .mgr').firstChild.textContent.trim();
    const decided = cs.filter(c => c.querySelector('.lean.a') || c.querySelector('.lean.b'));
    console.log('cards', cs.length, '| decided', decided.length, '| winner always left:', decided.every(c => c.querySelector('.lean').textContent.startsWith(leftName(c))));
    const c = cs[0];
    console.log('top:', c.querySelector('.mid .date').textContent, '|', leftName(c), '|', c.querySelector('.lean').textContent, '|', c.querySelector('.mid .k').textContent.slice(0, 90));
    const rev = cs.find(x => x.classList.contains('rev'));
    console.log('reversal card:', rev ? rev.querySelector('.side.a .assets').textContent + ' / ' + rev.querySelector('.side.b .assets').textContent : 'none');
    const unp = cs.find(x => /not priced by KTC/.test(x.textContent));
    console.log('unpriced example:', unp ? (unp.textContent.match(/not priced by KTC: [^·]*/) || [''])[0].slice(0, 80) : 'none');
    console.log('sort options:', [...d.querySelectorAll('#tr-sort option')].map(o => o.textContent).join(' / '));
    d.getElementById('tb-slots').click();
    const sb = d.getElementById('slots');
    console.log('slots cards:', sb.querySelectorAll('.card').length, '| team rows:', sb.querySelector('table').querySelectorAll('tbody tr').length);
    console.log('first team row:', [...sb.querySelector('table tbody tr').children].map(x => x.textContent.slice(0, 24)).join(' | '));
    console.log('heat rows:', sb.querySelectorAll('table.heat tbody tr').length, '| calibration rows:', sb.querySelectorAll('.card')[1].querySelectorAll('tbody tr').length);
    d.getElementById('tb-rosters').click(); console.log('rosters ok:', d.getElementById('roster').innerHTML.length > 1000);
    d.getElementById('tb-perf').click(); console.log('perf cards:', d.querySelectorAll('#perf .card').length);
  } catch (e) { errors.push('check: ' + e.stack.slice(0, 300)); }
  console.log('errors:', errors.length, errors.join(' || '));
  process.exit(0);
}, 1500);
