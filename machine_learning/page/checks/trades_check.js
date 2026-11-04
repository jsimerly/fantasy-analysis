// Headless check of the Trades tab: render, sort, filter.
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
    d.getElementById('tb-trades').click();
    console.log('tab-trades hidden:', d.getElementById('tab-trades').hidden, '| other tabs hidden:', ['players', 'rosters', 'perf'].map(k => d.getElementById('tab-' + k).hidden).join(','));
    const box = d.getElementById('trades');
    const cards = [...box.querySelectorAll('.card h3')].map(h => h.textContent);
    console.log('cards:', JSON.stringify(cards));
    const tables = [...box.querySelectorAll('table')];
    console.log('tables:', tables.length, '| rows:', tables.map(t => t.querySelectorAll('tbody tr').length).join(','));
    const first = row => [...row.children].map(td => td.textContent).join(' | ');
    console.log('scorecard top:', first(tables[0].querySelector('tbody tr')));
    // sort scorecards by trades
    [...tables[0].querySelectorAll('thead th')].find(th => th.dataset.k === 'trades').click();
    let t0 = d.getElementById('trades').querySelector('table');
    console.log('after sort by trades:', first(t0.querySelector('tbody tr')), '| header:', [...t0.querySelectorAll('thead th')].filter(th => th.classList.contains('on')).map(th => th.textContent).join(''));
    // filter by manager
    const mg = d.getElementById('tr-mgr'); mg.value = 'Jacob Simerly'; mg.dispatchEvent(new dom.window.Event('change'));
    let ts = [...d.getElementById('trades').querySelectorAll('table')];
    console.log('after manager filter: log rows', ts[1].querySelectorAll('tbody tr').length, '| log top:', first(ts[1].querySelector('tbody tr')).slice(0, 200));
    // age filter any
    const mn = d.getElementById('tr-min'); mn.value = '0'; mn.dispatchEvent(new dom.window.Event('change'));
    ts = [...d.getElementById('trades').querySelectorAll('table')];
    console.log('age any: log rows', ts[1].querySelectorAll('tbody tr').length, '| scorecard rows', ts[0].querySelectorAll('tbody tr').length);
    // switch league
    const lg = d.getElementById('tr-lg'); lg.value = [...lg.options].map(o => o.value).find(v => /Sigma/.test(v)); lg.dispatchEvent(new dom.window.Event('change'));
    ts = [...d.getElementById('trades').querySelectorAll('table')];
    console.log('Sigma: scorecard rows', ts[0].querySelectorAll('tbody tr').length, '| log rows', ts[1].querySelectorAll('tbody tr').length, '| manager select reset:', d.getElementById('tr-mgr').value === '');
    console.log('patterns h4:', [...d.getElementById('trades').querySelectorAll('h4')].map(h => h.textContent.slice(0, 40)).join(' / '));
    d.getElementById('tb-perf').click();
    console.log('perf cards:', d.querySelectorAll('#perf .card').length);
  } catch (e) { errors.push('check: ' + e.stack); }
  console.log('errors:', errors.length);
  errors.slice(0, 5).forEach(e => console.log(e.split('\n').slice(0, 5).join('\n')));
  process.exit(0);
}, 1500);
