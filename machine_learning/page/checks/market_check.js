// Headless check of the Market tab: tab wiring, the seasonality heat-maps, the edge list, momentum, age, pick cycle svg, big weeks, injuries.
const fs = require('fs');
const { JSDOM, VirtualConsole } = require('jsdom');
const html = fs.readFileSync(process.argv[2], 'utf8'), data = fs.readFileSync(process.argv[3], 'utf8');
const errors = [];
const vc = new VirtualConsole();
vc.on('jsdomError', e => errors.push(String((e.detail && e.detail.stack) || e.message).slice(0, 300)));
const dom = new JSDOM(html, { runScripts: 'dangerously', virtualConsole: vc, pretendToBeVisual: true, url: 'https://example.org/',
  beforeParse(w) { w.fetch = () => Promise.resolve({ json: () => Promise.resolve(JSON.parse(data)) }); w.SVGElement.prototype.getBoundingClientRect = () => ({ left: 0, top: 0, width: 960, height: 420 }); } });
setTimeout(() => {
  try {
    const d = dom.window.document;
    console.log('tabs:', [...d.querySelectorAll('.tabs button')].map(b => b.textContent).join(' / '));
    d.getElementById('tb-market').click();
    console.log('hidden:', ['players', 'rosters', 'perf', 'trades', 'slots', 'teamvalue', 'market'].map(k => k + '=' + d.getElementById('tab-' + k).hidden).join(' '));
    const box = d.getElementById('market');
    console.log('cards:', box.querySelectorAll('.card').length, '| h3:', [...box.querySelectorAll('h3')].map(h => h.textContent.slice(0, 40)).join(' / '));
    const heats = box.querySelectorAll('table.heat');
    console.log('heat tables:', heats.length, '| rows:', [...heats].map(t => t.querySelectorAll('tbody tr').length).join(','), '| first row:', [...heats[0].querySelector('tbody tr').children].map(x => x.textContent).join(' '));
    const cells = box.querySelectorAll('td.hc'); const pos = box.querySelectorAll('td.hc.pos').length, neg = box.querySelectorAll('td.hc.neg').length, dim = box.querySelectorAll('td.hc.dim').length;
    console.log('heat cells:', cells.length, 'pos', pos, 'neg', neg, 'blank', dim, '| sample style:', cells[0].getAttribute('style'), cells[0].getAttribute('title'));
    console.log('edges:', box.querySelectorAll('.edges li').length, '| first:', box.querySelector('.edges li').textContent.slice(0, 60));
    const tables = box.querySelectorAll('table:not(.heat)');
    console.log('other tables:', tables.length, '| rows:', [...tables].map(t => t.querySelectorAll('tbody tr').length).join(','));
    console.log('momentum row1:', [...tables[0].querySelector('tbody tr').children].map(x => x.textContent).join(' | '));
    console.log('big moves row1:', [...tables[1].querySelector('tbody tr').children].map(x => x.textContent).join(' | '));
    console.log('age row1:', [...tables[2].querySelector('tbody tr').children].map(x => x.textContent).join(' | '));
    const svg = box.querySelector('svg'); console.log('pick svg:', !!svg, '| polylines:', svg && svg.querySelectorAll('polyline').length, '| texts:', svg && [...svg.querySelectorAll('text')].map(t => t.textContent).join(','));
    console.log('polyline pts:', svg && svg.querySelector('polyline').getAttribute('points').slice(0, 80));
    const inj = [...tables].slice(-3); console.log('injury rows:', inj.map(t => t.querySelectorAll('tbody tr').length).join(','), '| row1:', [...inj[0].querySelector('tbody tr').children].map(x => x.textContent).join(' | '));
    console.log('footer:', box.lastElementChild.textContent.slice(0, 120));
    console.log('nan/undefined:', (box.innerHTML.match(/NaN|undefined|null/g) || []).length);
    d.getElementById('tb-players').click(); console.log('players rows:', d.querySelectorAll('#body tr').length > 50);
  } catch (e) { errors.push('check: ' + e.stack.slice(0, 400)); }
  console.log('errors:', errors.length, errors.join(' || '));
  process.exit(0);
}, 1500);
