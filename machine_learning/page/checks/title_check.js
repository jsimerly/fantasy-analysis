// Headless check of the Rosters tab's title-odds lens: standings columns, the curve, player and target columns, the head stats.
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
    d.getElementById('tb-rosters').click();
    const box = d.getElementById('roster');
    const head = box.querySelector('.teamhead'); console.log('head:', head.textContent.replace(/\s+/g, ' ').slice(0, 160));
    const st = box.querySelector('table.standings'); console.log('standings headers:', [...st.querySelectorAll('thead th')].map(x => x.textContent).join(','));
    console.log('standings row 1:', [...st.querySelector('tbody tr').children].map(x => x.textContent).join(' | '));
    console.log('curve svg:', !!box.querySelector('figure.arch svg'), '| polylines:', box.querySelectorAll('figure.arch svg polyline').length);
    const tables = box.querySelectorAll('.rgrid table, .rgrid2 table');
    const pl = tables[1]; console.log('players headers:', [...pl.querySelectorAll('thead th')].map(x => x.textContent).join(','));
    const rows = [...pl.querySelectorAll('tbody tr')].slice(0, 3).map(r => [...r.children].map(x => x.textContent.trim()).join(' | ')); console.log('players:', rows.join('\n         '));
    const tg = tables[2]; console.log('targets headers:', [...tg.querySelectorAll('thead th')].map(x => x.textContent).join(','));
    console.log('target 1:', [...tg.querySelector('tbody tr').children].map(x => x.textContent.trim()).join(' | '));
    console.log('title cells with a value:', [...pl.querySelectorAll('tbody tr')].filter(r => /pts/.test(r.children[3].textContent)).length, 'of', pl.querySelectorAll('tbody tr').length);
    // switch league and team
    const sel = d.getElementById('league'); if (sel) { sel.value = [...sel.options].map(o => o.value).find(v => /sigma/i.test(v)) || sel.value; sel.dispatchEvent(new dom.window.Event('change')); }
    console.log('after league switch head:', d.querySelector('#roster .teamhead').textContent.replace(/\s+/g, ' ').slice(0, 120));
    console.log('nan/undefined:', (box.innerHTML.match(/NaN|undefined/g) || []).length);
  } catch (e) { errors.push('check: ' + e.stack.slice(0, 500)); }
  console.log('errors:', errors.length, errors.join(' || '));
  process.exit(0);
}, 1500);
