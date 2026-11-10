// Headless check of the Team value tab: tabs, chart lines, legend, highlight, hover standings, metric switch, league switch, table.
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
    d.getElementById('tb-teamvalue').click();
    console.log('hidden:', ['players', 'rosters', 'perf', 'trades', 'slots', 'teamvalue'].map(k => k + '=' + d.getElementById('tab-' + k).hidden).join(' '));
    const box = d.getElementById('teamvalue');
    const paths = box.querySelectorAll('svg path'), legend = box.querySelectorAll('.tvl button');
    console.log('league:', d.getElementById('tv-lg').value, '| metric:', d.getElementById('tv-metric').value, '| options:', [...d.querySelectorAll('#tv-metric option')].map(o => o.value).join(','));
    console.log('lines:', paths.length, '| legend:', legend.length, '| first legend:', legend[0].textContent.trim(), '| you:', [...legend].filter(b => /\(you\)/.test(b.textContent)).map(b => b.textContent.trim()).join(';'));
    console.log('bands:', box.querySelectorAll('svg rect').length - 1, '| draft lines:', box.querySelectorAll('svg line[stroke-dasharray]').length, '| x labels:', [...box.querySelectorAll('svg text')].filter(t => /\d{2}$/.test(t.textContent) && /^[A-Z][a-z]{2} /.test(t.textContent)).map(t => t.textContent).join(','));
    console.log('count:', d.getElementById('tv-count').textContent, '| table rows:', box.querySelectorAll('table tbody tr').length, '| mine rows:', box.querySelectorAll('table tbody tr.mine').length);
    console.log('row 1:', [...box.querySelector('table tbody tr').children].map(x => x.textContent.trim()).join(' | '));
    // hover
    const hit = box.querySelector('#tv-hit'); const ev = new dom.window.MouseEvent('mousemove', { clientX: 500, clientY: 100, bubbles: true }); hit.dispatchEvent(ev);
    const tip = box.querySelector('#tv-tip'); console.log('tip shown:', tip.style.display, '|', tip.querySelector('b').textContent, '| rows:', tip.querySelectorAll('div').length, '| top:', tip.querySelector('div').textContent.trim());
    // highlight
    legend[1].click();
    const after = d.getElementById('teamvalue'); const widths = [...after.querySelectorAll('svg path')].map(p => p.getAttribute('stroke-width'));
    console.log('highlight widths:', widths.join(','), '| off legend:', after.querySelectorAll('.tvl button.off').length);
    // metric switch
    const ms = d.getElementById('tv-metric'); ms.value = 'rel_mean'; ms.dispatchEvent(new dom.window.Event('change'));
    console.log('rel_mean y labels:', [...d.querySelectorAll('#teamvalue svg text')].slice(0, 6).map(t => t.textContent).join(','), '| ref line:', d.querySelectorAll('#teamvalue svg line[opacity="0.35"]').length);
    ms.value = 'fc_value'; ms.dispatchEvent(new dom.window.Event('change'));
    console.log('fc:', d.getElementById('tv-count').textContent, '| lines:', d.querySelectorAll('#teamvalue svg path').length);
    // league switch
    const ls = d.getElementById('tv-lg'); ls.value = [...ls.options].map(o => o.value).find(v => /Sigma/.test(v)); ls.dispatchEvent(new dom.window.Event('change'));
    console.log('switched:', d.querySelector('#teamvalue h3').textContent, '|', d.getElementById('tv-count').textContent, '| metric now:', d.getElementById('tv-metric').value);
    d.getElementById('tb-players').click(); console.log('players rows:', d.querySelectorAll('#body tr').length > 50);
  } catch (e) { errors.push('check: ' + e.stack.slice(0, 400)); }
  console.log('errors:', errors.length, errors.join(' || '));
  process.exit(0);
}, 1500);
