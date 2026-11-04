const fs = require('fs');
const { JSDOM, VirtualConsole } = require('jsdom');
const html = fs.readFileSync(process.argv[2], 'utf8'), data = fs.readFileSync(process.argv[3], 'utf8');
const errors = []; const vc = new VirtualConsole();
vc.on('jsdomError', e => errors.push(String((e.detail && e.detail.stack) || e.message).slice(0, 300)));
const dom = new JSDOM(html, { runScripts: 'dangerously', virtualConsole: vc, pretendToBeVisual: true, url: 'https://example.org/',
  beforeParse(w) { w.fetch = () => Promise.resolve({ json: () => Promise.resolve(JSON.parse(data)) }); } });
setTimeout(() => {
  try {
    const d = dom.window.document;
    d.getElementById('tb-trades').click();
    const mn = d.getElementById('tr-min'); mn.value = '0'; mn.dispatchEvent(new dom.window.Event('change'));
    const all = [...d.querySelectorAll('#trades .trc')], multi = all.filter(c => c.classList.contains('multi'));
    console.log('cards', all.length, '| multi-team cards', multi.length);
    const m = multi[0];
    console.log('date:', m.querySelector('.mid .date').textContent);
    [...m.querySelectorAll('.side')].forEach(s => console.log('  side:', s.querySelector('.mgr').textContent, '|', [...s.querySelectorAll('.assets span')].map(x => x.textContent).join(', ').slice(0, 90), '|', s.querySelector('.k:last-child').textContent.slice(0, 80)));
    console.log('meta line:', d.getElementById('meta').textContent.slice(-60));
  } catch (e) { errors.push('check: ' + e.stack.slice(0, 300)); }
  console.log('errors:', errors.length, errors.join(' || ')); process.exit(0);
}, 1500);
