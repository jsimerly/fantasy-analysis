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
  d.getElementById('tb-perf').click();
  const cards = [...d.querySelectorAll('#perf .card h3')].map(h => h.textContent.slice(0, 48));
  const card = [...d.querySelectorAll('#perf .card')].find(c => /Inside TabPFN/.test(c.textContent));
  console.log('perf cards:', cards.length, '|', cards.join(' / '));
  console.log('tabpfn card:', !!card, '| svg:', card && card.querySelectorAll('svg').length, '| figcaption:', card && card.querySelector('figcaption').textContent.slice(0, 60), '| grid rows:', card && card.querySelectorAll('.arch-grid b').length);
  console.log('svg texts:', card && card.querySelectorAll('svg text').length, '| marker:', card && !!card.querySelector('#arch-arrow'));
  console.log('header:', d.querySelector('.sub') ? d.querySelector('.sub').textContent.slice(-60) : '-');
  console.log('errors:', errors.length ? errors : 'none');
}, 1500);
