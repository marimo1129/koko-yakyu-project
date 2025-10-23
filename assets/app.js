window.goHome=()=>location.href='./index.html';
window.goBack=()=>history.length>1?history.back():goHome();
document.addEventListener('DOMContentLoaded',()=>{
  if(!location.pathname.endsWith('alumni.html'))return;
  const list=document.getElementById('alumniList');
  const players=[
    {name:'佐藤一郎',school:'報徳学園',year:2024},
    {name:'田中太郎',school:'明石商業',year:2023},
    {name:'鈴木大輝',school:'大阪桐蔭',year:2024}
  ];
  players.forEach(p=>{
    const div=document.createElement('div');
    div.className='card';
    div.innerHTML=`<h3>${p.name}</h3><p>${p.school}</p><p>卒業年:${p.year}</p>`;
    list.appendChild(div);
  });
});
// ===== チーム一覧 初期化 =====
(async function initTeams(){
  if (!location.pathname.endsWith('teams.html')) return;

  const teams = await fetchCSV('./data/teams.csv');

  // ランクを数値化（並び替え用）
  const rankScore = (v) => {
    const m = String(v||'').trim();
    if (m === '優勝') return 4;
    if (m === '準優勝') return 3;
    if (m === 'ベスト4') return 2;
    if (m === 'ベスト8') return 1;
    return 0;
  };
  teams.forEach(t=>{
    t.prefectural_rank = rankScore(t.prefectural_result);
    t.area_rank = rankScore(t.area_result);
  });

  const $q = qs('#tq');
  const $r = qs('#tregion');
  const $p = qs('#tpref');
  const $s = qs('#tsort');
  const $list = qs('#teamList');
  const $cnt = qs('#tcount');

  const regions = uniq(teams.map(t=>t.region).filter(Boolean)).sort();
  const prefs   = uniq(teams.map(t=>t.prefecture).filter(Boolean)).sort();
  fillSelect($r, regions);
  fillSelect($p, prefs);

  const state = { q:'', region:'', pref:'', sort:'-prefectural_rank' };

  [$q,$r,$p,$s].forEach(el=> el && el.addEventListener('input', ()=>{
    state.q = $q.value.trim();
    state.region = $r.value;
    state.pref = $p.value;
    state.sort = $s.value;
    render();
  }));

  window.resetTeamsFilters = function(){
    $q.value=''; $r.value=''; $p.value=''; $s.value='-prefectural_rank';
    state.q=''; state.region=''; state.pref=''; state.sort='-prefectural_rank';
    render();
  };

  function render(){
    let rows = teams.slice();

    // フィルタ
    if (state.q){
      const q = state.q.toLowerCase();
      rows = rows.filter(t=>[
        t.team_name,t.prefecture,t.region,t.note,t.area,t.area_round,t.prefectural_result,t.area_result
      ].some(v=> (v||'').toLowerCase().includes(q)));
    }
    if (state.region) rows = rows.filter(t=> t.region===state.region);
    if (state.pref)   rows = rows.filter(t=> t.prefecture===state.pref);

    // 並び替え
    const key = state.sort.replace('-','');
    const dir = state.sort.startsWith('-')? -1: 1;
    rows.sort((a,b)=>{
      const av = (key==='prefectural_rank' || key==='area_rank') ? Number(a[key]||0) : (a[key]||'');
      const bv = (key==='prefectural_rank' || key==='area_rank') ? Number(b[key]||0) : (b[key]||'');
      return av>bv? dir: av<bv? -dir: 0;
    });

    $cnt.textContent = `${rows.length} 校`;

    $list.innerHTML='';
    const tpl = qs('#teamCardTpl');
    rows.forEach(t=>{
      const node = tpl.content.cloneNode(true);
      node.querySelector('.title').textContent = `${t.team_name}（${t.prefecture}・${t.region}）`;
      node.querySelector('.meta').textContent =
        `県大会: ${t.prefectural_result || '—'} ／ エリア(${t.area||'—'}): ${t.area_result || '—'} ${t.area_round? '・'+t.area_round : ''}`;
      node.querySelector('.meta2').textContent = t.note || '';
      $list.appendChild(node);
    });
  }

  render();
  
// ===== 卒業生一覧 初期化（players.csv を表示）=====
(function initAlumniPage(){
  // URLのどこかに alumni.html が含まれていれば実行
  if (!location.pathname.includes('alumni.html')) return;

  async function renderAlumni(){
    // PapaParseで確実に読む（BOM・改行・カンマ全部ケア）
    const res = await fetch('../data/players.csv');
    const txt = await res.text();
    const data = Papa.parse(txt, { header: true, skipEmptyLines: true }).data
      .filter(r => (r.player_name || '').trim());

    const list = document.querySelector('#alumniList'); // ← HTMLのIDと一致
    list.innerHTML = '';

    data.forEach(r => {
      const node = document.createElement('article');
      node.className = 'card';
      node.innerHTML = `
        <div class="content">
          <h3 class="title">${r.player_name || '—'}（${r.grade || ''}年・${r.position || ''}）</h3>
          <p class="meta">${r.team_name || '—'}（${r.prefecture || ''}）</p>
          <p class="meta2">卒業年: ${r.graduation_year || '—'}｜進路: ${(r.dest_type||'—')}${r.dest_name ? '・'+r.dest_name : ''}</p>
          ${ (r.comment && r.comment.trim())
              ? `<p class="muted" style="margin-top:6px">${r.comment}</p>` : `` }
        </div>
      `;

      // YouTube（watch?v= / youtu.be / embed すべてOK）
      if ((r.youtube_url || '').trim()){
        const id = (function(url){
          const s = String(url);
          const m = s.match(/[?&]v=([\w-]{6,})|youtu\.be\/([\w-]{6,})|\/embed\/([\w-]{6,})/);
          return m ? (m[1] || m[2] || m[3]) : '';
        })(r.youtube_url);
        if (id){
          const wrap = document.createElement('div');
          wrap.className = 'embed';
          wrap.innerHTML = `<iframe src="https://www.youtube.com/embed/${id}" title="YouTube video" frameborder="0" allowfullscreen loading="lazy"></iframe>`;
          node.querySelector('.content').appendChild(wrap);
        }
      }

      list.appendChild(node);
    });
  }

  renderAlumni();
})();
// ==============================
// 試合結果（大会別スコア表示用）
// ==============================
(function initMatchesPage() {
  async function loadCSV(path) {
    const res = await fetch(path);
    const text = await res.text();
    const [header, ...rows] = text.trim().split(/\r?\n/);
    const cols = header.split(",");
    return rows.map(r => {
      const vals = r.split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/);
      const obj = {};
      cols.forEach((c, i) => (obj[c] = vals[i]?.replace(/^"|"$/g, "")));
      return obj;
    });
  }

  async function renderMatches() {
    const matches = await loadCSV("data/matches.csv");
    const byTournament = {};

    for (const m of matches) {
      (byTournament[m.tournament] ||= []).push(m);
    }

    const root = document.querySelector("#app");
    if (!root) return;
    root.innerHTML = "";

    for (const [tname, rows] of Object.entries(byTournament)) {
      const sec = document.createElement("section");
      sec.innerHTML = `<h2 class="title">${tname}（${rows[0]?.year ?? ""}）</h2>`;

      const byPref = {};
      for (const r of rows) {
        const key = r.prefecture || "（全国）";
        (byPref[key] ||= []).push(r);
      }

      for (const [pref, rlist] of Object.entries(byPref)) {
        const box = document.createElement("div");
        box.innerHTML = `<h3>${pref}</h3>`;
        const tbl = document.createElement("table");
        tbl.className = "table";
        tbl.innerHTML = `
          <thead><tr>
            <th>日付</th><th>ステージ</th><th>対戦</th><th>スコア</th><th>勝者</th>
          </tr></thead><tbody></tbody>`;
        const tb = tbl.querySelector("tbody");

        rlist.sort((a, b) => (a.date || "").localeCompare(b.date || ""));
        for (const r of rlist) {
          const tr = document.createElement("tr");
          const score = r.score_a && r.score_b ? `${r.score_a}-${r.score_b}` : "";
          tr.innerHTML = `
            <td>${r.date || ""}</td>
            <td>${r.stage || ""}</td>
            <td><a href="${r.source_url}" target="_blank" rel="noopener">${r.team_a} vs ${r.team_b}</a></td>
            <td>${score}</td>
            <td>${r.winner || ""}</td>`;
          tb.appendChild(tr);
        }

        box.appendChild(tbl);
        sec.appendChild(box);
      }
      root.appendChild(sec);
    }
  }

  // index.html / teams.html のどちらかで自動発火
  if (document.querySelector("#app")) {
    renderMatches();
  }
})();
