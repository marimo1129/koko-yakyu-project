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
