(function(){let e=document.createElement(`link`).relList;if(e&&e.supports&&e.supports(`modulepreload`))return;for(let e of document.querySelectorAll(`link[rel="modulepreload"]`))n(e);new MutationObserver(e=>{for(let t of e)if(t.type===`childList`)for(let e of t.addedNodes)e.tagName===`LINK`&&e.rel===`modulepreload`&&n(e)}).observe(document,{childList:!0,subtree:!0});function t(e){let t={};return e.integrity&&(t.integrity=e.integrity),e.referrerPolicy&&(t.referrerPolicy=e.referrerPolicy),e.crossOrigin===`use-credentials`?t.credentials=`include`:e.crossOrigin===`anonymous`?t.credentials=`omit`:t.credentials=`same-origin`,t}function n(e){if(e.ep)return;e.ep=!0;let n=t(e);fetch(e.href,n)}})();var e={mediums:[{value:`cpc`,label:`CPC`,sourceType:`select`,sources:[`yandex`,`headhunter`]},{value:`email`,label:`Email`,sourceType:`select`,sources:[`sendsay`,`hacker`,`securitylab`,`stepik`]},{value:`social`,label:`Social`,sourceType:`select`,hasPartner:!0,sources:[`telegram`,`youtube`,`yandex_dzen`,`instagram`,`rutube`,`vk`,`max`]},{value:`referral`,label:`Referral`,sourceType:`freetext`,sources:[]},{value:`website`,label:`Website`,sourceType:`select`,sources:[`banner`]}]}.mediums,t=document.querySelector(`#app`);if(!t)throw Error(`#app not found`);function n(e){return e.replace(/&/g,`&amp;`).replace(/</g,`&lt;`).replace(/>/g,`&gt;`).replace(/\"/g,`&quot;`).replace(/'/g,`&#039;`)}function r(e){return e.replaceAll(`_`,` `)}async function i(e,t){let n=await fetch(e,{cache:`no-store`,...t}),r=await n.text(),i=null;try{i=r?JSON.parse(r):null}catch{throw Error(`Unexpected response: ${r.slice(0,120)}`)}if(!n.ok){let e=typeof i==`object`&&i&&`error`in i?String(i.error):String(n.status);throw Error(e)}return i}function a(e){if(!e.length)return`<p class="muted">Пока нет записей</p>`;let t=Object.keys(e[0]);return`<div class="table-wrap"><table><thead><tr>${t.map(e=>`<th>${n(r(String(e)))}</th>`).join(``)}</tr></thead><tbody>${e.map(e=>`<tr>${t.map(t=>`<td>${n(String(e[t]??``))}</td>`).join(``)}</tr>`).join(``)}</tbody></table></div>`}function o(){t.innerHTML=`
    <main class="page">
      <section class="panel">
        <h1>UTM Builder</h1>
        <p class="muted">Отдельная страница без аналитических данных.</p>

        <div class="grid">
          <label>Medium <span class="required-marker">*</span>
            <select class="medium-select"></select>
          </label>
          <label>Source <span class="required-marker">*</span>
            <select class="source-select"></select>
            <input class="source-freetext" type="text" placeholder="Введите источник" style="display:none" />
          </label>
          <label>Name (Campaign) <span class="required-marker">*</span>
            <input class="campaign-input" type="text" placeholder="Например, spring_sale_2026" />
          </label>
          <label>Partner
            <input class="partner-input" type="text" placeholder="Опционально" />
          </label>
          <label>Link <span class="required-marker">*</span>
            <input class="link-input" type="url" placeholder="https://example.com/campaign" />
          </label>
          <label>Content
            <input class="content-input" type="text" placeholder="Например, banner_a" />
          </label>
          <label>Term
            <input class="term-input" type="text" placeholder="Например, python_course" />
          </label>
        </div>

        <div class="actions">
          <button class="write-btn" disabled>write</button>
          <span class="status muted"></span>
        </div>

        <div class="preview muted"></div>
        <div class="rows"></div>
      </section>
    </main>
  `}async function s(){o();let r=t.querySelector(`.medium-select`),s=t.querySelector(`.source-select`),c=t.querySelector(`.source-freetext`),l=t.querySelector(`.campaign-input`),u=t.querySelector(`.partner-input`),d=t.querySelector(`.link-input`),f=t.querySelector(`.content-input`),p=t.querySelector(`.term-input`),m=t.querySelector(`.write-btn`),h=t.querySelector(`.status`),g=t.querySelector(`.preview`),_=t.querySelector(`.rows`),v=(e,t=`muted`)=>{h.textContent=e,h.className=`status ${t}`},y=async()=>{try{_.innerHTML=a(await i(`/api/utm`))}catch(e){_.innerHTML=`<p class="status error">Ошибка загрузки истории: ${n(String(e))}</p>`}},b=t=>{let r=e.find(e=>e.value===t);if(!r){s.innerHTML=``,s.style.display=``,c.style.display=`none`,c.value=``,u.value=``,u.disabled=!0;return}r.sourceType===`freetext`?(s.style.display=`none`,c.style.display=``,c.value=``):(s.style.display=``,c.style.display=`none`,c.value=``,s.innerHTML=r.sources.map(e=>`<option value="${n(e)}">${n(e)}</option>`).join(``)),r.hasPartner?(u.disabled=!1,u.placeholder=`Partner (опционально)`):(u.value=``,u.disabled=!0,u.placeholder=`Недоступно для выбранного medium`)},x=()=>{let t=e.find(e=>e.value===r.value)?.sourceType===`freetext`?c.value:s.value;m.disabled=![r.value,t,l.value,d.value].every(e=>String(e||``).trim()!==``)};r.innerHTML=e.map(e=>`<option value="${n(e.value)}">${n(e.label||e.value)}</option>`).join(``),b(r.value),r.onchange=()=>{b(r.value),x()},s.onchange=x,c.oninput=x,l.oninput=x,d.oninput=x,f.oninput=x,p.oninput=x,u.oninput=x,m.onclick=async()=>{let t=e.find(e=>e.value===r.value),n=t?.sourceType===`freetext`?c.value.trim():s.value.trim(),a=t?.hasPartner?u.value.trim():``,o=[l.value.trim(),a].filter(Boolean).join(`|`),h={utm_medium:r.value.trim(),utm_source:n,utm_campaign:o,campaign_link:d.value.trim(),utm_content:f.value.trim(),utm_term:p.value.trim()};if(!h.utm_medium||!h.utm_source||!h.utm_campaign||!h.campaign_link){v(`Заполните обязательные поля`,`error`);return}v(`Сохраняю...`),m.disabled=!0;try{let e=await i(`/api/utm`,{method:`POST`,headers:{"content-type":`application/json`},body:JSON.stringify(h)});g.textContent=String(e.row?.[`UTM Tag`]||e.utm_tag||``)||`Тег не вернулся в ответе`,v(`Сохранено`,`success`),await y()}catch(e){v(`Ошибка записи: ${String(e)}`,`error`)}finally{x()}},g.textContent=`После записи здесь появится UTM тег`,await y(),x()}s();