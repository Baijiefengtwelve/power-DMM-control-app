
/* Minimal web UI for hv_test_system.
 * - uses /ws/telemetry for state + plot
 * - uses REST endpoints for controls
 */

const $ = (id) => document.getElementById(id);

// Prevent telemetry/config refresh from overwriting user edits.
// We mark inputs as "dirty" once the user types, and only clear after a successful Save action.
const dirtyInputs = new Set();
const dirtyTs = {}; // id -> last edit ts (ms)

function markDirty(id){
  if (!id) return;
  dirtyInputs.add(id);
  dirtyTs[id] = Date.now();
}

function clearDirty(ids){
  for (const id of ids){
    dirtyInputs.delete(id);
    delete dirtyTs[id];
  }
}

function isDirty(id){
  return dirtyInputs.has(id);
}

function setValueIfNotDirty(id, value){
  const el = $(id);
  if (!el) return;
  if (isDirty(id) || document.activeElement === el) return;
  if (value === undefined || value === null) return;
  el.value = value;
}

// Keep web chart closer to desktop pyqtgraph:
// - use the same series colors from config.ini[PlotColors]
// - allow toggling series visibility
let plotColors = {};
let seriesVisibility = {
  cathode:true, gate:true, anode:true, backup:true,
  keithley_voltage:true, vacuum:true,
  gate_plus_anode:true, ratio:true,
  hv_voltage:true
};
let plotPaused = false;
let vacuumLogScale = true;

let lastPlotPayload = null;

const meterTypes = [
  {label:"阴极", key:"cathode"},
  {label:"栅极", key:"gate"},
  {label:"阳极", key:"anode"},
  {label:"收集极", key:"backup"},
  {label:"真空", key:"vacuum"},
];

async function postJSON(url, body=null){
  try{
    hint("正在执行…", true);
    const res = await fetch(url, {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: body ? JSON.stringify(body) : "{}"
    });
    const j = await res.json();
    if (j && j.ok){
      hint(j.message || "OK", true);
    }else{
      hint((j && j.message) ? j.message : "操作失败", false);
    }
    // Force refresh state once for immediate button/UI feedback
    try{
      const st = await loadState();
      if (st){
        applyUiConfig(st.ui);
        // also apply basic telemetry fields
        applyTelemetry({type:"telemetry", state:{ok:true,data:st}});
      }
    }catch(e){}
    return j;
  }catch(e){
    hint(String(e), false);
    return {ok:false,message:String(e),data:null};
  }
}

async function getJSON(url){
  const res = await fetch(url);
  return await res.json();
}

function setFlag(el, v){
  el.textContent = v ? "运行中" : "停止";
  el.style.color = v ? "#52ff9a" : "#ffd166";
}

function fmtNum(x){
  if (x === null || x === undefined) return "-";
  if (typeof x !== "number") return String(x);
  if (!isFinite(x)) return String(x);
  if (Math.abs(x) >= 1e4 || (Math.abs(x) > 0 && Math.abs(x) < 1e-3)) return x.toExponential(3);
  return x.toFixed(4);
}

function tsToStr(ts){
  if (!ts) return "-";
  try{
    const d = new Date(ts*1000); // meter ts in seconds maybe
    if (isNaN(d.getTime())) return String(ts);
    return d.toLocaleString();
  }catch(e){ return String(ts); }
}

async function refreshPorts(){
  await postJSON("/api/refresh_ports");
  const ports = await getJSON("/api/ports");
  const list = ports?.data?.ports || ports?.data?.data?.ports || ports?.data?.ports; // be tolerant
  const hvSel = $("hvPort");
  hvSel.innerHTML = "";
  (list || []).forEach(p=>{
    const opt = document.createElement("option");
    opt.value = p; opt.textContent = p;
    hvSel.appendChild(opt);
  });

  // For meter rows port select
  meterTypes.forEach(mt=>{
    const sel = $("port_"+mt.key);
    if (!sel) return;
    const cur = sel.value;
    sel.innerHTML = "";
    (list || []).forEach(p=>{
      const opt = document.createElement("option");
      opt.value = p; opt.textContent = p;
      sel.appendChild(opt);
    });
    if (cur) sel.value = cur;
  });
}

async function refreshGpibPorts(){
  const res = await getJSON("/api/gpib_ports");
  const list = (res && res.ok) ? (res.data || []) : [];
  const sel = $("keithleyPort");
  if (!sel) return;
  const cur = sel.value;
  sel.innerHTML = "";
  (list || []).forEach(p=>{
    const opt = document.createElement("option");
    opt.value = p; opt.textContent = p;
    sel.appendChild(opt);
  });
  // If nothing found, keep a fallback numeric list to allow selection
  if (!list || list.length === 0){
    for (let i=0;i<=30;i++){
      const opt = document.createElement("option");
      opt.value = String(i); opt.textContent = String(i);
      sel.appendChild(opt);
    }
  }
  if (cur && Array.from(sel.options).some(o=>o.value===cur)){
    sel.value = cur;
  }
}

function buildMeterTable(){
  const body = $("meterBody");
  body.innerHTML = "";
  for (const mt of meterTypes){
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${mt.label}</td>
      <td><select id="port_${mt.key}"></select></td>
      <td><input id="coeff_${mt.key}" type="number" step="0.0001" value="1.0"/></td>
      <td>
        <button id="btn_${mt.key}">连接</button>
      </td>
      <td id="val_${mt.key}">-</td>
      <td id="unit_${mt.key}">-</td>
      <td id="ts_${mt.key}">-</td>
    `;
    body.appendChild(tr);

    $("btn_"+mt.key).onclick = async ()=>{
      const port = $("port_"+mt.key).value;
      const coeff = parseFloat($("coeff_"+mt.key).value || "1.0");
      await postJSON("/api/meter/coeff", {meter_type: mt.key, coefficient: coeff});
      await postJSON("/api/meter/toggle", {meter_type: mt.key, port});
      $("connHint").textContent = "已发送万用表连接命令："+mt.key;
    };
  }
}

function drawSeries(canvas, t, seriesList, opts={}){
  const ctx = canvas.getContext("2d");
  const W = canvas.width, H = canvas.height;
  ctx.clearRect(0,0,W,H);

  // background grid
  ctx.globalAlpha = 0.3;
  ctx.strokeStyle = "#2a3b52";
  for(let i=1;i<5;i++){
    const y = (H*i)/5;
    ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(W,y); ctx.stroke();
  }
  ctx.globalAlpha = 1.0;

  // Collect min/max (respect visibility)
  let minY=Infinity, maxY=-Infinity;
  for(const s of seriesList){
    const key = s.key || s.name;
    if (opts.visibility && key && opts.visibility[key] === false) continue;
    for(const v of s.data){
      if (typeof v !== "number" || !isFinite(v)) continue;
      minY = Math.min(minY, v);
      maxY = Math.max(maxY, v);
    }
  }
  if (minY === Infinity){ minY=-1; maxY=1; }
  if (minY === maxY){ minY -= 1; maxY += 1; }

  // draw axes labels
  ctx.fillStyle = "#8aa0b6";
  ctx.font = "12px system-ui";
  ctx.fillText(`${fmtNum(maxY)}`, 6, 14);
  ctx.fillText(`${fmtNum(minY)}`, 6, H-6);

  const n = t.length;
  const start = Math.max(0, n-400);
  const sliceT = t.slice(start);

  function xOf(i){
    return ((i)/(sliceT.length-1 || 1))*(W-60) + 50;
  }
  function yOf(v){
    return H - 20 - ((v-minY)/(maxY-minY))*(H-40);
  }

  // Draw each series using plot colors (fallback by index)
  const fallback = ["#4aa3ff","#52ff9a","#ffd166","#ff6b6b","#b388ff","#9bf6ff"];
  const colorMap = opts.colors || {};
  seriesList.forEach((s, idx)=>{
    const key = s.key || s.name;
    if (opts.visibility && key && opts.visibility[key] === false) return;
    const data = s.data.slice(start);
    ctx.strokeStyle = (key && colorMap[key]) ? colorMap[key] : fallback[idx % fallback.length];
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    for(let i=0;i<data.length;i++){
      const v = data[i];
      if (typeof v !== "number" || !isFinite(v)) continue;
      const x = xOf(i);
      const y = yOf(v);
      if (i===0) ctx.moveTo(x,y);
      else ctx.lineTo(x,y);
    }
    ctx.stroke();

    ctx.fillStyle = (key && colorMap[key]) ? colorMap[key] : fallback[idx % fallback.length];
    ctx.fillText(s.name, W-220, 18 + idx*14);
  });

  // time range
  if (sliceT.length>=2){
    const t0 = sliceT[0], t1 = sliceT[sliceT.length-1];
    ctx.fillStyle="#8aa0b6";
    ctx.fillText(`t: ${fmtNum(t0)} ~ ${fmtNum(t1)}`, 50, H-6);
  }
}

function ensureLegendBuilt(){
  const cur = $("legendCurrent");
  const vol = $("legendVoltage");
  if (!cur || !vol) return;
  if (cur.dataset.built === "1" && vol.dataset.built === "1") return;

  const mk = (container, items)=>{
    container.innerHTML = "";
    items.forEach(it=>{
      const id = `chk_${it.key}`;
      const wrap = document.createElement("label");
      wrap.className = "legend-item";
      wrap.innerHTML = `
        <input type="checkbox" id="${id}" ${seriesVisibility[it.key] ? "checked" : ""}/>
        <span class="legend-swatch" style="background:${plotColors[it.key] || "#8aa0b6"}"></span>
        <span class="legend-name">${it.name}</span>
      `;
      container.appendChild(wrap);
      $(id).onchange = ()=>{
        seriesVisibility[it.key] = $(id).checked;
        // re-draw using cached payload
        if (lastPlotPayload) applyTelemetry(lastPlotPayload);
      };
    });
  };

  mk(cur, [
    {key:"cathode", name:"阴极"},
    {key:"gate", name:"栅极"},
    {key:"anode", name:"阳极"},
    {key:"backup", name:"收集极"},
  ]);
  cur.dataset.built = "1";

  mk(vol, [
    {key:"hv_voltage", name:"HAPS06"},
    {key:"keithley_voltage", name:"Keithley"},
    {key:"vacuum", name:"真空"},
  ]);
  vol.dataset.built = "1";
}

function applyTelemetry(payload){
  lastPlotPayload = payload;
  const state = payload.state?.data || payload.state; // tolerate wrapper
  const plot = payload.plot?.data || payload.plot;

  // Pull plot colors from config.ini[PlotColors] via /api/state
  try{
    const pc = state?.data?.ui?.plot_colors || state?.ui?.plot_colors || {};
    if (pc && Object.keys(pc).length){
      plotColors = pc;
      ensureLegendBuilt();
    }
  }catch(e){}

  // flags
  const flags = state?.data?.flags || state?.flags;
  if (flags){
    setFlag($("flagTesting"), !!flags.is_testing);
    setFlag($("flagStab"), !!flags.is_stabilizing);
    setFlag($("flagRecording"), !!flags.is_recording);
  }

  // hv/keithley
  const hv = state?.data?.hv || state?.hv;
  if (hv){
    $("hvState").textContent = hv.connected ? "已连接" : "未连接";
    setBtnState($("btnHvConnect"), !!hv.connected, "断开", "连接");
    // lock port selection when connected
    if ($("hvPort")) $("hvPort").disabled = !!hv.connected;

    $("hvVout").textContent = fmtNum(hv.voltage) + " V";
  }
  const kt = state?.data?.keithley || state?.keithley;
  if (kt){
    $("keithleyState").textContent = kt.connected ? "已连接" : "未连接";
    setBtnState($("btnKeithleyConnect"), !!kt.connected, "断开", "连接");
    if ($("keithleyPort")) $("keithleyPort").disabled = !!kt.connected;

    $("keithleyV").textContent = fmtNum(kt.voltage) + " V";
  }

  // meters
  const meters = state?.data?.meters || state?.meters;
  if (meters){
    for (const mt of meterTypes){
      const m = meters[mt.key];
      if (!m) continue;
      $("val_"+mt.key).textContent = fmtNum(m.value);
      $("unit_"+mt.key).textContent = m.unit || "-";
      $("ts_"+mt.key).textContent = tsToStr(m.timestamp);
      if (typeof m.connected !== "undefined") {
        setBtnState($("btn_"+mt.key), !!m.connected, "断开", "连接");
        const sel = $("port_"+mt.key); if (sel) sel.disabled = !!m.connected;
      }
      const c = $("coeff_"+mt.key);
      if (c && (c.value === "" || Math.abs(parseFloat(c.value)- (m.coefficient||1))<1e-12)){
        // leave as is
      }
    }
  }

  // params
  const tp = state?.data?.test_params || state?.test_params;
  if (tp){
    setValueIfNotDirty("testStartV", tp.start_voltage);
    setValueIfNotDirty("testTargetV", tp.target_voltage);
    setValueIfNotDirty("testStepV", tp.voltage_step);
    setValueIfNotDirty("testStepDelay", tp.step_delay);
    setValueIfNotDirty("testCycleTime", tp.cycle_time);
  }

  // stabilization params
  const sp = state?.data?.stabilization_params || state?.stabilization_params;
  if (sp){
    setValueIfNotDirty("stabTargetI", sp.target_current);
    setValueIfNotDirty("stabRangeI", sp.stability_range);
    setValueIfNotDirty("stabStartV", sp.start_voltage);
    setValueIfNotDirty("stabFreq", sp.adjust_frequency);
    setValueIfNotDirty("stabMaxAdjV", sp.max_adjust_voltage);
    if ($("stabSource")){
      const src = (sp.current_source || "keithley");
      setValueIfNotDirty("stabSource", src);
    }
    if ($("stabAlgo")){
      const algo = (sp.algorithm || "pid");
      setValueIfNotDirty("stabAlgo", algo);
    }
  }

  // plots
  if (!plotPaused && plot && plot.t && plot.t.length){
    const t = plot.t;
    drawSeries($("plotCurrent"), t, [
      {key:"cathode", name:"cathode", data: plot.cathode || []},
      {key:"gate", name:"gate", data: plot.gate || []},
      {key:"anode", name:"anode", data: plot.anode || []},
      {key:"backup", name:"backup", data: plot.backup || []},
    ], {colors: plotColors, visibility: seriesVisibility});
    // voltage/vacuum
    const hvv = (hv && typeof hv.voltage==="number") ? hv.voltage : null;
    const hvSeries = hvv===null ? [] : t.map(()=>hvv);
    // Map desktop color keys
    const voltageColors = Object.assign({}, plotColors, {hv_voltage: "#4aa3ff"});
    drawSeries($("plotVoltage"), t, [
      {key:"hv_voltage", name:"HAPS06 Vout", data: hvSeries},
      {key:"keithley_voltage", name:"Keithley V", data: plot.keithley_voltage || []},
      {key:"vacuum", name:(vacuumLogScale ? "log10(vacuum)" : "vacuum"), data: (vacuumLogScale ? (plot.vacuum||[]).map(v=>{v=parseFloat(v); return (v>0)?Math.log10(v):NaN;}) : (plot.vacuum||[]))},
    ], {colors: voltageColors, visibility: seriesVisibility});
  }
}

async function refreshFiles(){
  const resp = await getJSON("/api/files");
  const data = resp.data || resp?.data?.data;
  if (!data){ $("fileList").textContent = "-"; return; }
  const files = data.files || [];
  if (!files.length){ $("fileList").textContent = "无文件"; return; }
  $("fileList").innerHTML = files.map(f=>{
    const name = f.name;
    return `<div><a href="/download/${encodeURIComponent(name)}">${name}</a> <span class="muted">(${f.size||0} bytes)</span></div>`;
  }).join("");
}

async function refreshDbStats(){
  try{
    const resp = await getJSON("/api/db/stats");
    if (!resp || !resp.ok){
      $("dbStatus").textContent = (resp && resp.message) ? resp.message : "-";
      return resp;
    }
    const st = resp.data || {};
    const sizeMB = (st.size_bytes||0) / (1024*1024);
    const runs = st.runs ?? "-";
    const rows = st.rows ?? "-";
    $("dbStatus").textContent = `SQLite: ${sizeMB.toFixed(1)} MB, runs=${runs}, rows=${rows}`;
    return resp;
  }catch(e){
    $("dbStatus").textContent = String(e);
    return {ok:false,message:String(e),data:null};
  }
}

function connectWS(){
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws/telemetry`);
  ws.onmessage = (e)=>{
    try{
      const payload = JSON.parse(e.data);
      if (payload.type === "telemetry") applyTelemetry(payload);
    }catch(err){}
  };
  ws.onopen = ()=>{ $("connHint").textContent = "WebSocket 已连接"; };
  ws.onclose = ()=>{ $("connHint").textContent = "WebSocket 断开，正在重连…"; setTimeout(connectWS, 1500); };
}

function bindActions(){

  // Mark key config inputs as dirty when user edits (prevents auto-refresh overwriting)
  const dirtyIds = [
    "testStartV","testTargetV","testStepV","testStepDelay","testCycleTime",
    "stabTargetI","stabRangeI","stabStartV","stabSource","stabFreq","stabMaxAdjV","stabAlgo",
    "recordPath"
    ,"dbKeepDays","dbKeepRuns","dbArchive","dbArchiveDir","dbVacuum"
  ];
  for (const id of dirtyIds){
    const el = $(id);
    if (!el) continue;
    el.addEventListener("input", ()=>markDirty(id));
    el.addEventListener("change", ()=>markDirty(id));
  }

  $("btnRefreshPorts").onclick = async ()=>{
    await refreshPorts();
    await refreshGpibPorts();
  };

  // plot controls
  const btnPause = $("btnPlotPause");
  if (btnPause){
    btnPause.onclick = ()=>{
      plotPaused = !plotPaused;
      btnPause.textContent = plotPaused ? "继续" : "暂停";
      hint(plotPaused ? "绘图已暂停（数据仍在采集）" : "绘图已恢复");
    };
  }
  const btnClear = $("btnPlotClear");
  if (btnClear){
    btnClear.onclick = ()=>{
      lastPlotPayload = {type:"telemetry", state:lastPlotPayload?lastPlotPayload.state:null, plot:{t:[]}};
      // Clear canvases
      const c1 = $("plotCurrent"); const c2 = $("plotVoltage");
      if (c1) c1.getContext("2d").clearRect(0,0,c1.width,c1.height);
      if (c2) c2.getContext("2d").clearRect(0,0,c2.width,c2.height);
      hint("已清空曲线（仅清除网页显示）");
    };
  }
  const chkVac = $("chkVacuumLog");
  if (chkVac){
    chkVac.onchange = ()=>{
      vacuumLogScale = !!chkVac.checked;
      if (lastPlotPayload) applyTelemetry(lastPlotPayload);
    };
  }


  $("btnHvConnect").onclick = async ()=>{
    const connected = ($("btnHvConnect").textContent || "").includes("断开");
    if (!connected){
      const port = $("hvPort").value;
      await postJSON("/api/hv/connect", {port});
    }else{
      await postJSON("/api/hv/disconnect", {});
    }
  };

  $("btnKeithleyConnect").onclick = async ()=>{
    const connected = ($("btnKeithleyConnect").textContent || "").includes("断开");
    if (!connected){
      const resource_name = $("keithleyPort").value;
      await postJSON("/api/keithley/connect", {resource_name});
    }else{
      await postJSON("/api/keithley/disconnect", {});
    }
  };

  $("btnStabStart").onclick = async ()=>{ await postJSON("/api/stabilization/start"); };
  $("btnStabStop").onclick = async ()=>{ await postJSON("/api/stabilization/stop"); };

  $("btnSaveTestParams").onclick = async ()=>{
    const body = {
      start_voltage: parseFloat($("testStartV").value||"0"),
      target_voltage: parseFloat($("testTargetV").value||"0"),
      voltage_step: parseFloat($("testStepV").value||"1"),
      step_delay: parseFloat($("testStepDelay").value||"1"),
      cycle_time: parseFloat($("testCycleTime").value||"0"),
    };
    const r = await postJSON("/api/params/test", body);
    if (r && r.ok){ clearDirty(["testStartV","testTargetV","testStepV","testStepDelay","testCycleTime"]); }
  };

  const btnSaveStab = $("btnSaveStabParams");
  if (btnSaveStab){
    btnSaveStab.onclick = async ()=>{
      const body = {
        target_current: parseFloat($("stabTargetI").value || "0"),
        stability_range: parseFloat($("stabRangeI").value || "0"),
        start_voltage: parseFloat($("stabStartV").value || "0"),
        current_source: String($("stabSource").value || "keithley"),
        adjust_frequency: parseFloat($("stabFreq").value || "0"),
        max_adjust_voltage: parseFloat($("stabMaxAdjV").value || "0"),
        algorithm: String(($("stabAlgo") && $("stabAlgo").value) || "pid"),
      };
      const r = await postJSON("/api/params/stabilization", body);
      if (r && r.ok){ clearDirty(["stabTargetI","stabRangeI","stabStartV","stabSource","stabFreq","stabMaxAdjV","stabAlgo"]); }
    };
  }

  $("btnTestStart").onclick = async ()=>{ await postJSON("/api/test/start"); };
  $("btnTestStartCycle").onclick = async ()=>{ await postJSON("/api/test/start_cycle"); };
  $("btnTestStop").onclick = async ()=>{ await postJSON("/api/test/stop"); };
  $("btnResetVoltage").onclick = async ()=>{ await postJSON("/api/test/reset_voltage"); };

  $("btnSetRecordPath").onclick = async ()=>{
    const path = $("recordPath").value || "";
    const r = await postJSON("/api/record/path", {path});
    if (r && r.ok){ clearDirty(["recordPath"]); }
  };
  $("btnToggleRecording").onclick = async ()=>{ await postJSON("/api/record/toggle"); };
  $("btnRefreshFiles").onclick = async ()=>{ await refreshFiles(); };

  // SQLite maintenance
  const btnDbRefresh = $("btnDbRefresh");
  if (btnDbRefresh){
    btnDbRefresh.onclick = async ()=>{ await refreshDbStats(); };
  }
  const btnDbCleanup = $("btnDbCleanup");
  if (btnDbCleanup){
    btnDbCleanup.onclick = async ()=>{
      const body = {
        keep_days: parseInt($("dbKeepDays")?.value || "30", 10),
        keep_runs: parseInt($("dbKeepRuns")?.value || "200", 10),
        archive_before_delete: (String($("dbArchive")?.value || "1") === "1"),
        archive_dir: String($("dbArchiveDir")?.value || "data\\archive"),
        vacuum_mode: String($("dbVacuum")?.value || "incremental"),
      };
      const r = await postJSON("/api/db/cleanup", body);
      if (r && r.ok){
        clearDirty(["dbKeepDays","dbKeepRuns","dbArchive","dbArchiveDir","dbVacuum"]);
        await refreshDbStats();
      }
    };
  }

  $("btnClearChart").onclick = async ()=>{ await postJSON("/api/chart/clear"); };
}

async function loadState(){
  try{
    const res = await getJSON("/api/state");
    if (!res || !res.ok) return null;
    return res.data || null;
  }catch(e){
    return null;
  }
}

function applyUiConfig(ui){
  if (!ui) return;

  // HV
  if ($("hvPort") && ui.hv_port) $("hvPort").value = ui.hv_port;

  // Keithley
  if ($("keithleyPort") && ui.keithley_resource) $("keithleyPort").value = ui.keithley_resource;

  // Meters
  if (ui.meters){
    for (const mt of meterTypes){
      const m = ui.meters[mt.key];
      if (!m) continue;
      const sel = $("port_"+mt.key);
      if (sel && m.port) sel.value = m.port;
      const c = $("coeff_"+mt.key);
      if (c && (c.value==="" || isNaN(parseFloat(c.value)))) c.value = (m.coefficient ?? 1.0);
    }
  }

  // Record path
  setValueIfNotDirty("recordPath", ui.record_path);

  // Retention defaults
  try{
    const rp = ui.retention || {};
    setValueIfNotDirty("dbKeepDays", rp.keep_days);
    setValueIfNotDirty("dbKeepRuns", rp.keep_runs);
    setValueIfNotDirty("dbArchiveDir", rp.archive_dir);
    if ($("dbArchive") && !isDirty("dbArchive")) $("dbArchive").value = rp.archive_before_delete ? "1" : "0";
    if ($("dbVacuum") && !isDirty("dbVacuum")) $("dbVacuum").value = String(rp.vacuum_mode || "incremental");
  }catch(e){}
}

function setBtnState(btn, on, textOn, textOff){
  if (!btn) return;
  btn.textContent = on ? textOn : textOff;
  btn.classList.toggle("btn-connected", !!on);
  btn.classList.toggle("btn-disconnected", !on);
}

function hint(msg, ok=true){
  const el = $("connHint");
  if (!el) return;
  el.textContent = msg;
  el.classList.toggle("hint-ok", !!ok);
  el.classList.toggle("hint-err", !ok);
}

async function init(){
  buildMeterTable();
  bindActions();

  // 1) Load saved configuration/state first (so selects can preserve selection)
  const st = await loadState();
  if (st){
    applyUiConfig(st.ui);
    applyTelemetry({type:"telemetry", state:{ok:true,data:st}});
  }

  // 2) Populate selectable ports/resources
  await refreshPorts();
  await refreshGpibPorts();

  await refreshDbStats();

  await refreshFiles();
  connectWS();
}

init()
;
