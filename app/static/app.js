// 原生 JS：统一读取页面 data-endpoint，提交表单并渲染结果；无框架、无打包器。
(function () {
  function qs(sel, root) { return (root || document).querySelector(sel); }
  function qsa(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }

  // 1) 分段控件（结果页 5 段 / 旧版 .tabs 同样适用）
  qsa('[data-tab-target]').forEach(function (btn) {
    btn.addEventListener('click', function () {
      var group = btn.closest('.segmented') || btn.closest('.tabs');
      if (group) qsa('button', group).forEach(function (b) { b.classList.remove('active'); });
      btn.classList.add('active');
      var target = document.getElementById(btn.getAttribute('data-tab-target'));
      var host = btn.closest('.panel') || document;
      if (!target) return;
      qsa('.tab-panel', host).forEach(function (panel) { panel.hidden = (panel !== target); });
    });
  });

  // 2) 统一 POST JSON
  window.postJSON = async function (url, payload) {
    var res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload || {})
    });
    var data = null;
    try { data = await res.json(); } catch (e) { data = { detail: '响应不是 JSON' }; }
    if (!res.ok) {
      var msg = (data && (data.detail || data.message)) || ('请求失败：' + res.status);
      if (typeof msg !== 'string') msg = JSON.stringify(msg);
      throw new Error(msg);
    }
    return data;
  };

  window.showError = function (host, err) {
    if (!host) return;
    host.hidden = false;
    host.textContent = '执行失败：' + (err && err.message ? err.message : String(err));
  };

  // 3) 表格导出 CSV（只导出当前可见行，带 BOM 便于 Excel 识别 UTF-8）
  function exportTable(table, filename) {
    if (!table) return;
    var rows = qsa('tr', table).filter(function (tr) {
      return !tr.hidden && tr.offsetParent !== null;
    });
    var lines = rows.map(function (tr) {
      return qsa('th,td', tr).map(function (cell) {
        var text = (cell.innerText || cell.textContent || '').replace(/\s+/g, ' ').trim();
        return '"' + text.replace(/"/g, '""') + '"';
      }).join(',');
    });
    var blob = new Blob(['\ufeff' + lines.join('\r\n')], { type: 'text/csv;charset=utf-8' });
    var url = URL.createObjectURL(blob);
    var link = document.createElement('a');
    link.href = url;
    link.download = filename || ((table.id || 'table') + '.csv');
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    setTimeout(function () { URL.revokeObjectURL(url); }, 1000);
  }

  qsa('[data-export-csv]').forEach(function (btn) {
    btn.addEventListener('click', function () {
      var table = document.getElementById(btn.getAttribute('data-export-csv'));
      exportTable(table, btn.getAttribute('data-export-name'));
    });
  });

  // 4) 复核页：变更日志关键字过滤 + 命中计数（纯前端，不新增接口）
  var search = qs('#review-search');
  var logTable = qs('#changelog-table');
  if (search && logTable) {
    var stat = qs('#review-stat');
    var total = qsa('tbody tr', logTable).length;
    var applyFilter = function () {
      var needle = search.value.trim().toLowerCase();
      var shown = 0;
      qsa('tbody tr', logTable).forEach(function (tr) {
        var haystack = ((tr.getAttribute('data-search') || '') + ' ' + tr.textContent).toLowerCase();
        var hit = !needle || haystack.indexOf(needle) !== -1;
        tr.hidden = !hit;
        if (hit) shown += 1;
      });
      if (stat) {
        stat.textContent = needle
          ? '命中 ' + shown + ' / ' + total + ' 条'
          : '本页 ' + total + ' 条';
      }
    };
    search.addEventListener('input', applyFilter);
    search.addEventListener('keydown', function (event) {
      if (event.key === 'Escape') { search.value = ''; applyFilter(); }
    });
    applyFilter();
  }

  // --- 工作台：执行合并 + 进度 + 即时摘要 --------------------------------
  var form = qs('#merge-form');
  if (!form) return;

  var btn = qs('#run-btn');
  var errBox = qs('#form-error');
  var steps = qsa('#progress-steps li');
  var note = qs('#progress-note');
  var summaryCard = qs('#summary-card');
  var metricsBox = qs('#summary-metrics');
  var gateBox = qs('#summary-gate');
  var resultLink = qs('#result-link');
  var snapshotLink = qs('#snapshot-link');
  var timer = null;

  function resetSteps() {
    steps.forEach(function (li) { li.classList.remove('done', 'active', 'failed'); });
  }

  function startProgress() {
    resetSteps();
    var index = 0;
    steps[0].classList.add('active');
    note.textContent = '正在执行：' + steps[0].textContent.trim();
    timer = setInterval(function () {
      if (index < steps.length - 1) {
        steps[index].classList.remove('active');
        steps[index].classList.add('done');
        index += 1;
        steps[index].classList.add('active');
        note.textContent = '正在执行：' + steps[index].textContent.trim();
      }
    }, 180);
  }

  function finishProgress(ok, message) {
    if (timer) { clearInterval(timer); timer = null; }
    steps.forEach(function (li) {
      li.classList.remove('active');
      li.classList.add(ok ? 'done' : 'failed');
    });
    note.textContent = message;
  }

  function kpi(value, label, note) {
    var box = document.createElement('div');
    box.className = 'kpi';
    var l = document.createElement('div');
    l.className = 'k-label';
    l.textContent = label;
    var v = document.createElement('div');
    v.className = 'k-value';
    if (String(value).length > 8) {
      v.style.fontSize = '13px';
      v.style.fontFamily = 'var(--font-mono)';
      v.style.letterSpacing = '0';
    }
    v.textContent = value;
    var n = document.createElement('div');
    n.className = 'k-note';
    n.textContent = note || '';
    box.appendChild(l);
    box.appendChild(v);
    if (note) box.appendChild(n);
    return box;
  }

  var VERDICT_ZH = {
    compatible: '可放行',
    compatible_with_backfill: '放行+回填',
    blocked: '阻断'
  };

  function renderSummary(data) {
    var s = data.summary;
    var published = (s.published_version || {});
    var versionText = Object.keys(published).map(function (table) {
      return table + ' v' + published[table];
    }).join(' · ') || '—';
    metricsBox.innerHTML = '';
    metricsBox.appendChild(kpi(s.alive_rows, '合并后最终行数', '存活键'));
    metricsBox.appendChild(kpi(s.conflicts, '同版本冲突', '需人工裁决'));
    metricsBox.appendChild(kpi(s.pending || 0, '待补迟到事件', '未改写已发布结果'));
    metricsBox.appendChild(kpi(s.watermark, '全局水位线（UTC）', 'min(每键最大 ts)'));
    metricsBox.appendChild(kpi(s.open_keys, '未闭合键', '水位线之上'));
    metricsBox.appendChild(kpi(s.tombstones, '删除墓碑', '保留不清理'));
    metricsBox.appendChild(kpi(s.pending_tombstone || 0, '待补中的墓碑后事件', '补入时按墓碑规则忽略'));
    metricsBox.appendChild(kpi(s.deduped_silent, '同内容静默去重', '不计冲突'));
    metricsBox.appendChild(kpi(versionText, '已发布版本', '每表一条发布指针'));
    metricsBox.appendChild(kpi(VERDICT_ZH[s.gate_verdict] || s.gate_verdict, '门禁总判定', '逐条理由见结果页'));

    var counts = s.gate_counts || {};
    var reasons = s.pending_reasons || {};
    gateBox.textContent =
      '门禁：可放行 ' + (counts.compatible || 0) + ' 条 · 放行+回填 ' +
      (counts.compatible_with_backfill || 0) + ' 条 · 阻断 ' + (counts.blocked || 0) +
      ' 条。已合并表：' + data.tables.join(' / ') + '（目标 schema v' + data.schema_version + '）。' +
      ' 本次 ' + (s.pending || 0) + ' 条迟到事件进入待补队列（水位线以内 ' + (reasons.within_watermark || 0) +
      ' 条 · 边界外 ' + (reasons.late_arrival || 0) + ' 条），补齐前不改写已发布结果。';
    resultLink.href = data.result_url;
    snapshotLink.href = data.snapshot_url;
    summaryCard.hidden = false;
    summaryCard.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  form.addEventListener('submit', async function (event) {
    event.preventDefault();
    errBox.hidden = true;
    var tables = qsa('input[name="tables"]:checked', form).map(function (input) { return input.value; });
    var versionInput = qs('input[name="schema_version"]:checked', form);
    if (!tables.length) {
      window.showError(errBox, new Error('请至少选择一张表'));
      return;
    }
    if (!versionInput) {
      window.showError(errBox, new Error('请选择目标 schema 版本'));
      return;
    }
    btn.disabled = true;
    var label = btn.textContent;
    btn.textContent = '执行中…';
    summaryCard.hidden = true;
    startProgress();
    var startedAt = Date.now();
    try {
      var data = await window.postJSON('/api/merge', {
        tables: tables,
        schema_version: Number(versionInput.value)
      });
      var cost = ((Date.now() - startedAt) / 1000).toFixed(2);
      finishProgress(true, '完成：run_id ' + data.run_id + '，用时 ' + cost + ' 秒。');
      renderSummary(data);
    } catch (err) {
      finishProgress(false, '执行失败，请看左面板错误提示后重试。');
      window.showError(errBox, err);
    } finally {
      btn.disabled = false;
      btn.textContent = label;
    }
  });
})();

// --- 待补队列：补齐并切换（结果与水位线同批） ----------------------------
(function () {
  var form = document.getElementById('backfill-form');
  if (!form) return;

  var btn = document.getElementById('backfill-btn');
  var allBtn = document.getElementById('backfill-all-btn');
  var errBox = document.getElementById('backfill-error');
  var resultPanel = document.getElementById('backfill-result');
  var title = document.getElementById('batch-title');
  var summary = document.getElementById('batch-summary');
  var changes = document.getElementById('batch-changes');
  var tableSelect = document.getElementById('backfill-table');
  var cutoffInput = document.getElementById('backfill-cutoff');
  var schemaSelect = document.getElementById('backfill-schema');
  var failInput = document.getElementById('backfill-fail');

  function esc(value) {
    return String(value === null || value === undefined ? '—' : value)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function stateCell(state) {
    if (!state) return '<span class="muted">不存在</span>';
    if (state.deleted) {
      return '<span class="tag err">墓碑 v' + esc(state.tombstone_version) + '</span>' +
        '<div class="small muted mono">第 ' + esc(state.tombstone_line_no) + ' 行</div>';
    }
    return '<span class="tag ok">存活 v' + esc(state.version) + '</span>' +
      '<div class="small mono">' + esc(state.ts) + '</div>' +
      '<div class="small muted mono">' + esc(JSON.stringify(state.after || {})) + '</div>';
  }

  function render(data) {
    var result = data.result || {};
    title.textContent = '批次 ' + data.batch_key + (data.reused ? ' · 复用原版本（未新增）' : ' · 已切换');
    summary.innerHTML =
      '<p class="small" style="margin:0 0 6px">' +
      '<strong>表：</strong>' + esc(data.table) +
      ' · <strong>版本：</strong>v' + esc(data.version_before) + ' → v' + esc(data.version_no) +
      ' · <strong>状态：</strong>' + esc(data.status) +
      ' · <strong>尝试：</strong>' + esc(data.attempts) + '（复用 ' + esc(data.replayed) + '）' +
      '</p>' +
      '<p class="small" style="margin:0 0 6px">' +
      '<strong>水位线：</strong><span class="mono">' + esc(data.watermark_before) + '</span> → ' +
      '<span class="mono">' + esc(data.watermark_after) + '</span>' +
      ' · <strong>待补：</strong>' + esc(data.pending_before) + ' → ' + esc(data.pending_after) +
      ' · <strong>受影响键：</strong>' + esc((data.affected_keys || []).join('、')) +
      ' · <strong>待补原因：</strong>' + esc((data.reasons || []).join('、')) +
      '</p>' +
      '<p class="small muted" style="margin:0">' +
      '最终行 ' + esc(result.rows_before) + ' → ' + esc(result.rows_after) +
      ' · 墓碑 ' + esc(result.tombstones_before) + ' → ' + esc(result.tombstones_after) +
      ' · 变更键 ' + esc(result.changed_keys) + ' / 未变 ' + esc(result.unchanged_keys) + '</p>';

    var rows = (result.changes || []).map(function (item) {
      var fields = (item.changed_fields || []).join('、') || '—';
      return '<tr class="' + (item.changed ? 'row-conflict' : '') + '">' +
        '<td class="mono">' + esc(item.pk) + '</td>' +
        '<td class="small">' + esc(item.change_type) + '</td>' +
        '<td class="small">' + esc(fields) + '</td>' +
        '<td class="small">' + stateCell(item.before) + '</td>' +
        '<td class="small">' + stateCell(item.after) + '</td>' +
        '</tr>';
    }).join('');

    var tombstones = (result.tombstones_preserved || []).map(function (item) {
      return '<code>' + esc(item.pk) + '</code> 墓碑 v' + esc(item.tombstone_version) +
        '（第 ' + esc(item.tombstone_line_no) + ' 行）';
    }).join('、') || '无';
    var conflicts = (result.conflicts_preserved || []).map(function (item) {
      return '<code>' + esc(item.pk) + '</code> v' + esc(item.version) + '（字段 ' + esc((item.diff || []).join('、')) + '）';
    }).join('、') || '无';

    changes.innerHTML =
      '<table class="dense"><thead><tr><th>键</th><th>变化</th><th>变化字段</th><th>补前最终结果</th><th>补后最终结果</th></tr></thead>' +
      '<tbody>' + (rows || '<tr><td colspan="5" class="muted">本批没有受影响键。</td></tr>') + '</tbody></table>' +
      '<div class="panel-body tight"><p class="small muted" style="margin:0">' +
      '<strong>保留的墓碑：</strong>' + tombstones + '<br><strong>保留的冲突：</strong>' + conflicts +
      '</p></div>';
    resultPanel.hidden = false;
    resultPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  async function submit(cutoff) {
    errBox.hidden = true;
    btn.disabled = true;
    allBtn.disabled = true;
    var label = btn.textContent;
    btn.textContent = '补齐中…';
    try {
      var data = await window.postJSON('/api/backfill', {
        table: tableSelect.value,
        cutoff: cutoff,
        schema_version: Number(schemaSelect.value),
        simulate_failure: !!(failInput && failInput.checked)
      });
      render(data);
      if (!data.reused) {
        setTimeout(function () { window.location.reload(); }, 1200);
      }
    } catch (err) {
      window.showError(errBox, err);
    } finally {
      btn.disabled = false;
      allBtn.disabled = false;
      btn.textContent = label;
    }
  }

  form.addEventListener('submit', function (event) {
    event.preventDefault();
    submit(cutoffInput.value.trim() || null);
  });

  allBtn.addEventListener('click', function () {
    cutoffInput.value = '';
    submit(null);
  });
})();
