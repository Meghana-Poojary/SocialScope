import React, { createContext, useContext } from "react";

/* ---------------------------------------------------------------- brand colours
 * Colour follows the entity: each brand gets a fixed categorical slot from its
 * position in the (sorted) brand list, so filters never repaint survivors. */
export const BrandCtx = createContext({ brands: [], myBrand: null, meta: null });
export const useBrands = () => useContext(BrandCtx);

export function colorFor(brands, brand) {
  const i = brands.indexOf(brand);
  return i < 0 ? "var(--muted)" : `var(--series-${(i % 8) + 1})`;
}

export function useBrandColor() {
  const { brands } = useBrands();
  return (b) => colorFor(brands, b);
}

/* ---------------------------------------------------------------- formatting */
export const fmt = {
  int: (v) => (v == null ? "–" : Math.round(v).toLocaleString()),
  compact: (v) =>
    v == null ? "–" : new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(v),
  pct: (v, d = 1) => (v == null ? "–" : `${(v * 100).toFixed(d)}%`),
  pctRaw: (v, d = 1) => (v == null ? "–" : `${Number(v).toFixed(d)}%`),
  signedPct: (v, d = 1) => (v == null ? "–" : `${v > 0 ? "+" : ""}${Number(v).toFixed(d)}%`),
  num: (v, d = 2) => (v == null ? "–" : Number(v).toFixed(d)),
  label: (s) => (s || "").replace(/_/g, " "),
};

export const PLATFORM_LABEL = { all: "All platforms", instagram: "Instagram", reddit: "Reddit", youtube: "YouTube", x: "X" };
const PLATFORM_ORDER = ["instagram", "reddit", "youtube", "x"];

/** Platform filter options for the platforms this dataset actually has.
 *  all: include an "All" entry (only when there is more than one platform). */
export function usePlatformOptions({ all = true, short = false } = {}) {
  const { meta } = useBrands();
  const present = meta?.platforms?.length ? meta.platforms : PLATFORM_ORDER;
  const opts = PLATFORM_ORDER.filter((p) => present.includes(p)).map((p) => [p, PLATFORM_LABEL[p]]);
  if (!all) return opts;
  return opts.length > 1 ? [["all", short ? "All" : PLATFORM_LABEL.all], ...opts] : [["all", opts[0]?.[1] || "All"]];
}

/** What the loaded dataset contains (follower counts, likes, products ...). Older runs
 *  without this metadata are treated as having everything. */
export function useCaps() {
  const caps = useBrands().meta?.capabilities;
  return caps || { follower_counts: true, follower_snapshots: true, likes: true, comments: true,
                   shares: true, views: true, products: true, influencer_mode: "followers" };
}

/* ---------------------------------------------------------------- layout */
export function Card({ title, sub, actions, children, style }) {
  return (
    <section className="card" style={style}>
      {(title || actions) && (
        <div className="card-head">
          <div>
            {title && <h2>{title}</h2>}
            {sub && <div className="card-sub">{sub}</div>}
          </div>
          {actions && <div className="filters">{actions}</div>}
        </div>
      )}
      {children}
    </section>
  );
}

export function Async({ state, children, empty = "No data yet." }) {
  if (state.loading && !state.data) return <div className="empty">Loading…</div>;
  if (state.error) return <div className="error">{String(state.error.message || state.error)}</div>;
  const d = state.data;
  if (!d || (Array.isArray(d) && d.length === 0) || (d.items && d.items.length === 0)) return <div className="empty">{empty}</div>;
  return children(d);
}

export function Segmented({ value, options, onChange }) {
  if (options.length <= 1) return null;   // nothing to choose (e.g. a single-platform dataset)
  return (
    <div className="seg" role="tablist">
      {options.map((o) => {
        const [v, l] = Array.isArray(o) ? o : [o, o];
        return (
          <button key={v} className={value === v ? "on" : ""} onClick={() => onChange(v)} role="tab" aria-selected={value === v}>
            {l}
          </button>
        );
      })}
    </div>
  );
}

export function BrandSelect({ value, onChange, allowAll = true, label = "All brands" }) {
  const { brands } = useBrands();
  return (
    <select value={value || ""} onChange={(e) => onChange(e.target.value || null)} aria-label="Brand">
      {allowAll && <option value="">{label}</option>}
      {brands.map((b) => <option key={b} value={b}>{b}</option>)}
    </select>
  );
}

export function Swatch({ color }) {
  return <span className="swatch" style={{ background: color }} />;
}

export function BrandLegend({ only }) {
  const { brands } = useBrands();
  return (
    <div className="legend">
      {brands.filter((b) => !only || only.includes(b)).map((b) => (
        <span key={b}><Swatch color={colorFor(brands, b)} />{b}</span>
      ))}
    </div>
  );
}

/* Recharts custom tooltip: text in ink colours, swatch carries identity. */
export function ChartTooltip({ active, payload, label, format = fmt.int, labelFormat }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="tooltip">
      <div className="t-title">{labelFormat ? labelFormat(label) : label}</div>
      {payload.filter((p) => p.value != null).sort((a, b) => b.value - a.value).map((p) => (
        <div className="t-row" key={p.dataKey}>
          <span style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
            <Swatch color={p.color || p.stroke || p.fill} />{p.name}
          </span>
          <b>{format(p.value)}</b>
        </div>
      ))}
    </div>
  );
}

export const axisProps = {
  stroke: "var(--axis)",
  tick: { fill: "var(--muted)", fontSize: 11 },
  tickLine: false,
};

/** Simple horizontal bar list (HTML, so labels never collide). */
export function BarList({ items, color = "var(--series-1)", format = fmt.int, max }) {
  const m = max ?? Math.max(...items.map((i) => i.value), 1);
  return (
    <div>
      {items.map((i) => (
        <div className="bar-row" key={i.key ?? i.label} title={i.title || `${i.label}: ${format(i.value)}`}>
          <div className="name">{i.label}</div>
          <div className="track">
            <div className="fill" style={{ width: `${Math.max(1, (i.value / m) * 100)}%`, background: i.color || color }} />
          </div>
          <div className="val">{format(i.value)}</div>
        </div>
      ))}
    </div>
  );
}

export function SentimentPill({ value }) {
  if (value == null) return <span className="pill">–</span>;
  const cls = value >= 0.05 ? "pos" : value <= -0.05 ? "neg" : "";
  return <span className={`pill ${cls}`}>{value >= 0 ? "+" : ""}{value.toFixed(2)}</span>;
}

/** Pivot long rows [{date, brand, metric}] into wide rows [{date, BrandA, BrandB}]. */
export function pivot(rows, key, series, value) {
  const by = new Map();
  for (const r of rows) {
    if (!by.has(r[key])) by.set(r[key], { [key]: r[key] });
    by.get(r[key])[r[series]] = typeof value === "function" ? value(r) : r[value];
  }
  return [...by.values()].sort((a, b) => (a[key] < b[key] ? -1 : 1));
}

/** Trailing moving average across every series column of pivoted rows. */
export function smooth(rows, keys, window = 7) {
  return rows.map((r, i) => {
    const out = { ...r };
    for (const k of keys) {
      const slice = rows.slice(Math.max(0, i - window + 1), i + 1).map((x) => x[k]).filter((v) => v != null);
      out[k] = slice.length ? slice.reduce((a, b) => a + b, 0) / slice.length : null;
    }
    return out;
  });
}

export const shortDate = (d) => (d ? new Date(d + "T00:00:00Z").toLocaleDateString(undefined, { month: "short", day: "numeric", timeZone: "UTC" }) : "");
