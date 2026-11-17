import React, { useState } from "react";
import {
  Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import { useApi } from "../api.js";
import {
  Async, BrandLegend, Card, ChartTooltip, PLATFORM_LABEL, Segmented, Swatch, axisProps, fmt, pivot,
  shortDate, useBrandColor, useBrands, useCaps, usePlatformOptions,
} from "../components/ui.jsx";

// [key, label, format, capability the dataset needs for it]
const ALL_METRICS = [
  ["brand_engagement_rate", "Engagement rate (brand posts)", (v) => fmt.pctRaw(v, 2), "follower_counts"],
  ["avg_engagement", "Avg engagement / post", (v) => fmt.num(v, 1), null],
  ["likes", "Likes", fmt.compact, "likes"],
  ["comments", "Comments", fmt.compact, "comments"],
  ["shares", "Shares", fmt.compact, "shares"],
  ["mentions", "Audience mentions", fmt.compact, null],
];

export default function Engagement() {
  const caps = useCaps();
  const METRICS = ALL_METRICS.filter(([, , , need]) => !need || caps[need]);
  const [picked, setMetric] = useState(null);
  const metric = METRICS.some(([k]) => k === picked) ? picked : METRICS[0][0];
  const eng = useApi("/engagement");
  const { brands } = useBrands();
  const color = useBrandColor();
  const [, label, format] = METRICS.find(([k]) => k === metric);

  return (
    <>
      <Card
        title={`${label} by platform`}
        sub={caps.follower_counts ? "Engagement = likes + comments + shares. Engagement rate = engagement ÷ account followers." : "Engagement = the interaction counts this dataset has (replies received for Kaggle Twitter data)."}
        actions={
          <select value={metric} onChange={(e) => setMetric(e.target.value)} aria-label="Metric">
            {METRICS.map(([k, l]) => <option key={k} value={k}>{l}</option>)}
          </select>
        }
      >
        <BrandLegend />
        <Async state={eng}>
          {(rows) => {
            const data = pivot(rows, "platform", "brand", metric).map((r) => ({ ...r, label: PLATFORM_LABEL[r.platform] }));
            return (
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={data} margin={{ top: 12, right: 12, bottom: 0, left: 0 }} barGap={2} barCategoryGap="22%">
                  <CartesianGrid stroke="var(--grid)" vertical={false} />
                  <XAxis dataKey="label" {...axisProps} />
                  <YAxis width={52} tickFormatter={format} {...axisProps} axisLine={false} />
                  <Tooltip content={<ChartTooltip format={format} />} cursor={{ fill: "var(--surface-2)" }} />
                  {brands.map((b) => <Bar isAnimationActive={false} key={b} dataKey={b} name={b} fill={color(b)} radius={[4, 4, 0, 0]} maxBarSize={28} />)}
                </BarChart>
              </ResponsiveContainer>
            );
          }}
        </Async>
      </Card>
      {caps.follower_snapshots ? (
        <div className="grid-2">
          <FollowerGrowth />
          <EngagementTable state={eng} />
        </div>
      ) : (
        <EngagementTable state={eng} />
      )}
    </>
  );
}

function FollowerGrowth() {
  const [platform, setPlatform] = useState("all");
  const fol = useApi("/followers", { platform });
  const platformOptions = usePlatformOptions({ short: true });
  const { brands } = useBrands();
  const color = useBrandColor();
  return (
    <Card
      title="Follower growth"
      sub="Change in followers since the start of the window (indexed, %)"
      actions={<Segmented value={platform} onChange={setPlatform} options={platformOptions} />}
    >
      <BrandLegend />
      <Async state={fol} empty="This dataset has no follower snapshots. Growth is shown as audience (unique-author) growth on the Overview tab.">
        {(rows) => {
          const base = {};
          for (const r of rows) if (base[r.brand] == null) base[r.brand] = r.followers;
          const data = pivot(rows, "date", "brand", (r) => ((r.followers - base[r.brand]) / base[r.brand]) * 100);
          return (
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={data} margin={{ top: 12, right: 12, bottom: 0, left: 0 }}>
                <CartesianGrid stroke="var(--grid)" vertical={false} />
                <XAxis dataKey="date" tickFormatter={shortDate} minTickGap={40} {...axisProps} />
                <YAxis width={48} tickFormatter={(v) => `${v.toFixed(0)}%`} {...axisProps} axisLine={false} />
                <Tooltip content={<ChartTooltip format={(v) => fmt.signedPct(v)} labelFormat={shortDate} />} cursor={{ stroke: "var(--axis)" }} />
                {brands.map((b) => <Line isAnimationActive={false} key={b} dataKey={b} name={b} stroke={color(b)} strokeWidth={2} dot={false} />)}
              </LineChart>
            </ResponsiveContainer>
          );
        }}
      </Async>
    </Card>
  );
}

function EngagementTable({ state }) {
  const color = useBrandColor();
  const caps = useCaps();
  const options = usePlatformOptions({ all: false });
  const [picked, setPlatform] = useState(null);
  const platform = picked || options[0]?.[0];
  // only the columns this dataset has
  const cols = [
    ["mentions", "Mentions", fmt.int, true],
    ["likes", "Likes", fmt.compact, caps.likes],
    ["comments", caps.likes ? "Comments" : "Replies", fmt.compact, caps.comments],
    ["shares", "Shares", fmt.compact, caps.shares],
    ["brand_engagement_rate", "ER", (v) => fmt.pctRaw(v, 2), caps.follower_counts],
    ["complaints", "Complaints", fmt.int, !caps.follower_counts],
  ].filter((c) => c[3]);
  const title = options.length > 1 ? "Platform breakdown" : `Breakdown · ${options[0]?.[1] || ""}`;
  return (
    <Card title={title} actions={<Segmented value={platform} onChange={setPlatform} options={options} />}>
      <Async state={state}>
        {(rows) => (
          <div className="table-wrap">
            <table>
              <thead>
                <tr><th>Brand</th>{cols.map(([k, l]) => <th key={k} className="num">{l}</th>)}</tr>
              </thead>
              <tbody>
                {rows.filter((r) => r.platform === platform).map((r) => (
                  <tr key={r.brand}>
                    <td><span style={{ display: "inline-flex", gap: 8, alignItems: "center" }}><Swatch color={color(r.brand)} />{r.brand}</span></td>
                    {cols.map(([k, , f]) => <td key={k} className="num">{f(r[k])}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Async>
    </Card>
  );
}
