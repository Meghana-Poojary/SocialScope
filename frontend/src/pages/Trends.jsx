import React, { useState } from "react";
import { CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useApi } from "../api.js";
import { Async, Card, ChartTooltip, Segmented, Swatch, axisProps, fmt, shortDate, useBrandColor } from "../components/ui.jsx";

function addDays(iso, n) {
  const d = new Date(iso + "T00:00:00Z");
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

export default function Trends() {
  const [level, setLevel] = useState("brand");
  const trends = useApi("/trends", { level }, [level]);
  const [sel, setSel] = useState(0);
  const color = useBrandColor();

  return (
    <Async state={trends} empty="Not enough recent data to model trends.">
      {(rows) => {
        const pick = rows[Math.min(sel, rows.length - 1)];
        const name = (t) => t.product || t.brand;
        const last = pick.history.at(-1)?.date;
        const data = [
          ...pick.history.map((h) => ({ date: h.date, actual: h.mentions })),
          ...pick.forecast.map((f) => ({ date: addDays(last, f.day), forecast: f.mentions })),
        ];
        if (last) data[pick.history.length - 1].forecast = pick.history.at(-1).mentions;
        return (
          <>
            <Card
              title="Who will trend next?"
              sub="Probability of trending = logistic(momentum, mention growth slope, engagement slope, sentiment trend) over the last 14 days"
              actions={<Segmented value={level} onChange={(v) => { setLevel(v); setSel(0); }} options={[["brand", "Brands"], ["product", "Products"]]} />}
            >
              <div className="table-wrap">
                <table>
                  <thead><tr><th>#</th><th>{level === "brand" ? "Brand" : "Product"}</th><th>Trend probability</th><th>Direction</th><th className="num">Momentum</th><th className="num">Daily growth</th><th className="num">Mentions/day</th><th className="num">Sentiment trend</th></tr></thead>
                  <tbody>
                    {rows.map((t, i) => (
                      <tr key={name(t)} onClick={() => setSel(i)} style={{ cursor: "pointer", background: i === sel ? "var(--surface-2)" : undefined }}>
                        <td>{t.rank}</td>
                        <td><span style={{ display: "inline-flex", gap: 8, alignItems: "center" }}><Swatch color={color(t.brand)} />{name(t)}</span></td>
                        <td style={{ minWidth: 160 }}>
                          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                            <div style={{ flex: 1, height: 8, background: "var(--surface-2)", borderRadius: 4 }}>
                              <div style={{ width: `${t.trend_probability * 100}%`, height: 8, borderRadius: 4, background: color(t.brand) }} />
                            </div>
                            <span style={{ fontVariantNumeric: "tabular-nums", minWidth: 36 }}>{fmt.pct(t.trend_probability, 0)}</span>
                          </div>
                        </td>
                        <td><span className={`pill ${t.direction === "rising" ? "pos" : t.direction === "falling" ? "neg" : ""}`}>{t.direction === "rising" ? "▲" : t.direction === "falling" ? "▼" : "■"} {t.direction}</span></td>
                        <td className={`num ${t.momentum_pct >= 0 ? "up" : "down"}`}>{fmt.signedPct(t.momentum_pct)}</td>
                        <td className="num">{fmt.signedPct(t.daily_growth_pct, 2)}</td>
                        <td className="num">{fmt.num(t.avg_daily_mentions, 1)}</td>
                        <td className="num">{fmt.num(t.sentiment_trend, 3)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
            <Card title={`${name(pick)}: mentions & 7-day forecast`} sub="Solid = observed daily mentions · dashed = linear-trend forecast">
              <div className="legend" style={{ marginBottom: 6 }}>
                <span><Swatch color={color(pick.brand)} />Observed</span>
                <span><span style={{ width: 16, borderTop: `2px dashed ${color(pick.brand)}` }} />Forecast</span>
              </div>
              <ResponsiveContainer width="100%" height={280}>
                <LineChart data={data} margin={{ top: 12, right: 12, bottom: 0, left: 0 }}>
                  <CartesianGrid stroke="var(--grid)" vertical={false} />
                  <XAxis dataKey="date" tickFormatter={shortDate} minTickGap={30} {...axisProps} />
                  <YAxis width={44} {...axisProps} axisLine={false} />
                  {last && <ReferenceLine x={last} stroke="var(--axis)" strokeDasharray="2 3" label={{ value: "today", fill: "var(--muted)", fontSize: 11, position: "insideTopRight" }} />}
                  <Tooltip content={<ChartTooltip format={(v) => fmt.num(v, 1)} labelFormat={shortDate} />} cursor={{ stroke: "var(--axis)" }} />
                  <Line isAnimationActive={false} dataKey="actual" name="Observed" stroke={color(pick.brand)} strokeWidth={2} dot={{ r: 3 }} connectNulls={false} />
                  <Line isAnimationActive={false} dataKey="forecast" name="Forecast" stroke={color(pick.brand)} strokeWidth={2} strokeDasharray="5 4" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </Card>
          </>
        );
      }}
    </Async>
  );
}
