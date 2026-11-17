import React, { useState } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useApi } from "../api.js";
import {
  Async, BrandLegend, Card, ChartTooltip, Segmented, Swatch, axisProps, fmt, pivot,
  shortDate, smooth, useBrandColor, useBrands, usePlatformOptions,
} from "../components/ui.jsx";

const COMPONENTS = [
  ["engagement", "Engagement"], ["sentiment", "Sentiment"], ["share_of_voice", "Share of voice"],
  ["growth", "Growth"], ["complaint_health", "Complaint health"],
];

export default function Overview() {
  const overview = useApi("/overview");
  const color = useBrandColor();
  return (
    <>
      <Async state={overview}>
        {({ leaderboard }) => (
          <>
            <div className="kpis">
              {leaderboard.map((b) => (
                <div className="kpi" key={b.brand}>
                  <div className="label"><Swatch color={color(b.brand)} />{b.brand}<span className="rank">#{b.rank}</span></div>
                  <div className="value">{b.score}</div>
                  <div className="sub">
                    Performance score · {fmt.int(b.mentions)} mentions · {fmt.pct(b.share_of_voice)} share of voice
                  </div>
                </div>
              ))}
            </div>
            <Card title="Who is winning, and why" sub="Each component normalised 0–100 across competitors. Score = 25% engagement + 25% sentiment + 20% share of voice + 15% growth + 15% complaint health.">
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Brand</th><th className="num">Score</th>
                      {COMPONENTS.map(([k, l]) => <th className="num" key={k}>{l}</th>)}
                      <th className="num">Avg sentiment</th><th className="num" title={leaderboard[0]?.growth_basis === "audience" ? "Unique-author growth, last 14 days vs first 14 days" : "Follower growth over the period"}>{leaderboard[0]?.growth_basis === "audience" ? "Audience growth" : "Followers Δ"}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {leaderboard.map((b) => (
                      <tr key={b.brand}>
                        <td><span style={{ display: "inline-flex", gap: 8, alignItems: "center" }}><Swatch color={color(b.brand)} />{b.brand}</span></td>
                        <td className="num"><b>{b.score}</b></td>
                        {COMPONENTS.map(([k]) => <td className="num" key={k}>{fmt.num(b.components[k], 0)}</td>)}
                        <td className="num">{fmt.num(b.avg_sentiment)}</td>
                        <td className="num">{fmt.signedPct(b.growth_pct ?? b.follower_growth_pct)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          </>
        )}
      </Async>
      <MentionsOverTime />
    </>
  );
}

function MentionsOverTime() {
  const [platform, setPlatform] = useState("all");
  const ts = useApi("/timeseries", { platform });
  const { brands } = useBrands();
  const platformOptions = usePlatformOptions();
  const color = useBrandColor();
  return (
    <Card
      title="Mentions over time"
      sub="Audience posts per day, 7-day moving average"
      actions={<Segmented value={platform} onChange={setPlatform} options={platformOptions} />}
    >
      <BrandLegend />
      <Async state={ts}>
        {(rows) => {
          const data = smooth(pivot(rows, "date", "brand", "mentions"), brands);
          return (
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={data} margin={{ top: 12, right: 12, bottom: 0, left: 0 }}>
                <CartesianGrid stroke="var(--grid)" vertical={false} />
                <XAxis dataKey="date" tickFormatter={shortDate} minTickGap={40} {...axisProps} />
                <YAxis width={44} {...axisProps} axisLine={false} />
                <Tooltip content={<ChartTooltip format={(v) => fmt.num(v, 1)} labelFormat={shortDate} />} cursor={{ stroke: "var(--axis)" }} />
                {brands.map((b) => (
                  <Line isAnimationActive={false} key={b} dataKey={b} name={b} stroke={color(b)} strokeWidth={2} dot={false} activeDot={{ r: 4, strokeWidth: 2, stroke: "var(--surface)" }} />
                ))}
              </LineChart>
            </ResponsiveContainer>
          );
        }}
      </Async>
    </Card>
  );
}
