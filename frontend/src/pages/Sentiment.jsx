import React, { useState } from "react";
import {
  Bar, BarChart, CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import { useApi } from "../api.js";
import {
  Async, BrandLegend, Card, ChartTooltip, PLATFORM_LABEL, Segmented, Swatch, axisProps, fmt, pivot,
  shortDate, smooth, useBrandColor, useBrands, usePlatformOptions,
} from "../components/ui.jsx";

const PARTS = [["positive_pct", "Positive", "var(--pos)"], ["neutral_pct", "Neutral", "var(--neu)"], ["negative_pct", "Negative", "var(--neg)"]];

export default function Sentiment() {
  const [platform, setPlatform] = useState("all");
  const eng = useApi("/engagement", { platform });
  const platformOptions = usePlatformOptions();
  return (
    <>
      <Card
        title="Audience sentiment mix"
        sub="Share of audience posts by VADER sentiment label"
        actions={<Segmented value={platform} onChange={setPlatform} options={platformOptions} />}
      >
        <div className="legend" style={{ marginBottom: 8 }}>
          {PARTS.map(([, l, c]) => <span key={l}><Swatch color={c} />{l}</span>)}
        </div>
        <Async state={eng}>
          {(rows) => (
            <ResponsiveContainer width="100%" height={Math.max(160, rows.length * 44)}>
              <BarChart data={rows} layout="vertical" margin={{ top: 4, right: 16, bottom: 0, left: 0 }} stackOffset="expand" barCategoryGap="28%">
                <XAxis type="number" tickFormatter={(v) => `${Math.round(v * 100)}%`} {...axisProps} />
                <YAxis type="category" dataKey="brand" width={90} {...axisProps} axisLine={false} />
                <Tooltip content={<ChartTooltip format={(v) => fmt.pct(v)} />} cursor={{ fill: "var(--surface-2)" }} />
                {PARTS.map(([k, l, c], i) => (
                  <Bar isAnimationActive={false} key={k} dataKey={k} name={l} stackId="s" fill={c} stroke="var(--surface)" strokeWidth={2}
                    radius={i === 2 ? [0, 4, 4, 0] : i === 0 ? [4, 0, 0, 4] : 0} />
                ))}
              </BarChart>
            </ResponsiveContainer>
          )}
        </Async>
      </Card>
      <SentimentOverTime platform={platform} />
    </>
  );
}

function SentimentOverTime({ platform }) {
  const ts = useApi("/timeseries", { platform });
  const { brands } = useBrands();
  const color = useBrandColor();
  const platformOptions = usePlatformOptions();
  return (
    <Card title="Sentiment over time" sub={`Average VADER compound score (−1 to +1), 7-day moving average · ${(platformOptions.find(([k]) => k === platform) || [])[1] || PLATFORM_LABEL[platform]}`}>
      <BrandLegend />
      <Async state={ts}>
        {(rows) => {
          const data = smooth(pivot(rows, "date", "brand", "avg_sentiment"), brands);
          return (
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={data} margin={{ top: 12, right: 12, bottom: 0, left: 0 }}>
                <CartesianGrid stroke="var(--grid)" vertical={false} />
                <XAxis dataKey="date" tickFormatter={shortDate} minTickGap={40} {...axisProps} />
                <YAxis width={44} domain={["auto", "auto"]} tickFormatter={(v) => v.toFixed(2)} {...axisProps} axisLine={false} />
                <ReferenceLine y={0} stroke="var(--axis)" />
                <Tooltip content={<ChartTooltip format={(v) => fmt.num(v, 3)} labelFormat={shortDate} />} cursor={{ stroke: "var(--axis)" }} />
                {brands.map((b) => <Line isAnimationActive={false} key={b} dataKey={b} name={b} stroke={color(b)} strokeWidth={2} dot={false} />)}
              </LineChart>
            </ResponsiveContainer>
          );
        }}
      </Async>
    </Card>
  );
}
