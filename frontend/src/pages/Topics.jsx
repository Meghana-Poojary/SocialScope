import React from "react";
import { useApi } from "../api.js";
import { Async, BarList, Card, SentimentPill, Swatch, fmt, useBrandColor, useBrands } from "../components/ui.jsx";

export default function Topics() {
  const { brands } = useBrands();
  const topics = useApi("/topics", { limit: 12 });
  const trending = useApi("/topics/trending");
  const color = useBrandColor();
  return (
    <>
      <Card title="Trending topics across the market" sub="Fastest-growing phrases and hashtags: last 7 days vs the 7 days before">
        <Async state={trending}>
          {(rows) => (
            <div className="table-wrap">
              <table>
                <thead><tr><th>Topic</th><th className="num">Last 7d</th><th className="num">Prev 7d</th><th className="num">Growth</th><th className="num">Sentiment</th><th>Brands</th></tr></thead>
                <tbody>
                  {rows.slice(0, 12).map((t) => (
                    <tr key={t.topic}>
                      <td>{t.topic}</td>
                      <td className="num">{fmt.int(t.last_7d)}</td>
                      <td className="num">{fmt.int(t.prev_7d)}</td>
                      <td className={`num ${t.growth_pct >= 0 ? "up" : "down"}`}>{fmt.signedPct(t.growth_pct, 0)}</td>
                      <td className="num"><SentimentPill value={t.avg_sentiment} /></td>
                      <td>
                        <span style={{ display: "inline-flex", gap: 8, flexWrap: "wrap" }}>
                          {t.brands.slice().sort().map((b) => <span key={b} style={{ display: "inline-flex", gap: 4, alignItems: "center" }}><Swatch color={color(b)} />{b}</span>)}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Async>
      </Card>
      <Async state={topics}>
        {(rows) => (
          <div className="grid-2">
            {brands.map((b) => {
              const mine = rows.filter((r) => r.brand === b);
              return (
                <Card key={b} title={<span style={{ display: "inline-flex", gap: 8, alignItems: "center" }}><Swatch color={color(b)} />{b}</span>} sub="Top topics by volume · hover for sentiment and lift">
                  <BarList
                    color={color(b)}
                    items={mine.map((t) => ({
                      label: t.topic, value: t.count,
                      title: `${t.topic}: ${t.count} mentions · sentiment ${fmt.num(t.avg_sentiment)} · lift ${fmt.num(t.lift)}× vs market`,
                    }))}
                  />
                </Card>
              );
            })}
          </div>
        )}
      </Async>
    </>
  );
}
