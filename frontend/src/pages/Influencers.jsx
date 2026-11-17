import React, { useState } from "react";
import { useApi } from "../api.js";
import {
  Async, BrandSelect, Card, Segmented, SentimentPill, Swatch, fmt, useBrandColor, useBrands, useCaps,
} from "../components/ui.jsx";

export default function Influencers() {
  const { brands, myBrand } = useBrands();
  const competitors = brands.filter((b) => b !== myBrand);
  const [brand, setBrand] = useState(null);
  const [role, setRole] = useState("promoter");
  const list = useApi("/influencers", { brand, role, limit: 30 }, [brand, role]);
  const shared = useApi("/influencers/shared");
  const color = useBrandColor();
  const activity = useCaps().influencer_mode === "activity";
  const how = activity
    ? "Dataset has no follower counts, so these are the most active authors per brand (3+ posts). Score = ln(1+posts) × ln(1+engagement+posts) × positivity."
    : "Accounts with 10k+ followers. Score = log₁₀(followers) × ln(1+engagement) × positivity.";

  return (
    <>
      <Card
        title={(role === "promoter" ? "Influencers promoting each brand" : "Influencers criticising each brand").replace("Influencers", activity ? "Vocal customers" : "Influencers")}
        sub={`${how} Competitors of ${myBrand}: ${competitors.join(", ")}`}
        actions={<>
          <BrandSelect value={brand} onChange={setBrand} />
          <Segmented value={role} onChange={setRole} options={[["promoter", "Promoters"], ["detractor", "Detractors"]]} />
        </>}
      >
        <Async state={list} empty="No influencers found for this filter.">
          {({ items, source }) => (
            <div className="table-wrap">
              <table>
                <thead><tr><th>Influencer</th><th>Brand</th><th className="num">Followers</th><th className="num">Posts</th><th className="num">Engagement</th><th className="num">Sentiment</th><th>Platforms</th><th>Also promotes</th><th className="num">Score</th></tr></thead>
                <tbody>
                  {items.map((i) => (
                    <tr key={i.author + i.brand}>
                      <td><b>@{i.author}</b></td>
                      <td><span style={{ display: "inline-flex", gap: 6, alignItems: "center" }}><Swatch color={color(i.brand)} />{i.brand}{i.brand !== myBrand && <span className="pill">competitor</span>}</span></td>
                      <td className="num">{fmt.compact(i.followers)}</td>
                      <td className="num">{fmt.int(i.posts)}</td>
                      <td className="num">{fmt.compact(i.engagement)}</td>
                      <td className="num"><SentimentPill value={i.avg_sentiment} /></td>
                      <td>{(i.platforms || []).join(", ")}</td>
                      <td>{(i.also_promotes || []).join(", ") || "–"}</td>
                      <td className="num">{fmt.num(i.score, 1)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <div className="card-sub" style={{ marginTop: 8 }}>Source: {source === "neo4j" ? "Neo4j graph" : "MongoDB (Neo4j unavailable)"}</div>
            </div>
          )}
        </Async>
      </Card>
      <Card title="Shared influencers" sub="Promote more than one competing brand — warm leads for partnerships">
        <Async state={shared} empty="No influencer promotes more than one brand.">
          {(rows) => (
            <div className="table-wrap">
              <table>
                <thead><tr><th>Influencer</th><th className="num">Followers</th><th>Promotes</th><th className="num">Combined score</th></tr></thead>
                <tbody>
                  {rows.map((r) => (
                    <tr key={r.author}>
                      <td><b>@{r.author}</b></td>
                      <td className="num">{fmt.compact(r.followers)}</td>
                      <td><span style={{ display: "inline-flex", gap: 10, flexWrap: "wrap" }}>{r.brands.slice().sort().map((b) => <span key={b} style={{ display: "inline-flex", gap: 4, alignItems: "center" }}><Swatch color={color(b)} />{b}</span>)}</span></td>
                      <td className="num">{fmt.num(r.score, 1)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Async>
      </Card>
    </>
  );
}
