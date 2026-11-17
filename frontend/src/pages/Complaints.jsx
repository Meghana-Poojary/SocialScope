import React, { useState } from "react";
import { useApi } from "../api.js";
import { Async, BarList, BrandSelect, Card, Swatch, fmt, useBrandColor, useBrands } from "../components/ui.jsx";

const SEQ = ["--seq-1", "--seq-2", "--seq-3", "--seq-4", "--seq-5", "--seq-6", "--seq-7"];

export default function Complaints() {
  const { brands, myBrand } = useBrands();
  const [focus, setFocus] = useState(null);
  const brand = focus || brands[0];
  const all = useApi("/complaints");
  const color = useBrandColor();

  return (
    <>
      <Gaps myBrand={myBrand} />
      <Card title="Complaint heatmap" sub="Complaints per category as % of each brand’s audience mentions · darker = worse · click a cell to drill in">
        <Async state={all}>
          {(rows) => {
            const cats = [...new Set(rows.map((r) => r.category))].sort();
            const val = Object.fromEntries(rows.map((r) => [`${r.brand}|${r.category}`, r.pct_of_mentions || 0]));
            const max = Math.max(...Object.values(val), 0.01);
            const step = (v) => SEQ[Math.min(SEQ.length - 1, Math.floor((v / max) * (SEQ.length - 0.001)))];
            return (
              <div className="table-wrap">
                <table className="heat">
                  <thead><tr><th>Brand</th>{cats.map((c) => <th key={c} className="num" style={{ textAlign: "center" }}>{fmt.label(c)}</th>)}</tr></thead>
                  <tbody>
                    {brands.map((b) => (
                      <tr key={b}>
                        <td><span style={{ display: "inline-flex", gap: 8, alignItems: "center" }}><Swatch color={color(b)} />{b}</span></td>
                        {cats.map((c) => {
                          const v = val[`${b}|${c}`] || 0;
                          const s = step(v);
                          const dark = SEQ.indexOf(s) >= 3;
                          return (
                            <td key={c} className="cell" title={`${b} · ${fmt.label(c)}: ${v.toFixed(2)}% of mentions`}
                              onClick={() => setFocus(b)}
                              style={{ background: `var(${s})`, color: dark ? "#fff" : "#0b0b0b", cursor: "pointer" }}>
                              {v.toFixed(1)}%
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            );
          }}
        </Async>
      </Card>
      <BrandDrill brand={brand} setBrand={setFocus} />
    </>
  );
}

function Gaps({ myBrand }) {
  const gaps = useApi("/gaps", { my_brand: myBrand, limit: 6 }, [myBrand]);
  const color = useBrandColor();
  return (
    <Card title={`Opportunity gaps for ${myBrand || "your brand"}`} sub="Where competitors draw clearly more complaints than you and the market average. Change “My brand” in the header.">
      <Async state={gaps} empty="No clear gaps — no competitor is struggling noticeably more than you in any category.">
        {(rows) => rows.map((g) => (
          <div className="gap-card" key={`${g.competitor}-${g.category}`} style={{ borderLeftColor: color(g.competitor) }}>
            <div className="gap-title">
              <Swatch color={color(g.competitor)} />{g.competitor} · {fmt.label(g.category)}
              <span className="pill neg">{g.competitor_rate.toFixed(1)}% vs your {g.my_rate.toFixed(1)}%</span>
              <span className="pill">score {g.opportunity_score}</span>
            </div>
            <div style={{ marginTop: 4 }}>{g.insight}</div>
            {g.evidence && <div className="quote" style={{ marginTop: 4 }}>“{g.evidence}”</div>}
          </div>
        ))}
      </Async>
    </Card>
  );
}

function BrandDrill({ brand, setBrand }) {
  const complaints = useApi("/complaints", { brand }, [brand]);
  const problems = useApi("/problems", { brand }, [brand]);
  const color = useBrandColor();
  return (
    <div className="grid-2">
      <Card title={`Complaint categories · ${brand}`} sub="Share of this brand’s complaints" actions={<BrandSelect value={brand} onChange={setBrand} allowAll={false} />}>
        <Async state={complaints}>
          {(rows) => (
            <>
              <BarList color={color(brand)} format={(v) => `${v.toFixed(1)}%`}
                items={rows.map((r) => ({ label: fmt.label(r.category), value: r.pct_of_complaints || 0, title: `${r.count} complaints · ${r.platforms.join(", ")}` }))} />
              <h3 style={{ margin: "16px 0 6px" }}>Sample complaints</h3>
              {rows.slice(0, 4).map((r) => (r.samples || []).slice(0, 1).map((s) => (
                <div key={r.category + s.text} style={{ padding: "6px 0", borderTop: "1px solid var(--grid)" }}>
                  <span className="pill">{fmt.label(r.category)}</span> <span className="pill">{s.platform}</span>
                  <div className="quote" style={{ marginTop: 4 }}>“{s.text}”</div>
                </div>
              )))}
            </>
          )}
        </Async>
      </Card>
      <Card title={`Frequently discussed problems · ${brand}`} sub="Most common phrases inside complaints">
        <Async state={problems}>
          {(rows) => <BarList color={color(brand)} items={rows.map((r) => ({ label: r.phrase, value: r.count }))} />}
        </Async>
      </Card>
    </div>
  );
}
