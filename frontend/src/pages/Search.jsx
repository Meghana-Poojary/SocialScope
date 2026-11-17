import React, { useState } from "react";
import { useApi } from "../api.js";
import { Async, BrandSelect, Card, PLATFORM_LABEL, SentimentPill, Swatch, fmt, useBrandColor, usePlatformOptions } from "../components/ui.jsx";

export default function Search() {
  const [draft, setDraft] = useState("battery");
  const [q, setQ] = useState("battery");
  const [brand, setBrand] = useState(null);
  const [platform, setPlatform] = useState("");
  const [sentiment, setSentiment] = useState("");
  const [complaintsOnly, setComplaintsOnly] = useState(false);
  const res = useApi("/search", { q, brand, platform, sentiment, complaints_only: complaintsOnly || undefined, size: 30 });
  const color = useBrandColor();
  const platformOptions = usePlatformOptions({ all: false });

  return (
    <Card
      title="Search every post"
      sub="Full-text search powered by Elasticsearch"
      actions={
        <form className="filters" onSubmit={(e) => { e.preventDefault(); setQ(draft); }}>
          <input type="search" value={draft} onChange={(e) => setDraft(e.target.value)} placeholder="e.g. refund, noise cancellation…" aria-label="Search text" />
          <BrandSelect value={brand} onChange={setBrand} />
          <select value={platform} onChange={(e) => setPlatform(e.target.value)} aria-label="Platform">
            <option value="">All platforms</option>
            {platformOptions.map(([k, l]) => <option key={k} value={k}>{l}</option>)}
          </select>
          <select value={sentiment} onChange={(e) => setSentiment(e.target.value)} aria-label="Sentiment">
            <option value="">Any sentiment</option><option value="positive">Positive</option>
            <option value="neutral">Neutral</option><option value="negative">Negative</option>
          </select>
          <label className="check"><input type="checkbox" checked={complaintsOnly} onChange={(e) => setComplaintsOnly(e.target.checked)} />Complaints only</label>
          <button type="submit" className="pill" style={{ border: "1px solid var(--border)", cursor: "pointer", padding: "5px 12px" }}>Search</button>
        </form>
      }
    >
      <Async state={res} empty="No matching posts.">
        {(r) => (
          <>
            <div className="legend" style={{ marginBottom: 8 }}>
              <span><b>{fmt.int(r.total)}</b>&nbsp;matches</span>
              {Object.entries(r.by_brand).map(([b, n]) => <span key={b}><Swatch color={color(b)} />{b} {fmt.int(n)}</span>)}
            </div>
            <div className="hits">
              {r.hits.map((h) => (
                <div className="hit" key={h.post_id}>
                  <div>{h.text}</div>
                  <div className="hit-meta">
                    <span style={{ display: "inline-flex", gap: 4, alignItems: "center" }}><Swatch color={color(h.brand)} />{h.brand}</span>
                    <span>{PLATFORM_LABEL[h.platform]}</span>
                    <span>@{h.author}</span>
                    <SentimentPill value={h.sentiment} />
                    {(h.categories || []).map((c) => <span className="pill neg" key={c}>{fmt.label(c)}</span>)}
                    <span>{fmt.compact(h.engagement)} engagements</span>
                    <span>{h.created_at?.slice(0, 10)}</span>
                  </div>
                </div>
              ))}
            </div>
          </>
        )}
      </Async>
    </Card>
  );
}
