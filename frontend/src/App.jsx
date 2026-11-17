import React, { useEffect, useMemo, useState } from "react";
import { useApi } from "./api.js";
import { BrandCtx, BrandSelect, PLATFORM_LABEL } from "./components/ui.jsx";
import Overview from "./pages/Overview.jsx";
import Engagement from "./pages/Engagement.jsx";
import Sentiment from "./pages/Sentiment.jsx";
import Topics from "./pages/Topics.jsx";
import Complaints from "./pages/Complaints.jsx";
import Influencers from "./pages/Influencers.jsx";
import Trends from "./pages/Trends.jsx";
import Search from "./pages/Search.jsx";

const TABS = [
  ["overview", "Overview", Overview],
  ["engagement", "Engagement", Engagement],
  ["sentiment", "Sentiment", Sentiment],
  ["topics", "Topics", Topics],
  ["complaints", "Complaints & gaps", Complaints],
  ["influencers", "Influencers", Influencers],
  ["trends", "Trend forecast", Trends],
  ["search", "Search", Search],
];

const SOURCE_NAMES = { synthetic: "Synthetic sample", "kaggle:twcs": "Kaggle · Customer Support on Twitter" };

function sourceLabel(sources = []) {
  return sources.map((s) => SOURCE_NAMES[s] || (s.startsWith("kaggle:") ? `Kaggle · ${s.slice(7)}` : s)).join(" + ");
}

/** Plain-language notes on what the loaded dataset lacks and how the analyses adapt. */
function dataNotes(caps) {
  if (!caps) return [];
  const notes = [];
  if (!caps.follower_counts) {
    notes.push("It has no follower counts, so engagement rate is unavailable and influencers are its most active authors per brand.");
  }
  if (!caps.follower_snapshots) notes.push("Growth is measured as unique-author growth (last 14 days vs first 14 days).");
  const missing = ["likes", "shares", "views"].filter((k) => !caps[k]);
  if (missing.length) {
    notes.push(`It has no ${missing.join(", ")}; engagement = ${caps.comments ? "replies received" : "the counts available"}.`);
  }
  if (!caps.products) notes.push("No products were detected, so the trend forecast covers brands only.");
  return notes;
}

function readHash() {
  const t = window.location.hash.replace("#", "");
  return TABS.some(([k]) => k === t) ? t : "overview";
}

export default function App() {
  const meta = useApi("/meta");
  const [tab, setTab] = useState(readHash);
  const [myBrand, setMyBrand] = useState(() => {
    try { return localStorage.getItem("ss.myBrand"); } catch { return null; }
  });
  const [theme, setTheme] = useState(() => {
    try { return localStorage.getItem("ss.theme") || "auto"; } catch { return "auto"; }
  });

  useEffect(() => {
    const onHash = () => setTab(readHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);
  useEffect(() => {
    if (theme === "auto") document.documentElement.removeAttribute("data-theme");
    else document.documentElement.setAttribute("data-theme", theme);
    try { localStorage.setItem("ss.theme", theme); } catch { /* storage unavailable */ }
  }, [theme]);

  const brands = useMemo(() => (meta.data?.brands || []).slice().sort(), [meta.data]);
  const effectiveMy = myBrand && brands.includes(myBrand) ? myBrand : brands[0] || null;
  const chooseMy = (b) => {
    setMyBrand(b);
    try { localStorage.setItem("ss.myBrand", b); } catch { /* storage unavailable */ }
  };

  const Page = TABS.find(([k]) => k === tab)[2];

  return (
    <BrandCtx.Provider value={{ brands, myBrand: effectiveMy, meta: meta.data }}>
      <header className="topbar">
        <div className="brandmark">Social<span>Scope</span></div>
        {meta.data && (
          <div className="meta">
            {sourceLabel(meta.data.sources)} · {meta.data.posts.toLocaleString()} posts · {meta.data.platforms.map((p) => PLATFORM_LABEL[p] || p).join(", ")} ·{" "}
            {meta.data.first_date ? `${meta.data.first_date} to ${meta.data.latest_date}` : `through ${meta.data.latest_date}`}
          </div>
        )}
        <div className="spacer" />
        {brands.length > 0 && (
          <label className="check">My brand <BrandSelect value={effectiveMy} onChange={chooseMy} allowAll={false} /></label>
        )}
        <select value={theme} onChange={(e) => setTheme(e.target.value)} aria-label="Theme">
          <option value="auto">Auto theme</option>
          <option value="light">Light</option>
          <option value="dark">Dark</option>
        </select>
      </header>
      <nav className="tabs">
        {TABS.map(([k, label]) => (
          <button key={k} className={`tab ${tab === k ? "active" : ""}`} onClick={() => { window.location.hash = k; }}>
            {label}
          </button>
        ))}
      </nav>
      <main>
        {meta.data && dataNotes(meta.data.capabilities).length > 0 && (
          <div className="notice">
            <b>About this dataset:</b> {dataNotes(meta.data.capabilities).join(" ")}
          </div>
        )}
        {meta.error ? (
          <div className="card error">
            Couldn’t load analytics: {meta.error.message}. Start the API and run the Spark batch job — see README.
          </div>
        ) : meta.loading ? (
          <div className="empty">Loading…</div>
        ) : (
          <Page />
        )}
      </main>
    </BrandCtx.Provider>
  );
}
