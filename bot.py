# bot.py
"""
Daily SmartInsider scraper -> Discord (or any) webhook
- Section 1: New filings (last 24h), grouped by ticker: #people, buys, sells, names
- Section 2: 30-day trend (buys & sells columns), ranked by buys
"""
import os, time, re, textwrap, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from dateutil import tz
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

SMART_URL = "https://www.smartinsider.com/politicians/"
UA = "CongressTradesBot/1.0 (+contact: webhook-report)"

def _now_tz(name="Europe/London"):
    return datetime.now(tz.gettz(name))

def _to_utc_naive(dt):
    return dt.astimezone(timezone.utc).replace(tzinfo=None)

def _mk_dirs(path): os.makedirs(path, exist_ok=True)

def _post_webhook(url, content):
    if not url: return
    for chunk in textwrap.wrap(content, 1900, replace_whitespace=False):
        r = requests.post(url, json={"content": chunk}, timeout=30)
        r.raise_for_status()

def _guess_header_map(th_texts):
    th = [re.sub(r"\s+"," ",t).strip().lower() for t in th_texts]
    want = {
        "ticker": ["ticker","symbol"],
        "name": ["politician","member","representative","senator","name"],
        "company": ["company","asset","asset description"],
        "type": ["type","transaction","transaction type"],
        "filing_date": ["filing","disclosure","filed","filing date","disclosure date","date filed"],
        "transaction_date": ["transaction date","trade date","date of transaction"],
    }
    out = {k:-1 for k in want}
    for i,t in enumerate(th):
        for k,keys in want.items():
            if out[k] == -1 and any(key in t for key in keys): out[k]=i
    return out

def _extract_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    frames=[]
    for tbl in soup.find_all("table"):
        head = tbl.find("thead") or tbl
        ths = [x.get_text(strip=True) for x in head.find_all(["th","td"], limit=20)]
        if not ths:
            fr = tbl.find("tr")
            if fr: ths = [td.get_text(strip=True) for td in fr.find_all(["th","td"])]
        if not ths: continue
        cmap = _guess_header_map(ths)
        if any(cmap[k]==-1 for k in ("ticker","name","type")): continue
        rows=[]
        for tr in tbl.find_all("tr"):
            tds = tr.find_all(["td","th"])
            if len(tds) <= max(cmap.values()): continue
            rows.append([td.get_text(" ", strip=True) for td in tds])
        if rows and [x.lower() for x in rows[0]] == [x.lower() for x in ths]:
            rows=rows[1:]
        if not rows: continue
        def pick(k): i=cmap.get(k,-1); return [r[i] if 0<=i<len(r) else "" for r in rows]
        df = pd.DataFrame({
            "ticker": pick("ticker"),
            "name": pick("name"),
            "company": pick("company"),
            "type": pick("type"),
            "filing_date": pick("filing_date"),
            "transaction_date": pick("transaction_date"),
        })
        frames.append(df)
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["ticker","name","company","type","filing_date","transaction_date"]
    )
    # clean
    df["ticker"] = df["ticker"].astype(str).str.upper().str.replace(r"[^A-Z.]", "", regex=True)
    for c in ["name","company","type"]: df[c]=df[c].astype(str).str.strip()
    for d in ["filing_date","transaction_date"]: df[d]=pd.to_datetime(df[d], errors="coerce")
    df["is_buy"]  = df["type"].str.contains(r"buy|purchase", case=False, na=False)
    df["is_sell"] = df["type"].str.contains(r"sell|sale",    case=False, na=False)
    df = df[(df["ticker"]!="") & (~df["filing_date"].isna())]
    return df.drop_duplicates()

def scrape_smartinsider(headless=True, wait_ms=1500, max_pages=50):
    with sync_playwright() as p:
        b = p.chromium.launch(headless=headless)
        ctx = b.new_context(user_agent=UA)
        page = ctx.new_page()
        page.goto(SMART_URL, timeout=30_000)
        page.wait_for_load_state("domcontentloaded")
        time.sleep(wait_ms/1000)
        df = _extract_from_html(page.content())

        # naive pagination support
        visited=1
        while visited<max_pages:
            locs = page.locator("text=/^(Next|›|→)$/i").all() or page.locator("[aria-label='Next']").all()
            if not locs: break
            try:
                locs[0].click()
                page.wait_for_load_state("domcontentloaded")
                time.sleep(wait_ms/1000)
                d2 = _extract_from_html(page.content())
                if d2.empty: break
                df = pd.concat([df,d2], ignore_index=True)
                visited += 1
            except Exception:
                break
        ctx.close(); b.close()
    return df

def load_public_fallback():
    # Senate + House mirrors — resilience if scraping fails
    urls = [
        "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json",
        "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json",
    ]
    frames=[]
    for u in urls:
        try:
            r=requests.get(u,timeout=60); r.raise_for_status()
            frames.append(pd.DataFrame(r.json()))
        except Exception:
            pass
    if not frames: return pd.DataFrame()
    df=pd.concat(frames, ignore_index=True)
    df=df.rename(columns={
        "transaction_date":"transaction_date",
        "disclosure_date":"filing_date",
        "senator":"name", "representative":"name",
        "ticker":"ticker","asset_description":"company","type":"type",
    })
    for c in ["filing_date","transaction_date"]: df[c]=pd.to_datetime(df[c], errors="coerce")
    df["ticker"]=df["ticker"].astype(str).str.upper().str.replace(r"[^A-Z.]","",regex=True)
    for c in ["name","company","type"]: df[c]=df[c].astype(str).str.strip()
    df["is_buy"]=df["type"].str.contains(r"buy|purchase",case=False,na=False)
    df["is_sell"]=df["type"].str.contains(r"sell|sale",case=False,na=False)
    return df[(df["ticker"]!="") & (~df["filing_date"].isna())].drop_duplicates()

def group_daily(df, since, whitelist=None, blacklist=None):
    since=pd.Timestamp(since)
    d=df[df["filing_date"]>=since].copy()
    if whitelist: d=d[d["ticker"].isin(whitelist)]
    if blacklist: d=d[~d["ticker"].isin(blacklist)]
    agg=(d.groupby("ticker")
           .agg(trades=("ticker","size"),
                politicians=("name", lambda s: sorted(set([x for x in s if isinstance(x,str)]))),
                buys=("is_buy","sum"), sells=("is_sell","sum"))
           .reset_index())
    agg["people"]=agg["politicians"].apply(len)
    return d, agg.sort_values(["people","trades"], ascending=[False,False])

def trend_30d(df, since, whitelist=None, blacklist=None):
    t=df[df["filing_date"]>=pd.Timestamp(since)].copy()
    if whitelist: t=t[t["ticker"].isin(whitelist)]
    if blacklist: t=t[~t["ticker"].isin(blacklist)]
    trend=(t.groupby("ticker")
             .agg(buys=("is_buy","sum"),
                  sells=("is_sell","sum"),
                  politicians=("name", lambda s: sorted(set([x for x in s if isinstance(x,str)]))))
             .reset_index())
    trend["total_trades"]=trend["buys"]+trend["sells"]
    trend=trend.sort_values(["buys","total_trades"], ascending=[False,False])
    return t, trend

def render_md(now_dt, tzname, daily_since, trend_since, daily_agg, trend_agg):
    def human(dt): return dt.strftime("%Y-%m-%d %H:%M")
    lines=[f"# Congressional Trades — {now_dt.strftime('%Y-%m-%d')} ({tzname})\n"]
    lines.append(f"## New Filings (since {human(daily_since)})\n")
    if daily_agg.empty:
        lines.append("No new filings in the period.\n")
    else:
        for _,r in daily_agg.iterrows():
            names=", ".join(r["politicians"]) if isinstance(r["politicians"],list) else ""
            lines.append(f"- **{r['ticker']}** — {int(r['people'])} people, {int(r['trades'])} trades (buys: {int(r['buys'])}, sells: {int(r['sells'])}) — {names}")
        lines.append("")
    lines.append(f"## 30-Day Trend (since {trend_since.strftime('%Y-%m-%d')}) — Ranked by Buys\n")
    if trend_agg.empty:
        lines.append("No filings in the 30-day window.\n")
    else:
        lines.append("| Rank | Ticker | Buys | Sells | Total | Politicians |")
        lines.append("|---:|:---:|---:|---:|---:|:--|")
        for i,r in enumerate(trend_agg.itertuples(index=False), start=1):
            names=", ".join(r.politicians) if isinstance(r.politicians,list) else ""
            lines.append(f"| {i} | **{r.ticker}** | {int(r.buys)} | {int(r.sells)} | {int(r.total_trades)} | {names} |")
        lines.append("")
    return "\n".join(lines)

def main():
    load_dotenv()
    tzname=os.getenv("TIMEZONE","Europe/London")
    outdir=os.getenv("OUTPUT_DIR","./out"); _mk_dirs(outdir)
    webhook=os.getenv("DISCORD_WEBHOOK_URL","").strip()
    headless=(os.getenv("HEADLESS","true").lower() in ("1","true","yes","y"))
    wait_ms=int(os.getenv("WAIT_AFTER_LOAD_MS","1500"))
    daily_hours=int(os.getenv("DAILY_WINDOW_HOURS","24"))
    lookback_days=int(os.getenv("LOOKBACK_DAYS","30"))
    whitelist=[t.strip().upper() for t in os.getenv("WHITELIST_TICKERS","").split(",") if t.strip()]
    blacklist=[t.strip().upper() for t in os.getenv("BLACKLIST_TICKERS","").split(",") if t.strip()]

    # scrape
    try:
        df = scrape_smartinsider(headless=headless, wait_ms=wait_ms)
    except Exception as e:
        print("Scrape failed:", e)
        df = pd.DataFrame()

    # fallback if needed
    if df.empty:
        print("Using fallback mirrors…")
        df = load_public_fallback()
    if df.empty:
        raise SystemExit("No data available.")

    now = _now_tz(tzname)
    daily_since = now - timedelta(hours=daily_hours)
    trend_since = now - timedelta(days=lookback_days)

    _, daily_agg = group_daily(df, _to_utc_naive(daily_since), whitelist, blacklist)
    _, trend_agg = trend_30d(df, _to_utc_naive(trend_since), whitelist, blacklist)

    md = render_md(now, tzname, daily_since, trend_since, daily_agg, trend_agg)

    # save + post
    with open(os.path.join(outdir, f"report_{now.strftime('%Y%m%d')}.md"), "w", encoding="utf-8") as f:
        f.write(md)
    daily_agg.to_csv(os.path.join(outdir, f"daily_agg_{now.strftime('%Y%m%d')}.csv"), index=False)
    trend_agg.to_csv(os.path.join(outdir, f"trend_agg_{now.strftime('%Y%m%d')}.csv"), index=False)

    _post_webhook(webhook, md)
    print("Done.")

if __name__ == "__main__":
    main()
