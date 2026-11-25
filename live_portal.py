"""
247Sports live transfer portal list -> cfbquant (/portal): the current cycle's entrants as they land.

Opens https://247sports.com/season/<CYCLE>-football/transferportal/ ("Latest" view) in headless
Chromium, clicks "Load More Players" until the list ends, reads every li.transfer-player row out of
the DOM, and posts the rows to cfbquant's ingest RPC (public.aq_247_portal_ingest). That keeps the
raw list; pg_cron `aq-247-portal-apply` mirrors it into the /portal tables within 10 minutes, and
drops a player from the 247 mirror once CFBD's own feed carries him.

Why (Nelson, 2026-11-25): CFBD's portal feed had 0 entrants for the 2027 cycle while 247's list
already showed the first FCS entrants, so /portal could not show who is entering.

Env:
  CYCLE                     the portal cycle (the season the move is for); blank = the next January window
  MAX_CLICKS                "Load More Players" clicks at most (default 400, ~10,000 rows)
  DRY_RUN                   "true" = scrape and print only; writes live_<cycle>.json, posts nothing
  CFBQ_SUPABASE_URL         https://<ref>.supabase.co
  CFBQ_SUPABASE_KEY         the project's publishable key (public: the site ships it)
  CFBQ_PORTAL_INGEST_TOKEN  the token aq_247_portal_ingest checks (Vault: cfbq_portal_ingest_token)

Exit 0 = posted (or dry run done). Exit 1 = the list never loaded (a block or challenge page),
the scrape came back with rows but no ids, or the post failed.
"""
import asyncio
import json
import os
import random
import re
import sys
from collections import Counter
from datetime import datetime, timezone

import requests
from playwright.async_api import async_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
ROW = "li.transfer-player"
LOAD_MORE = ('.transfer-group-loadMore, .showmore_lnk, '
             'button:has-text("Load More Players"), a:has-text("Load More Players")')


def default_cycle():
    now = datetime.now(timezone.utc)
    # Aug-Dec entries belong to the next January window; Jan-Jul to this year's.
    return now.year + 1 if now.month >= 8 else now.year


CYCLE = int(os.getenv("CYCLE") or 0) or default_cycle()
MAX_CLICKS = int(os.getenv("MAX_CLICKS") or 400)
DRY_RUN = (os.getenv("DRY_RUN") or "false").strip().lower() == "true"
URL = f"https://247sports.com/season/{CYCLE}-football/transferportal/"

# Every row, read in the page. The group header ("11/24/26 Updates") dates the rows under it.
EXTRACT_JS = r"""
() => {
  const clean = (s) => (s == null ? null : s.replace(/\s+/g, ' ').trim() || null);
  const out = [];
  const rows = document.querySelectorAll('li.transfer-player');
  for (const li of rows) {
    const group = li.closest('section.transfer-group');
    const h2 = group ? group.querySelector('h2') : null;
    const gm = h2 ? h2.textContent.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/) : null;
    let groupDate = null;
    if (gm) {
      let y = Number(gm[3]); if (y < 100) y += 2000;
      groupDate = `${y}-${String(gm[1]).padStart(2, '0')}-${String(gm[2]).padStart(2, '0')}`;
    }
    const txt = (sel) => { const e = li.querySelector(sel); return e ? clean(e.textContent) : null; };
    const a = li.querySelector('h3 a') || li.querySelector('a[href*="/player/"]');
    const stars = li.querySelectorAll('.starContainer svg path[fill="#FBD032"], .starContainer svg path[fill="#fbd032"]').length;
    const teams = [...li.querySelectorAll('img.logo, .transfer-prediction img')].map((img) => {
      const link = img.closest('a');
      return { alt: clean(img.getAttribute('alt')), cls: img.getAttribute('class') || '',
               href: link ? link.href : null, inPredictions: !!img.closest('ul.predictions') };
    });
    out.push({
      name: a ? clean(a.textContent) : null,
      url: a ? a.href : null,
      rating_text: txt('.rating'),
      rating_kind: txt('.trend'),
      position: txt('.position'),
      bio: txt('.bio'),
      status: txt('.status'),
      status_date: txt('.statusDate'),
      group_date: groupDate,
      stars,
      teams,
      predictions: txt('ul.predictions'),
    });
  }
  return out;
}
"""


def team_slug(href):
    m = re.search(r"/college/([^/]+)/", href or "")
    return m.group(1) if m else None


def shape(r):
    """One page row -> the ingest RPC's row (aq_247_portal_ingest's jsonb_to_recordset columns)."""
    m = re.search(r"-(\d+)/college-\d+", r.get("url") or "") or re.search(r"/player/[^/]*?-(\d+)/?", r.get("url") or "")
    id_247 = int(m.group(1)) if m else None
    rating = None
    rm = re.search(r"\d+\.\d+", r.get("rating_text") or "")
    if rm:
        rating = float(rm.group(0))
    height = weight = None
    bm = re.match(r"\s*([0-9][0-9'\"\-. ]*?)\s*/\s*(\d+)", r.get("bio") or "")
    if bm:
        height, weight = bm.group(1).strip(), int(bm.group(2))
    logos = [t for t in (r.get("teams") or []) if not t.get("inPredictions")]
    origin = next((t for t in logos if "source" in (t.get("cls") or "")), logos[0] if logos else None)
    dest = next((t for t in logos if re.search(r"dest|commit|target", t.get("cls") or "")), None)
    if dest is None and len(logos) > 1:
        dest = next((t for t in logos if t is not origin), None)
    return {
        "id_247": id_247,
        "name": r.get("name"),
        "position": r.get("position"),
        "height": height,
        "weight": weight,
        "stars": r.get("stars") or None,
        "rating": rating,
        "rating_kind": r.get("rating_kind"),
        "status": r.get("status"),
        "status_date": r.get("status_date"),
        "group_date": r.get("group_date"),
        "origin_team": origin.get("alt") if origin else None,
        "origin_slug": team_slug(origin.get("href")) if origin else None,
        "dest_team": dest.get("alt") if dest else None,
        "dest_slug": team_slug(dest.get("href")) if dest else None,
        "url": r.get("url"),
    }


async def scrape():
    async with async_playwright() as p:
        # CHROME_PATH: a local Chrome instead of Playwright's own build (testing off Actions only).
        browser = await p.chromium.launch(headless=True, executable_path=os.getenv("CHROME_PATH") or None,
                                          args=["--disable-blink-features=AutomationControlled"])
        ctx = await browser.new_context(user_agent=UA, locale="en-US", timezone_id="America/New_York",
                                        viewport={"width": 1366, "height": 900})
        await ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = await ctx.new_page()
        resp = await page.goto(URL, timeout=90000, wait_until="domcontentloaded")
        print(f"GET {URL} -> {resp.status if resp else 'no response'}")
        try:
            # A challenge page, if one is served, runs its script and reloads into the list.
            await page.wait_for_selector(ROW, timeout=90000)
        except Exception:
            title = await page.title()
            body = (await page.inner_text("body"))[:400].replace("\n", " ")
            empty = "transfer portal" in title.lower() and await page.locator("section.transfer-results").count() > 0
            await browser.close()
            if empty:
                print(f"The {CYCLE} list is empty (page loaded: {title!r}).")
                return [], True
            print(f"!!! the list never loaded: title={title!r} body={body!r}")
            sys.exit(1)

        clicks, complete = 0, False
        while clicks < MAX_CLICKS:
            btn = page.locator(LOAD_MORE).first
            if await btn.count() == 0 or not await btn.is_visible():
                complete = True
                break
            before = await page.locator(ROW).count()
            try:
                await btn.scroll_into_view_if_needed(timeout=10000)
                await btn.click(timeout=10000)
                await page.wait_for_function(
                    f"document.querySelectorAll('{ROW}').length > {before}", timeout=30000)
            except Exception as e:
                # The button can linger after the last page; no growth twice in a row = the end.
                await asyncio.sleep(3)
                if await page.locator(ROW).count() <= before:
                    complete = await btn.count() == 0 or not await btn.is_visible()
                    print(f"Load More stopped growing the list after {clicks} clicks ({before} rows): "
                          f"{type(e).__name__}; complete={complete}")
                    break
            clicks += 1
            await asyncio.sleep(random.uniform(0.6, 1.4))
        rows = await page.evaluate(EXTRACT_JS)
        print(f"{len(rows)} rows after {clicks} Load More clicks; list complete: {complete}")
        await browser.close()
        return rows, complete


def main():
    raw, complete = asyncio.run(scrape())
    rows = [shape(r) for r in raw]
    with_id = [r for r in rows if r["id_247"] and r["name"]]
    if raw and not with_id:
        print("!!! rows came back without 247 ids; the page markup has changed. First raw row:")
        print(json.dumps(raw[0])[:1500])
        sys.exit(1)
    dup = len(with_id) - len({r["id_247"] for r in with_id})

    print(f"cycle {CYCLE}: {len(with_id)} players ({dup} repeated ids), "
          f"status {dict(Counter(r['status'] for r in with_id))}")
    print(f"  entry dates {dict(sorted(Counter(r['group_date'] for r in with_id).items()))}")
    print(f"  origin read on {sum(1 for r in with_id if r['origin_team'])}, "
          f"destination on {sum(1 for r in with_id if r['dest_team'])}, "
          f"rating kinds {dict(Counter(r['rating_kind'] for r in with_id))}")
    for r in with_id[:5]:
        print(f"  {r['name']} | {r['position']} | {r['origin_team']} -> {r['dest_team']} | "
              f"{r['status']} | {r['group_date']} | {r['rating']} {r['rating_kind']}")

    if DRY_RUN:
        path = f"live_{CYCLE}.json"
        with open(path, "w") as f:
            json.dump({"cycle": CYCLE, "complete": complete, "rows": with_id, "raw_sample": raw[:30]}, f, indent=1)
        print(f"dry run: wrote {path}; nothing posted")
        return

    base = (os.getenv("CFBQ_SUPABASE_URL") or "").rstrip("/")
    key = os.getenv("CFBQ_SUPABASE_KEY") or ""
    token = os.getenv("CFBQ_PORTAL_INGEST_TOKEN") or ""
    if not (base and key and token):
        print("!!! CFBQ_SUPABASE_URL, CFBQ_SUPABASE_KEY and CFBQ_PORTAL_INGEST_TOKEN are all required to post")
        sys.exit(1)
    res = requests.post(f"{base}/rest/v1/rpc/aq_247_portal_ingest",
                        headers={"apikey": key, "Content-Type": "application/json"},
                        json={"p_token": token, "p_cycle": CYCLE, "p_rows": with_id, "p_complete": complete},
                        timeout=90)
    if res.status_code >= 300:
        print(f"!!! ingest failed: HTTP {res.status_code} {res.text[:500]}")
        sys.exit(1)
    print(f"ingest: {res.text[:500]}")


if __name__ == "__main__":
    main()
