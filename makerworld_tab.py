# /// script
# requires-python = ">=3.12"
#
# [tool.orcaslicer.plugin]
# name = "MakerWorld Tab"
# description = "Browse MakerWorld (home feeds, categories, contests, curated collections, search with sort and filters, designers, your collections, likes and downloads) in its own tab and open a model's 3MF in OrcaSlicer."
# author = "Royrdan"
# version = "0.2.0"
# ///
"""MakerWorld Tab.

Page  --orca.postMessage({cmd: ...})-->  on_message()  --> worker threads do the network work
Page  <--post_message({type: ...})--     worker (post_message is marshalled to the UI thread by Orca)

Two workers: "browse" (lists, details, config, sign-in) and "open" (prepare + download + hand-off), so
scrolling keeps working while a model downloads.

Opening a model: design JSON -> user picks a print profile (instance) -> /f3mf (needs the saved
Bambu login token) -> streamed download into Orca's own "Downloads folder" (app.download_path) ->
`/app/bin/entrypoint <file>` (through a fixed /bin/sh line so the process grant is asked once),
which hands the file to the running Orca when Preferences > single instance is on.

Endpoints (all verified 2026-11-25 against makerworld.com/api/v1/):
  personal: user-service/my/profile, design-service/my/favorites/listlite, design-service/my/design/like,
            design-service/my/favorites/download/designs, search-service/homepage/following
  public:   search-service/homepage/nav, search-service/select/design/nav2 (paged by seed),
            search-service/select/design2 (search + sort + filters + contestId), search-service/select/design/makerlab,
            search-service/search/hot-rank, design-service/contest/list, design-service/design/awarded,
            design-service/favorites-collections/tab, design-service/favorites/<id>/designs,
            design-service/published/<uid>/design, design-user-service/user/profile/<uid>, design-service/design/<id>
  category tree + sort/filter options: only in the /en/3d-models page's __NEXT_DATA__ (fetched once, cached).
"""
import html as htmllib
import json
import os
import queue
import random
import re
import sqlite3
import subprocess
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from html.parser import HTMLParser

import orca

VERSION = "0.2.0"
DATA_DIR = os.path.join(os.environ.get("XDG_CONFIG_HOME", os.path.expanduser("~/.config")), "OrcaSlicer")
COOKIE_DB = os.path.join(DATA_DIR, "cache", "cloud_cookies.db")
FALLBACK_DL_DIR = os.path.join(DATA_DIR, "makerworld_downloads")
LOG = os.path.join(DATA_DIR, "makerworld_tab.log")
TOKEN_FILE = os.path.join(DATA_DIR, "makerworld_tab_token.json")
ORCA_BIN = "/app/bin/entrypoint"
# Fixed argv: the file goes in through the environment, so OrcaSlicer's process permission
# (which in 2.5.2 stores the whole argv) is granted once instead of once per downloaded file.
OPEN_ARGV = ["/bin/sh", "-c", 'exec /app/bin/entrypoint "$MW_TAB_FILE"']
MW = "https://makerworld.com"
MWAPI = MW + "/api/v1/"
BAMBU_API = "https://api.bambulab.com"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/605.1.15 (KHTML, like Gecko) BBL-Slicer/v02.04.00.70"
PAGE_SIZE = 24
# Path components OrcaSlicer's plugin audit always refuses (plugin-permissions.md "denied paths").
DENIED_COMPONENTS = {"secret", "secrets", "cert", "certs", "certificate", "certificates", "conf", "config"}

# Snapshot of makerworld.com's own menus (2026-11-25), used when the live page can't be read.
SNAP_TREE = [[900, "3D Printer", [[901, "3D Printer Accessories"], [902, "3D Printer Parts"], [903, "Test Models"]]], [100, "Art", [[101, "2D Art"], [103, "Coin & Badges"], [102, "Signs & Logos"], [104, "Sculptures"], [105, "Other Art Models"]]], [500, "Education", [[503, "Biology"], [505, "Chemistry"], [501, "Engineering"], [506, "Geography"], [502, "Mathematics"], [504, "Physics & Astronomy"], [507, "Other Education Models"]]], [200, "Fashion", [[201, "Bags"], [202, "Clothes"], [206, "Earrings"], [204, "Footwear"], [203, "Glasses"], [208, "Jewelry"], [205, "Rings"], [207, "Other Fashion Models"]]], [300, "Hobby & DIY", [[301, "Electronics"], [303, "Music"], [304, "RC"], [305, "Robotics"], [306, "Sport & Outdoors"], [302, "Vehicles"], [307, "Other Hobby & DIY"]]], [400, "Household", [[401, "Decor"], [403, "Festivities"], [402, "Garden"], [404, "Office"], [405, "Pets"], [406, "Other House Models"]]], [600, "Miniatures", [[601, "Animals"], [602, "Architecture"], [603, "Creatures"], [604, "People"], [605, "Other Miniatures"]]], [1000, "Props & Cosplays", [[1003, "Costumes"], [1001, "Masks & Helmets"], [1002, "Cosplay Weapons"], [1004, "Other Props & Cosplays"]]], [700, "Tools", [[705, "Gadgets"], [703, "Hand Tools"], [704, "Machine Tools"], [702, "Measure Tools"], [707, "Medical Tools"], [701, "Organizers"], [706, "Other Tools"]]], [800, "Toys & Games", [[802, "Board Games"], [801, "Characters"], [803, "Outdoor Toys"], [804, "Puzzles"], [806, "Construction Sets"], [805, "Other Toys & Games"]]], [2000, "Generative 3D Model", [[2001, "Hueforge & Lithophane"], [2002, "Make My Sign"], [2003, "Make My Vase"], [2004, "Pixel Puzzle Maker"], [2005, "Relief Sculpture Maker"], [2006, "AI Scanner"], [2007, "Image to Keychain"], [2008, "Make My Desk Organizer"], [2009, "PrintMon Maker"], [2010, "Statue Maker"], [2011, "Christmas Ornament Maker"], [2012, "Make My Lantern"]]]]
SNAP_TABS = [["OFFICIAL_RECOMMENDATIONS", "Official Recommendations"], ["OFFICE_&_WORK", "Office & Work"], ["EDUCATION", "Education"], ["JUST_FOR_FUN", "Just for Fun"], ["PRINTER_ACCESSORIES", "Printer Accessories"], ["VEHICLES", "Vehicles"]]
SNAP_SORTS = [["hotScore", "Trending"], ["newUploads", "Newest"], ["downloadCount", "Most downloaded"], ["likeCount", "Most liked"], ["boosts", "Most boosted"], ["random", "Random"]]
SNAP_SEARCH_SORTS = [["score", "Best match"]] + SNAP_SORTS[:5]
SNAP_MODEL_TAGS = [["featured", "Staff picks (Featured)"], ["exclusive", "Exclusive"], ["custom", "Customizable"], ["param", "Parametric"], ["cb", "CyberBrick"], ["ms", "Maker's Supply"]]
SNAP_LICENSES = [["CC0", "Public Domain"], ["BY", "BY"], ["BY-SA", "BY-SA"], ["BY-ND", "BY-ND"], ["BY-NC", "BY-NC"], ["BY-NC-SA", "BY-NC-SA"], ["BY-NC-ND", "BY-NC-ND"], ["Standard Digital File License", "Standard Digital File License"]]
SNAP_NAVS = [["Trending", "Trending"], ["category_400", "Household"], ["category_800", "Toys & Games"], ["category_700", "Tools"], ["category_300", "Hobby & DIY"], ["category_900", "3D Printer"], ["category_100", "Art"], ["category_600", "Miniatures"], ["category_1000", "Props & Cosplays"], ["category_200", "Fashion"], ["category_2000", "Generative 3D Model"], ["category_500", "Education"]]
SORT_LABELS = {"hotScore": "Trending", "newUploads": "Newest", "downloadCount": "Most downloaded", "likeCount": "Most liked",
               "boosts": "Most boosted", "random": "Random", "score": "Best match"}
MODEL_TAG_LABELS = {"featured": "Staff picks (Featured)"}


def log(msg):
    try:
        with open(LOG, "a") as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S ") + str(msg) + "\n")
    except Exception:
        pass


def read_orca_download_dir():
    """Orca's Preferences 'Downloads folder' = app.download_path in OrcaSlicer.conf.

    Must run while register_capabilities runs: the plugin audit refuses any *.conf read once a
    capability call is in scope. Returns (folder, reason)."""
    try:
        with open(os.path.join(DATA_DIR, "OrcaSlicer.conf"), encoding="utf-8") as f:
            text = f.read()
        conf = json.loads("\n".join(l for l in text.splitlines() if not l.lstrip().startswith("#")))
        path = (conf.get("app") or {}).get("download_path") or ""
    except Exception as e:
        return FALLBACK_DL_DIR, "could not read OrcaSlicer.conf (%r)" % e
    if not path:
        return FALLBACK_DL_DIR, "Orca has no downloads folder set"
    path = os.path.realpath(os.path.expanduser(path))
    if not os.path.isabs(path):
        return FALLBACK_DL_DIR, "downloads folder %r is not absolute" % path
    if not path.startswith(DATA_DIR + os.sep):
        bad = [c for c in path.split(os.sep) if c.lower() in DENIED_COMPONENTS or c.lower().endswith((".conf", ".ini"))]
        if bad:
            return FALLBACK_DL_DIR, "downloads folder %r has a name Orca's plugin guard blocks (%s)" % (path, bad)
    if not os.path.isdir(path):
        return FALLBACK_DL_DIR, "downloads folder %r does not exist" % path
    if not os.access(path, os.W_OK):
        return FALLBACK_DL_DIR, "downloads folder %r is not writable from inside the flatpak" % path
    return path, "Orca downloads folder"


DL_DIR, DL_REASON = FALLBACK_DL_DIR, "not read yet"


def saved_token():
    try:
        with open(TOKEN_FILE) as f:
            return json.load(f).get("token")
    except Exception:
        return None


def save_token(tok):
    with open(TOKEN_FILE, "w") as f:
        json.dump({"token": tok, "saved": time.time()}, f)
    os.chmod(TOKEN_FILE, 0o600)


def http_post(url, body, want_cookies=False):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"User-Agent": UA, "Content-Type": "application/json",
                                          "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw, code, hdrs = r.read(), r.status, r.headers
    except urllib.error.HTTPError as e:
        raw, code, hdrs = e.read(), e.code, e.headers
    try:
        d = json.loads(raw.decode("utf-8", "replace") or "{}")
    except Exception:
        d = {"error": raw.decode("utf-8", "replace")[:200]}
    if want_cookies:
        for c in hdrs.get_all("Set-Cookie") or []:
            m = re.match(r"\s*token=([^;]+)", c)
            if m:
                d["_cookie_token"] = m.group(1)
    return code, d


def mw_cookies():
    """Login token: our own saved Bambu token first, else Orca's web cookie store (never had one in tests)."""
    out = {}
    tok = saved_token()
    if tok:
        out["token"] = tok
        return out
    if not os.path.exists(COOKIE_DB):
        return out
    con = sqlite3.connect("file:%s?mode=ro" % COOKIE_DB, uri=True, timeout=2)
    try:
        for name, value, host in con.execute("select name, value, host from moz_cookies"):
            if "makerworld" in host or "bambulab" in host:
                out[name] = value
    finally:
        con.close()
    return out


def http_json(url, cookies=None):
    headers = {"User-Agent": UA, "Accept": "application/json"}
    if cookies:
        headers["Cookie"] = "; ".join("%s=%s" % kv for kv in cookies.items())
        if cookies.get("token"):
            headers["Authorization"] = "Bearer " + cookies["token"]
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode("utf-8", "replace") or "{}")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:300]
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, {"error": body}
    except urllib.error.URLError as e:
        return 0, {"error": "network problem: %s" % e.reason}
    except (ValueError, OSError) as e:
        return 0, {"error": "bad answer: %s" % e}


def http_text(url, max_bytes=6 << 20):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/html"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read(max_bytes).decode("utf-8", "replace")


def api_message(d):
    if isinstance(d, dict):
        return str(d.get("message") or d.get("error") or d.get("msg") or d)[:200]
    return str(d)[:200]


def safe_filename(name, fallback):
    name = re.sub(r'[\x00-\x1f/\\:*?"<>|]+', "_", name or "").strip(" ._")
    name = re.sub(r"\s+", " ", name)
    if not name:
        name = re.sub(r'[\x00-\x1f/\\:*?"<>|]+', "_", str(fallback)).strip(" ._") or "makerworld_model"
    base, ext = os.path.splitext(name)
    if ext.lower() != ".3mf":
        base, ext = name, ".3mf"
    return base[:120].rstrip(" .") + ext


def unique_path(folder, name):
    base, ext = os.path.splitext(name)
    path, n = os.path.join(folder, name), 1
    while os.path.exists(path) or os.path.exists(path + ".part"):
        path = os.path.join(folder, "%s (%d)%s" % (base, n, ext))
        n += 1
    return path


# ---------------------------------------------------------------- description sanitiser
class _Sanitizer(HTMLParser):
    """Keeps a small set of formatting tags; drops every attribute except http(s) link targets and
    https image sources; drops script/style/iframe and friends together with their content."""
    OK = {"p", "br", "b", "strong", "i", "em", "u", "s", "ul", "ol", "li", "h1", "h2", "h3", "h4", "h5", "h6",
          "blockquote", "span", "div", "a", "img", "hr", "table", "thead", "tbody", "tr", "td", "th", "pre", "code",
          "sub", "sup", "figure", "figcaption"}
    VOID = {"br", "hr", "img"}
    DROP = {"script", "style", "iframe", "object", "noscript", "svg", "math", "template", "textarea", "select",
            "title", "head", "embed", "video", "audio"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out, self.stack, self.skip = [], [], 0

    def handle_starttag(self, tag, attrs, closed=False):
        if tag in self.DROP:
            if not closed and tag != "embed":
                self.skip += 1
            return
        if self.skip or tag not in self.OK:
            return
        a, extra = dict(attrs), ""
        if tag == "a":
            href = (a.get("href") or "").strip()
            if re.match(r"https?://", href, re.I):
                extra = ' href="%s" data-ext="1"' % htmllib.escape(href, quote=True)
        elif tag == "img":
            src = (a.get("src") or a.get("data-src") or "").strip()
            if not re.match(r"https://", src, re.I):
                return
            extra = ' src="%s" loading="lazy" alt=""' % htmllib.escape(src, quote=True)
        if tag in self.VOID:
            self.out.append("<%s%s>" % (tag, extra))
            return
        self.out.append("<%s%s>" % (tag, extra))
        if closed:
            self.out.append("</%s>" % tag)
        else:
            self.stack.append(tag)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs, closed=True)

    def handle_endtag(self, tag):
        if tag in self.DROP:
            self.skip = max(0, self.skip - 1)
            return
        if self.skip or tag not in self.stack:
            return
        while self.stack:
            t = self.stack.pop()
            self.out.append("</%s>" % t)
            if t == tag:
                break

    def handle_data(self, data):
        if not self.skip:
            self.out.append(htmllib.escape(data, quote=False))

    def result(self):
        while self.stack:
            self.out.append("</%s>" % self.stack.pop())
        return "".join(self.out)


def sanitize_html(s, limit=120000):
    if not s:
        return ""
    p = _Sanitizer()
    try:
        p.feed(str(s)[:limit * 2])
        p.close()
    except Exception:
        return htmllib.escape(plain_text(s))
    return p.result()[:limit]


def plain_text(s, limit=0):
    t = re.sub(r"<(br|/p|/div|/li|/h\d)\b[^>]*>", "\n", str(s or ""), flags=re.I)
    t = htmllib.unescape(re.sub(r"<[^>]+>", "", t))
    t = re.sub(r"[ \t\r\f\v]+", " ", t)
    t = re.sub(r"\n\s*\n+", "\n", t).strip()
    return (t[:limit].rstrip() + "...") if limit and len(t) > limit else t


def https(u):
    return u if isinstance(u, str) and u.startswith("https://") else ""


# ---------------------------------------------------------------- normalisers
def norm_hit(h):
    c = h.get("designCreator") or {}
    return {"id": h.get("id"), "title": h.get("title") or "", "cover": https(h.get("cover") or h.get("coverUrl")),
            "creator": c.get("name") or "", "uid": c.get("uid") or 0,
            "downloads": h.get("downloadCount") or 0, "likes": h.get("likeCount") or 0,
            "prints": h.get("printCount") or 0, "staff": bool(h.get("isStaffPicked")),
            "excl": bool(h.get("isExclusive"))}


def norm_collection(h):
    covers = [https(c) for c in (h.get("designCover") or []) if isinstance(c, str) and https(c)][:4]
    if not covers and https(h.get("cover")):
        covers = [h["cover"]]
    return {"id": h.get("id"), "title": h.get("title") or "Untitled", "count": h.get("designCnt") or 0,
            "isDefault": bool(h.get("isDefault")), "covers": covers,
            "creator": (h.get("creator") or {}).get("name") or "", "desc": plain_text(h.get("description"), 160)}


CONTEST_STATUS = {1: "Coming soon", 2: "Open for entries", 3: "Ended", 4: "Judging"}


def norm_contest(h):
    return {"id": h.get("id"), "title": h.get("contestName") or h.get("contestTitle") or h.get("bannerTitle") or "Contest",
            "cover": https(h.get("cover")), "status": h.get("status") or 0,
            "statusText": CONTEST_STATUS.get(h.get("status"), ""),
            "start": (h.get("startTime") or "")[:10], "end": (h.get("endTime") or "")[:10],
            "entrants": h.get("contestantsCount") or 0, "theme": plain_text(h.get("themeDesc") or h.get("theme"), 400),
            "desc": plain_text(h.get("contestDesc"), 600),
            "prizes": [str(i.get("reward") or "")[:120] for i in (h.get("incentiveInfo") or [])][:3],
            "winners": len(h.get("awardedDesigns") or [])}


def norm_instance(i, design_cover):
    mi = ((i.get("extention") or {}).get("modelInfo") or {})
    comp = mi.get("compatibility") or {}
    others = [c.get("devProductName") for c in (mi.get("otherCompatibility") or []) if c.get("devProductName")]
    plates = []
    for p in (mi.get("plates") or [])[:40]:
        plates.append({"name": p.get("name") or "Plate %s" % p.get("index", len(plates) + 1),
                       "minutes": int((p.get("prediction") or 0) / 60), "grams": p.get("weight") or 0,
                       "thumb": https(((p.get("thumbnail") or {}).get("url")))})
    fils = [{"type": f.get("type") or "", "color": f.get("color") if re.match(r"^#[0-9A-Fa-f]{3,8}$", str(f.get("color") or "")) else "",
             "grams": f.get("usedG") or "", "meters": f.get("usedM") or ""} for f in (i.get("instanceFilaments") or [])[:16]]
    ic = i.get("instanceCreator") or {}
    return {"id": i.get("id"), "profileId": i.get("profileId"), "title": i.get("title") or "Profile %s" % i.get("id"),
            "printer": comp.get("devProductName") or "", "nozzle": comp.get("nozzleDiameter"),
            "others": others[:12], "plates": len(mi.get("plates") or []), "plateList": plates,
            "minutes": int((i.get("prediction") or 0) / 60), "grams": i.get("weight") or 0,
            "colors": i.get("materialColorCnt") or 0, "ams": bool(i.get("needAms")), "filaments": fils,
            "rating": i.get("ratingScoreTotal") and i.get("ratingCount") and
                      round(i["ratingScoreTotal"] / i["ratingCount"], 1),
            "ratingCount": i.get("ratingCount") or 0,
            "downloads": i.get("downloadCount") or 0, "prints": i.get("printCount") or 0,
            "cover": https(i.get("cover")) or https(design_cover),
            "default": bool(i.get("isDefault")), "size": ((mi.get("f3mf") or {}).get("size") or 0),
            "author": ic.get("name") or "", "authorUid": ic.get("uid") or 0,
            "published": (i.get("publishTime") or i.get("createTime") or "")[:10],
            "official": bool(i.get("isOfficial")), "nozzleTypes": [str(x) for x in (mi.get("nozzleTypeList") or [])][:4],
            "note": plain_text(i.get("summary"), 400)}


def sorted_profiles(d):
    profs = [norm_instance(i, d.get("coverUrl") or d.get("cover")) for i in (d.get("instances") or [])]
    default_id = d.get("defaultInstanceId")
    # Recommended first: MakerWorld's default, else the most-downloaded.
    profs.sort(key=lambda p: (p["id"] != default_id, -p["downloads"]))
    return profs


def norm_detail(d):
    ext = d.get("designExtension") or {}
    pics, seen = [], set()
    for p in (ext.get("design_pictures") or []) + [{"url": d.get("coverUrl")}]:
        u = https((p or {}).get("url"))
        if u and u not in seen:
            seen.add(u)
            pics.append(u)
    pics = pics[:40]
    c = d.get("designCreator") or {}
    profs = sorted_profiles(d)
    rs = sum((i.get("ratingScoreTotal") or 0) for i in d.get("instances") or [])
    rc = sum((i.get("ratingCount") or 0) for i in d.get("instances") or [])
    contest = d.get("contest") or {}
    return {"id": d.get("id"), "title": d.get("title") or "", "pictures": pics,
            "summary": sanitize_html(d.get("summary")),
            "creator": {"uid": c.get("uid") or 0, "name": c.get("name") or "", "handle": c.get("handle") or "",
                        "avatar": https(c.get("avatar")), "fans": c.get("fanCount") or 0},
            "stats": {"likes": d.get("likeCount") or 0, "collects": d.get("collectionCount") or 0,
                      "downloads": d.get("downloadCount") or 0, "prints": d.get("printCount") or 0,
                      "comments": d.get("commentCount") or 0,
                      "rating": round(rs / rc, 1) if rc else 0, "ratingCount": rc},
            "tags": [str(t) for t in (d.get("tags") or [])][:40], "license": d.get("license") or "",
            "categories": [{"id": x.get("id"), "name": x.get("name")} for x in (d.get("categories") or []) if x.get("id")],
            "created": (d.get("createTime") or "")[:10], "updated": (d.get("updateTime") or "")[:10],
            "staff": bool(d.get("isStaffPicked")), "pickReason": plain_text(d.get("pickReason"), 300),
            "excl": bool(d.get("isExclusive")), "official": bool(d.get("isOfficial")),
            "contest": contest.get("contestName") if contest.get("contestId") else "",
            "contestId": contest.get("contestId") or 0,
            "profiles": profs, "url": "%s/en/models/%s" % (MW, d.get("id"))}


def _tree_from_next(node):
    out = []
    for ch in node.get("children") or []:
        out.append({"id": ch.get("id"), "name": ch.get("name") or "", "pic": https(ch.get("picUrl")),
                    "children": [{"id": s.get("id"), "name": s.get("name") or ""} for s in ch.get("children") or []]})
    return out


def snapshot_config():
    return {"tree": [{"id": t[0], "name": t[1], "pic": "", "children": [{"id": s[0], "name": s[1]} for s in t[2]]} for t in SNAP_TREE],
            "sorts": SNAP_SORTS, "searchSorts": SNAP_SEARCH_SORTS, "modelTags": SNAP_MODEL_TAGS,
            "licenses": SNAP_LICENSES, "live": False}


def parse_site_config(page):
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', page, re.S)
    if not m:
        return None
    pp = json.loads(m.group(1))["props"]["pageProps"]
    cfg = snapshot_config()
    tree = _tree_from_next(pp.get("categoryTree") or {})
    if tree:
        cfg["tree"] = tree
    sc = pp.get("searchConfig") or {}
    ob = sc.get("orderBy") or {}
    if ob.get("design"):
        cfg["sorts"] = [[o["name"], SORT_LABELS.get(o["name"], o.get("displayName") or o["name"])] for o in ob["design"] if o.get("name")]
    if ob.get("designSearch"):
        cfg["searchSorts"] = [[o["name"], SORT_LABELS.get(o["name"], o.get("displayName") or o["name"])] for o in ob["designSearch"] if o.get("name")]
    for f in (sc.get("filter") or {}).get("design") or []:
        if f.get("name") == "licenses" and f.get("option"):
            cfg["licenses"] = [[o["value"], o.get("displayName") or o["value"]] for o in f["option"] if o.get("value")]
        if f.get("name") == "model_tag" and f.get("option"):
            cfg["modelTags"] = [[o["value"], MODEL_TAG_LABELS.get(o["value"], o.get("displayName") or o["value"])] for o in f["option"] if o.get("value")]
    cfg["live"] = True
    return cfg


class NeedSignIn(Exception):
    pass


PAGE = r"""
<style>
  body { margin: 0; padding: 0; font-size: 13px; font-family: var(--orca-font, sans-serif); }
  a { color: var(--orca-accent); }
  .top { position: sticky; top: 0; z-index: 5; background: var(--orca-bg); padding: 10px 12px 0; border-bottom: 1px solid var(--orca-border); }
  .bar { display: flex; gap: 6px; flex-wrap: wrap; align-items: center; margin-bottom: 8px; }
  input[type=text], input[type=password] { padding: 5px; }
  select { padding: 4px; max-width: 220px; }
  #q { flex: 1; min-width: 200px; }
  #acct { margin-left: auto; color: var(--orca-muted); }
  #acct a, .link { color: var(--orca-accent); cursor: pointer; text-decoration: underline; }
  .nav { display: flex; gap: 2px; flex-wrap: wrap; }
  .nav button { background: transparent; color: var(--orca-fg); border: none; border-bottom: 2px solid transparent; border-radius: 0; padding: 6px 10px; }
  .nav button.on { border-bottom-color: var(--orca-accent); color: var(--orca-accent); font-weight: 600; }
  .nav .personal { display: none; }
  body.signedin .nav .personal { display: inline-block; }
  .nav .sep { width: 1px; background: var(--orca-border); margin: 6px 6px; }
  main { padding: 10px 12px 20px; }
  #head { display: flex; gap: 10px; align-items: center; margin-bottom: 8px; color: var(--orca-muted); min-height: 1.4em; flex-wrap: wrap; }
  #head b { color: var(--orca-fg); }
  #tools { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 10px; }
  #tools:empty { display: none; }
  #tools label { color: var(--orca-muted); font-size: 12px; display: inline-flex; gap: 4px; align-items: center; }
  .chips { display: flex; gap: 6px; flex-wrap: wrap; }
  .chip { background: transparent; color: var(--orca-fg); border: 1px solid var(--orca-border); border-radius: 14px; padding: 3px 10px; cursor: pointer; font-size: 12px; }
  .chip.on { border-color: var(--orca-accent); color: var(--orca-accent); font-weight: 600; }
  .chip:hover { border-color: var(--orca-accent); }
  #intro { margin-bottom: 10px; color: var(--orca-muted); line-height: 1.45; }
  #intro:empty { display: none; }
  #intro .who { display: flex; gap: 12px; align-items: center; }
  #intro .who img { width: 56px; height: 56px; border-radius: 50%; object-fit: cover; background: #8882; }
  #intro .who b { color: var(--orca-fg); font-size: 15px; }
  #grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(190px, 1fr)); gap: 10px; }
  .card { border: 1px solid var(--orca-border); border-radius: 6px; overflow: hidden; display: flex; flex-direction: column; position: relative; }
  .card img.cv { width: 100%; aspect-ratio: 4/3; object-fit: cover; background: #8882; cursor: pointer; display: block; }
  .card .t { padding: 6px 6px 2px; font-weight: 600; flex: 1; cursor: pointer; }
  .card .t:hover { color: var(--orca-accent); }
  .card .m { padding: 0 6px 6px; color: var(--orca-muted); font-size: 11px; }
  .card .m .by { cursor: pointer; }
  .card .m .by:hover { color: var(--orca-accent); text-decoration: underline; }
  .card button.open { margin: 0 6px 6px; }
  .badges { position: absolute; top: 6px; left: 6px; display: flex; gap: 4px; flex-wrap: wrap; pointer-events: none; }
  .badge { font-size: 10px; padding: 1px 6px; border-radius: 8px; background: #000a; color: #fff; }
  .badge.acc { background: var(--orca-accent); color: #fff; }
  .badge.inline { position: static; display: inline-block; margin-right: 4px; background: #8884; color: var(--orca-fg); }
  .badge.inline.acc { background: var(--orca-accent); color: #fff; }
  .coll, .contest, .catcard { cursor: pointer; }
  .coll .covers { display: grid; grid-template-columns: 1fr 1fr; aspect-ratio: 4/3; background: #8882; }
  .coll .covers img { width: 100%; height: 100%; object-fit: cover; }
  .contest img.cv { aspect-ratio: 16/7; }
  .catcard { padding: 10px; display: flex; flex-direction: column; gap: 8px; }
  .catcard .ch { display: flex; gap: 8px; align-items: center; font-weight: 600; font-size: 14px; }
  .catcard .ch img { width: 26px; height: 26px; }
  .catcard .ch:hover { color: var(--orca-accent); }
  .quiet { background: transparent; color: var(--orca-fg); border-color: var(--orca-border); }
  .empty { color: var(--orca-muted); padding: 30px; text-align: center; grid-column: 1 / -1; }
  #sentinel { height: 1px; }
  #more { text-align: center; color: var(--orca-muted); padding: 14px; min-height: 18px; }
  .spin { display: inline-block; width: 12px; height: 12px; border: 2px solid var(--orca-border); border-top-color: var(--orca-accent); border-radius: 50%; animation: rot .8s linear infinite; vertical-align: -2px; margin-right: 6px; }
  @keyframes rot { to { transform: rotate(360deg); } }
  /* details panel */
  #detail { position: fixed; inset: 0; z-index: 10; background: var(--orca-bg); color: var(--orca-fg); overflow: auto; display: none; }
  #detail.show { display: block; }
  #detail .dtop { position: sticky; top: 0; z-index: 2; background: var(--orca-bg); border-bottom: 1px solid var(--orca-border); padding: 8px 12px; display: flex; gap: 10px; align-items: center; }
  #detail .dtop .dt { font-weight: 600; font-size: 14px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  #detail .dbody { padding: 12px; max-width: 1200px; margin: 0 auto; }
  .dgrid { display: grid; grid-template-columns: minmax(0, 3fr) minmax(260px, 2fr); gap: 18px; }
  @media (max-width: 760px) { .dgrid { grid-template-columns: 1fr; } }
  .gal .main { position: relative; background: #8882; border-radius: 6px; overflow: hidden; aspect-ratio: 4/3; display: flex; align-items: center; justify-content: center; }
  .gal .main img { max-width: 100%; max-height: 100%; object-fit: contain; }
  .gal .nb { position: absolute; top: 50%; transform: translateY(-50%); background: #0007; color: #fff; border: none; border-radius: 50%; width: 32px; height: 32px; cursor: pointer; font-size: 16px; }
  .gal .nb.l { left: 8px; } .gal .nb.r { right: 8px; }
  .gal .cnt { position: absolute; bottom: 6px; right: 8px; background: #0007; color: #fff; font-size: 11px; padding: 1px 6px; border-radius: 8px; }
  .thumbs { display: flex; gap: 6px; overflow-x: auto; padding: 6px 0; }
  .thumbs img { width: 64px; height: 48px; object-fit: cover; border-radius: 4px; cursor: pointer; opacity: .65; border: 2px solid transparent; flex: none; }
  .thumbs img.on { opacity: 1; border-color: var(--orca-accent); }
  .info h2 { margin: 0 0 8px; font-size: 18px; line-height: 1.25; }
  .info .row { margin: 6px 0; line-height: 1.45; }
  .info .lbl { color: var(--orca-muted); }
  .designer { display: flex; gap: 10px; align-items: center; cursor: pointer; margin: 8px 0; }
  .designer img { width: 36px; height: 36px; border-radius: 50%; object-fit: cover; background: #8882; }
  .designer:hover b { color: var(--orca-accent); text-decoration: underline; }
  .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; margin: 10px 0; }
  .stats div { border: 1px solid var(--orca-border); border-radius: 6px; padding: 6px; text-align: center; }
  .stats b { display: block; font-size: 15px; }
  .stats span { color: var(--orca-muted); font-size: 11px; }
  h3.sec { font-size: 15px; margin: 22px 0 8px; border-bottom: 1px solid var(--orca-border); padding-bottom: 4px; }
  .pcard { border: 1px solid var(--orca-border); border-radius: 6px; padding: 10px; margin: 8px 0; display: flex; gap: 12px; }
  .pcard > img { width: 150px; height: 112px; object-fit: cover; border-radius: 4px; background: #8882; flex: none; }
  @media (max-width: 600px) { .pcard { flex-direction: column; } .pcard > img { width: 100%; height: auto; aspect-ratio: 4/3; } }
  .pcard .pb { flex: 1; min-width: 0; }
  .pcard .pt { font-weight: 600; font-size: 14px; margin-bottom: 4px; }
  .pcard .facts { display: flex; flex-wrap: wrap; gap: 4px 14px; margin: 4px 0; }
  .pcard .facts span b { font-weight: 600; }
  .fil { display: inline-flex; align-items: center; gap: 4px; margin: 2px 10px 2px 0; font-size: 12px; }
  .fil i { width: 12px; height: 12px; border-radius: 50%; border: 1px solid var(--orca-border); display: inline-block; }
  .plates { display: flex; gap: 6px; overflow-x: auto; margin-top: 6px; }
  .plates div { flex: none; width: 74px; font-size: 10px; color: var(--orca-muted); text-align: center; }
  .plates img { width: 74px; height: 74px; object-fit: contain; background: #8881; border-radius: 4px; display: block; }
  .pcard .pa { display: flex; flex-direction: column; gap: 6px; align-items: flex-end; justify-content: flex-start; }
  .desc { line-height: 1.5; overflow-wrap: anywhere; }
  .desc img { max-width: 100%; height: auto; border-radius: 4px; }
  .desc table { border-collapse: collapse; } .desc td, .desc th { border: 1px solid var(--orca-border); padding: 3px 6px; }
  .tagrow .chip { margin: 2px 0; }
  /* overlay */
  #ov { position: fixed; inset: 0; background: #0008; z-index: 20; display: none; align-items: center; justify-content: center; }
  #ov.show { display: flex; }
  .box { background: var(--orca-bg); color: var(--orca-fg); border: 1px solid var(--orca-border); border-radius: 8px; padding: 18px; width: min(560px, 92vw); max-height: 86vh; overflow: auto; box-shadow: 0 8px 30px #0006; }
  .box h3 { margin: 0 0 10px; font-size: 15px; }
  .box .msg { margin: 8px 0; line-height: 1.4; }
  .box .err { color: #d9534f; }
  .box .ok { color: var(--orca-accent); }
  .box .btns { display: flex; gap: 8px; justify-content: flex-end; margin-top: 14px; }
  .prog { height: 10px; border-radius: 5px; background: #8883; overflow: hidden; margin: 10px 0 4px; }
  .prog div { height: 100%; width: 0; background: var(--orca-accent); transition: width .15s; }
  .prog.busy div { width: 30%; animation: slide 1.1s linear infinite; }
  @keyframes slide { from { margin-left: -30%; } to { margin-left: 100%; } }
  .small { font-size: 11px; color: var(--orca-muted); }
  .prof { display: flex; gap: 10px; border: 1px solid var(--orca-border); border-radius: 6px; padding: 8px; margin: 6px 0; cursor: pointer; align-items: flex-start; }
  .prof.sel { border-color: var(--orca-accent); box-shadow: 0 0 0 1px var(--orca-accent) inset; }
  .prof img { width: 84px; height: 63px; object-fit: cover; border-radius: 4px; background: #8882; flex: none; }
  .prof .pt { font-weight: 600; margin-bottom: 3px; }
  .lrow { display: flex; flex-direction: column; gap: 8px; }
  .lrow input { width: 100%; box-sizing: border-box; }
  /* toast */
  #toast { position: fixed; right: 16px; bottom: 16px; z-index: 30; max-width: 380px; padding: 10px 14px; border-radius: 6px; overflow-wrap: anywhere;
           background: var(--orca-bg); border: 1px solid var(--orca-border); box-shadow: 0 4px 16px #0005; display: none; }
  #toast.err { border-color: #d9534f; }
</style>
<div class="top">
  <div class="bar">
    <input type="text" id="q" placeholder="Search MakerWorld, or paste a MakerWorld model link">
    <button id="go">Search</button>
    <span id="acct">Checking sign-in...</span>
  </div>
  <div class="nav" id="nav">
    <button data-v="home" class="on">Home</button>
    <button data-v="following" class="personal">Following</button>
    <button data-v="models">Models</button>
    <button data-v="categories">Categories</button>
    <button data-v="contests">Contests</button>
    <button data-v="collections">Collections</button>
    <button data-v="makerlab">MakerLab</button>
    <button data-v="search" id="navsearch" style="display:none">Search results</button>
    <span class="sep personal"></span>
    <button data-v="mycollections" class="personal">My collections</button>
    <button data-v="liked" class="personal">Liked</button>
    <button data-v="downloads" class="personal">Downloaded</button>
  </div>
</div>
<main>
  <div id="head"></div>
  <div id="intro"></div>
  <div id="tools"></div>
  <div id="grid"></div>
  <div id="sentinel"></div>
  <div id="more"></div>
</main>
<div id="detail"><div class="dtop"><button class="quiet" id="dback">&larr; Back</button><span class="dt" id="dtitle"></span></div><div class="dbody" id="dbody"></div></div>
<div id="ov"><div class="box" id="box"></div></div>
<div id="toast"></div>
<script>
(function () {
  var PERSONAL = ["following", "mycollections", "liked", "downloads"];
  var view = null, stack = [], seq = 0, signedIn = false, busy = false;
  var st = {next: 0, hasMore: false, loading: false, count: 0, ids: {}, failed: false};
  var cfg = null, cfgWaiters = [], navs = null, navWaiters = [];
  var TABS = __TABS__;
  var dlDir = "";
  function $(id) { return document.getElementById(id); }
  function esc(s) { return (s == null ? "" : String(s)).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;"); }
  function url(u) { return (typeof u === "string" && /^https:\/\//.test(u)) ? esc(u) : ""; }
  function num(n) { n = Number(n) || 0; if (n >= 1e6) return (n / 1e6).toFixed(n >= 1e7 ? 0 : 1) + "M"; if (n >= 1e3) return (n / 1e3).toFixed(n >= 1e4 ? 0 : 1) + "k"; return String(n); }
  function fmtBytes(n) { if (!n) return "0 MB"; return (n / 1048576).toFixed(n < 10485760 ? 1 : 0) + " MB"; }
  function fmtMin(m) { if (!m) return ""; var h = Math.floor(m / 60); return (h ? h + " h " : "") + (m % 60) + " min"; }
  function el(tag, cls, html) { var e = document.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; }
  var toastTimer = null;
  function toast(t, isErr) {
    var e = $("toast"); e.textContent = t; e.className = isErr ? "err" : ""; e.style.display = "block";
    clearTimeout(toastTimer); toastTimer = setTimeout(function () { e.style.display = "none"; }, isErr ? 8000 : 4500);
  }
  // ---------- overlay ----------
  function overlay(html) { $("box").innerHTML = html; $("ov").className = "show"; }
  function closeOverlay() { $("ov").className = ""; busy = false; }
  function btns(list) {
    return '<div class="btns">' + list.map(function (b, i) {
      return '<button data-b="' + i + '"' + (b.quiet ? ' class="quiet"' : '') + '>' + esc(b.label) + '</button>'; }).join("") + '</div>';
  }
  function wire(list) {
    list.forEach(function (b, i) { var e = $("box").querySelector('[data-b="' + i + '"]'); if (e) e.addEventListener("click", b.fn); });
  }
  function step(title, text, opts) {
    opts = opts || {};
    var list = opts.buttons || [];
    overlay('<h3>' + esc(title) + '</h3><div class="msg' + (opts.cls ? ' ' + opts.cls : '') + '">' + esc(text) + '</div>' +
      (opts.progress ? '<div class="prog' + (opts.progress === "busy" ? ' busy' : '') + '" id="pb"><div id="pbf"></div></div><div class="small" id="pbt"></div>' : '') +
      (opts.small ? '<div class="small">' + esc(opts.small) + '</div>' : '') + btns(list));
    wire(list);
  }
  var closeBtn = {label: "Close", fn: function () { closeOverlay(); }};
  var cancelBtn = {label: "Cancel download", quiet: true, fn: function () { orca.postMessage({cmd: "cancel"}); }};
  // ---------- sign in ----------
  function showLogin(note) {
    var list = [{label: "Cancel", quiet: true, fn: closeOverlay}, {label: "Sign in", fn: doLogin}];
    overlay('<h3>Sign in to MakerWorld</h3>' +
      '<div class="small">Uses your Bambu Lab account. If you sign in with Google, leave the password empty: Bambu will email you a code.</div>' +
      '<div class="lrow" style="margin-top:10px"><input type="text" id="email" placeholder="Email">' +
      '<input type="password" id="pw" placeholder="Password (leave empty for Google accounts)">' +
      '<input type="text" id="code" placeholder="Code from the email (when asked)"></div>' +
      '<div class="msg" id="lmsg">' + esc(note || "") + '</div>' + btns(list));
    wire(list);
    var saved = localStorage.getItem("mw_email"); if (saved) $("email").value = saved;
    ["email", "pw", "code"].forEach(function (id) { $(id).addEventListener("keydown", function (e) { if (e.key === "Enter") doLogin(); }); });
  }
  function doLogin() {
    var email = $("email").value.trim(); if (!email) { $("lmsg").textContent = "Type your email first."; return; }
    localStorage.setItem("mw_email", email);
    $("lmsg").className = "msg"; $("lmsg").textContent = "Working...";
    orca.postMessage({cmd: "login", email: email, password: $("pw").value, code: $("code").value.trim()});
    $("pw").value = "";
  }
  function setAccount(m) {
    signedIn = !!m.signedIn; document.body.className = signedIn ? "signedin" : "";
    if (m.dlDir) dlDir = m.dlDir;
    var a = $("acct");
    if (signedIn) { a.innerHTML = 'Signed in as <b>' + esc(m.name || "you") + '</b> &middot; <a id="signout">Sign out</a>';
      $("signout").addEventListener("click", function () { orca.postMessage({cmd: "signout"}); }); }
    else { a.innerHTML = (m.expired ? 'Sign-in expired &middot; ' : 'Not signed in &middot; ') + '<a id="signin">Sign in</a>';
      $("signin").addEventListener("click", function () { showLogin(); });
      if (view && (PERSONAL.indexOf(view.nav) >= 0 || (view.src && view.src.kind === "collection" && view.mine))) go({name: "home"}); }
  }
  // ---------- config (menus) ----------
  function needConfig(fn) { if (cfg) fn(cfg); else { cfgWaiters.push(fn); if (cfgWaiters.length === 1) orca.postMessage({cmd: "config"}); } }
  function needNavs(fn) { if (navs) fn(navs); else { navWaiters.push(fn); if (navWaiters.length === 1) orca.postMessage({cmd: "navs"}); } }
  function opts(list, sel) { return list.map(function (o) { return '<option value="' + esc(o[0]) + '"' + (String(o[0]) === String(sel) ? ' selected' : '') + '>' + esc(o[1]) + '</option>'; }).join(""); }
  function catName(id) {
    if (!cfg || !id) return "";
    for (var i = 0; i < cfg.tree.length; i++) { var t = cfg.tree[i]; if (t.id == id) return t.name;
      for (var j = 0; j < t.children.length; j++) if (t.children[j].id == id) return t.name + " › " + t.children[j].name; }
    return "Category " + id;
  }
  // ---------- views ----------
  // A view: {name, nav, title, src, item, back, ...}; build() fills in the header/tools and the list source.
  function build(v) {
    var n = v.name;
    v.nav = n; v.item = "design";
    if (n === "home") { v.key = v.key || "Trending"; v.src = {kind: "nav", key: v.key}; v.title = "MakerWorld home"; }
    else if (n === "following") { v.src = {kind: "following"}; v.title = "New from designers you follow"; }
    else if (n === "models" || n === "search") {
      v.f = v.f || {}; v.nav = v.q ? "search" : "models";
      if (v.f.orderBy == null) v.f.orderBy = v.q ? "score" : "hotScore";
      v.src = {kind: "models", q: v.q || "", orderBy: v.f.orderBy, since: v.f.since || 0, cat: v.f.cat || 0, tag: v.f.tag || "", license: v.f.license || ""};
      v.title = v.q ? 'Results for <b>' + esc(v.q) + '</b>' : (v.f.cat ? '<b>' + esc(catName(v.f.cat) || "Category") + '</b>' : "All models");
      if (v.f.cat) v.nav = "categories";
    }
    else if (n === "categories") { v.src = null; v.title = "Categories"; }
    else if (n === "contests") { v.src = {kind: "contests"}; v.item = "contest"; v.title = "Contests"; }
    else if (n === "contest") { v.nav = "contests"; v.sub = v.sub || "entries"; v.orderBy = v.orderBy || "hotScore";
      v.src = v.sub === "winners" ? {kind: "awarded", contestId: v.c.id} : {kind: "models", contestId: v.c.id, orderBy: v.orderBy};
      v.title = 'Contest <b>' + esc(v.c.title) + '</b>'; }
    else if (n === "collections") { v.key = v.key || TABS[0][0]; v.src = {kind: "curated", key: v.key}; v.item = "collection"; v.title = "Collections picked by MakerWorld"; }
    else if (n === "collection") { v.nav = v.mine ? "mycollections" : "collections"; v.src = {kind: "collection", id: v.id}; v.title = 'Collection <b>' + esc(v.ctitle) + '</b>'; }
    else if (n === "makerlab") { v.src = {kind: "makerlab"}; v.title = "Models made with MakerLab tools"; }
    else if (n === "designer") { v.nav = ""; v.orderBy = v.orderBy || ""; v.src = {kind: "designer", uid: v.uid, orderBy: v.orderBy}; v.title = 'Models by <b>' + esc(v.dname || "designer") + '</b>'; }
    else if (n === "mycollections") { v.src = {kind: "mycollections"}; v.item = "collection"; v.title = "Your collections"; }
    else if (n === "liked") { v.src = {kind: "liked"}; v.title = "Models you liked"; }
    else if (n === "downloads") { v.src = {kind: "downloads"}; v.title = "Models you have downloaded on MakerWorld"; }
    return v;
  }
  function setNav(k) {
    Array.prototype.forEach.call($("nav").children, function (b) { if (b.dataset.v) b.classList.toggle("on", b.dataset.v === k); });
  }
  function go(v, push) {
    if (push && view) stack.push(view); else if (!push) stack = [];
    show(build(v));
  }
  function back() { if (stack.length) show(build(stack.pop())); else go({name: "home"}); }
  function show(v) {
    view = v; seq++;
    st = {next: 0, hasMore: !!v.src, loading: false, count: 0, ids: {}, failed: false};
    setNav(v.nav);
    $("grid").innerHTML = ""; $("intro").innerHTML = ""; $("tools").innerHTML = ""; $("more").innerHTML = "";
    var b = stack.length ? '<button class="quiet" id="back">&larr; Back</button>' : '';
    $("head").innerHTML = b + '<span id="hl">' + v.title + '</span><span id="hc"></span>';
    if ($("back")) $("back").addEventListener("click", back);
    tools(v);
    window.scrollTo(0, 0);
    if (v.name === "categories") { $("grid").innerHTML = '<div class="empty"><span class="spin"></span>Loading categories...</div>'; needConfig(function () { if (view === v) renderCategories(); }); return; }
    if (v.name === "models" && v.f.cat && !cfg) needConfig(function () { if (view === v) { v.title = '<b>' + esc(catName(v.f.cat)) + '</b>'; $("hl").innerHTML = v.title; } });
    loadMore();
  }
  function chipRow(list, sel, fn) {
    var row = el("div", "chips");
    list.forEach(function (o) { var c = el("button", "chip" + (String(o[0]) === String(sel) ? " on" : ""), esc(o[1])); c.addEventListener("click", function () { fn(o[0]); }); row.appendChild(c); });
    return row;
  }
  function selectBox(label, list, sel, fn) {
    var l = el("label", "", esc(label) + ' <select>' + opts(list, sel) + '</select>');
    l.querySelector("select").addEventListener("change", function (e) { fn(e.target.value); });
    return l;
  }
  function tools(v) {
    var t = $("tools");
    if (v.name === "home") {
      needNavs(function (list) { if (view !== v) return; t.innerHTML = ""; t.appendChild(chipRow(list, v.key, function (k) { go({name: "home", key: k}); })); });
    } else if (v.name === "collections") {
      t.appendChild(chipRow(TABS, v.key, function (k) { go({name: "collections", key: k}); }));
    } else if (v.name === "models" || v.name === "search") {
      if (!v.q && !v.f.cat) t.appendChild(chipRow([["hotScore|", "Trending"], ["hotScore|featured", "Staff picks"], ["newUploads|", "Newest"], ["downloadCount|", "Most downloaded"], ["likeCount|", "Most liked"], ["hotScore|exclusive", "Exclusive"]],
          v.f.orderBy + "|" + (v.f.tag || ""), function (k) { var p = k.split("|"); go({name: "models", f: {orderBy: p[0], tag: p[1], since: 0}}); }));
      needConfig(function (c) {
        if (view !== v) return;
        var f = v.f, row = el("div", "chips");
        function re(ch) { var nf = {}; for (var k in f) nf[k] = f[k]; for (var k2 in ch) nf[k2] = ch[k2]; var nv = {name: v.name, q: v.q, f: nf}; view = null; stack.length ? show(build(nv)) : go(nv); }
        row.appendChild(selectBox("Sort", v.q ? c.searchSorts : c.sorts, f.orderBy, function (x) { re({orderBy: x}); }));
        row.appendChild(selectBox("Uploaded", [[0, "Any time"], [1, "Last 24 hours"], [7, "Last 7 days"], [30, "Last 30 days"]], f.since || 0, function (x) { re({since: +x}); }));
        var cats = [[0, "All categories"]]; c.tree.forEach(function (tt) { cats.push([tt.id, tt.name]); tt.children.forEach(function (s) { cats.push([s.id, "   " + s.name]); }); });
        row.appendChild(selectBox("Category", cats, f.cat || 0, function (x) { re({cat: +x}); }));
        row.appendChild(selectBox("Show", [["", "All models"]].concat(c.modelTags), f.tag || "", function (x) { re({tag: x}); }));
        row.appendChild(selectBox("License", [["", "Any license"]].concat(c.licenses), f.license || "", function (x) { re({license: x}); }));
        t.appendChild(row);
        if (f.cat) { v.title = '<b>' + esc(catName(f.cat)) + '</b>'; if ($("hl") && !v.q) $("hl").innerHTML = v.title; }
      });
    } else if (v.name === "contest") {
      var c = v.c;
      $("intro").innerHTML = '<div><span class="badge inline' + (c.status === 2 ? ' acc' : '') + '">' + esc(c.statusText || "Contest") + '</span> ' +
        esc(c.start) + ' to ' + esc(c.end) + ' &middot; ' + num(c.entrants) + ' entrants</div>' +
        (c.theme ? '<div style="margin-top:4px">' + esc(c.theme) + '</div>' : '') +
        (c.prizes && c.prizes.length ? '<div class="small" style="margin-top:4px">Prizes: ' + esc(c.prizes.join(" · ")) + '</div>' : '');
      var subs = [["entries", "Entries"]]; if (c.status === 3 || c.winners) subs.push(["winners", "Winners"]);
      t.appendChild(chipRow(subs, v.sub, function (k) { var nv = {name: "contest", c: c, sub: k, orderBy: v.orderBy}; show(build(nv)); }));
      if (v.sub === "entries") t.appendChild(selectBox("Sort", [["hotScore", "Trending"], ["newUploads", "Newest"], ["likeCount", "Most liked"], ["downloadCount", "Most downloaded"]], v.orderBy,
        function (x) { show(build({name: "contest", c: c, sub: "entries", orderBy: x})); }));
    } else if (v.name === "designer") {
      t.appendChild(selectBox("Sort", [["", "Newest"], ["likeCount", "Most liked"], ["downloadCount", "Most downloaded"]], v.orderBy,
        function (x) { show(build({name: "designer", uid: v.uid, dname: v.dname, orderBy: x})); }));
    }
  }
  // ---------- infinite scroll ----------
  function loadMore() {
    if (!view || !view.src || st.loading || !st.hasMore) return;
    st.loading = true; st.failed = false;
    $("more").innerHTML = st.count ? '<span class="spin"></span>Loading more...' : '';
    if (!st.count) $("grid").innerHTML = '<div class="empty"><span class="spin"></span>Loading...</div>';
    orca.postMessage({cmd: "list", src: view.src, cursor: st.next, seq: seq});
  }
  function nearBottom() { var r = $("sentinel").getBoundingClientRect(); return r.top < window.innerHeight + 700; }
  function maybeMore() { if (st.hasMore && !st.loading && !st.failed && nearBottom()) loadMore(); }
  if ("IntersectionObserver" in window) {
    new IntersectionObserver(function (es) { if (es.some(function (e) { return e.isIntersecting; })) maybeMore(); }, {rootMargin: "0px 0px 700px 0px"}).observe($("sentinel"));
  }
  window.addEventListener("scroll", function () { maybeMore(); }, {passive: true});
  function renderResults(m) {
    if (m.seq !== seq) return;
    var g = $("grid"), first = st.count === 0;
    st.loading = false;
    if (m.error) {
      if (first) { g.innerHTML = '<div class="empty">' + esc(m.error) + '</div>'; $("more").innerHTML = ""; st.hasMore = false; }
      else { st.failed = true; $("more").innerHTML = esc(m.error) + ' <a class="link" id="retry">Try again</a>'; $("retry").addEventListener("click", function () { loadMore(); }); }
      return;
    }
    if (first) g.innerHTML = "";
    if (m.profile && view.name === "designer") renderDesigner(m.profile);
    var added = 0;
    (m.items || []).forEach(function (h) {
      var key = view.item + ":" + h.id + (h.rank ? ":" + h.rank : "");
      if (h.id != null && st.ids[key]) return; st.ids[key] = 1;
      g.appendChild(view.item === "contest" ? contestCard(h) : view.item === "collection" ? collCard(h) : card(h)); added++;
    });
    st.count += added;
    var stuck = m.next === st.next;
    st.next = m.next; st.hasMore = !!m.hasMore && !stuck;
    if (!st.count && !st.hasMore) g.innerHTML = '<div class="empty">' + esc(m.emptyText || "Nothing here yet.") + '</div>';
    var tot = m.total && m.total < 10000 ? m.total : 0;
    $("hc").textContent = st.count ? " · " + st.count + (tot ? " of " + tot : "") : "";
    $("more").innerHTML = st.hasMore ? "" : (st.count > 8 ? "That's everything." : "");
    setTimeout(maybeMore, 50);
  }
  // ---------- cards ----------
  function badgeHtml(h) {
    var b = ""; if (h.rank) b += '<span class="badge acc">' + esc(h.rank) + '</span>';
    if (h.staff) b += '<span class="badge acc">Staff pick</span>'; if (h.excl) b += '<span class="badge">Exclusive</span>';
    return b ? '<div class="badges">' + b + '</div>' : '';
  }
  function card(h) {
    var c = el("div", "card");
    c.innerHTML = badgeHtml(h) + '<img class="cv" loading="lazy" src="' + url(h.cover) + '"><div class="t">' + esc(h.title) + '</div>' +
      '<div class="m">' + (h.creator ? 'by <span class="by">' + esc(h.creator) + '</span> &middot; ' : '') + num(h.downloads) + ' downloads &middot; ' + num(h.likes) + ' likes</div>' +
      '<button class="open">Open in Orca</button>';
    function det() { openDetail(h.id, h.title); }
    c.querySelector(".cv").addEventListener("click", det); c.querySelector(".t").addEventListener("click", det);
    var by = c.querySelector(".by"); if (by && h.uid) by.addEventListener("click", function () { go({name: "designer", uid: h.uid, dname: h.creator}, true); });
    c.querySelector(".open").addEventListener("click", function () { startOpen({design: h.id, title: h.title}); });
    return c;
  }
  function collCard(it) {
    var c = el("div", "card coll");
    var covers = (it.covers || []).slice(0, 4).map(function (u) { return '<img loading="lazy" src="' + url(u) + '">'; }).join("");
    c.innerHTML = '<div class="covers">' + covers + '</div><div class="t">' + esc(it.title) + '</div><div class="m">' + num(it.count) + ' models' +
      (it.isDefault ? ' &middot; default' : '') + (it.creator ? ' &middot; by ' + esc(it.creator) : '') + '</div>' +
      (it.desc ? '<div class="m">' + esc(it.desc) + '</div>' : '');
    c.addEventListener("click", function () { go({name: "collection", id: it.id, ctitle: it.title, mine: view.name === "mycollections"}, true); });
    return c;
  }
  function contestCard(it) {
    var c = el("div", "card contest");
    c.innerHTML = '<div class="badges"><span class="badge' + (it.status === 2 ? ' acc' : '') + '">' + esc(it.statusText || "Contest") + '</span></div>' +
      '<img class="cv" loading="lazy" src="' + url(it.cover) + '"><div class="t">' + esc(it.title) + '</div>' +
      '<div class="m">' + esc(it.status === 2 ? "Closes " + it.end : it.start + " to " + it.end) + ' &middot; ' + num(it.entrants) + ' entrants</div>';
    c.addEventListener("click", function () { go({name: "contest", c: it}, true); });
    return c;
  }
  function renderCategories() {
    var g = $("grid"); g.innerHTML = "";
    cfg.tree.forEach(function (t) {
      var c = el("div", "card catcard");
      c.innerHTML = '<div class="ch">' + (t.pic ? '<img src="' + url(t.pic) + '">' : '') + esc(t.name) + '</div>';
      c.querySelector(".ch").addEventListener("click", function () { go({name: "models", f: {cat: t.id}}, true); });
      var row = el("div", "chips");
      t.children.forEach(function (s) { var ch = el("button", "chip", esc(s.name)); ch.addEventListener("click", function () { go({name: "models", f: {cat: s.id}}, true); }); row.appendChild(ch); });
      c.appendChild(row); g.appendChild(c);
    });
    $("hc").textContent = " · " + cfg.tree.length + " groups";
  }
  function renderDesigner(p) {
    $("intro").innerHTML = '<div class="who">' + (p.avatar ? '<img src="' + url(p.avatar) + '">' : '') + '<div><b>' + esc(p.name) + '</b>' +
      (p.handle ? ' <span class="small">@' + esc(p.handle) + '</span>' : '') +
      '<div class="small">' + num(p.fans) + ' followers &middot; ' + num(p.likes) + ' likes &middot; ' + num(p.downloads) + ' downloads</div>' +
      (p.bio ? '<div style="margin-top:4px">' + esc(p.bio) + '</div>' : '') + '</div></div>';
    if (p.name && !view.dname) { view.dname = p.name; $("hl").innerHTML = 'Models by <b>' + esc(p.name) + '</b>'; }
  }
  // ---------- details ----------
  var detailFor = 0;
  function openDetail(id, title) {
    detailFor = id;
    $("dtitle").textContent = title || "";
    $("dbody").innerHTML = '<div class="empty"><span class="spin"></span>Loading model details...</div>';
    $("detail").className = "show"; $("detail").scrollTop = 0;
    orca.postMessage({cmd: "detail", design: id});
  }
  function closeDetail() { $("detail").className = ""; detailFor = 0; }
  function renderDetail(m) {
    if (m.design !== detailFor) return;
    var b = $("dbody");
    if (m.error) { b.innerHTML = '<div class="empty">' + esc(m.error) + '</div>'; return; }
    var d = m.detail, s = d.stats;
    $("dtitle").textContent = d.title;
    var pics = d.pictures.length ? d.pictures : [""];
    var badges = (d.staff ? '<span class="badge inline acc">Staff pick</span>' : '') + (d.excl ? '<span class="badge inline">Exclusive</span>' : '') +
      (d.official ? '<span class="badge inline">Official</span>' : '') + (d.contest ? '<span class="badge inline">Contest: ' + esc(d.contest) + '</span>' : '');
    var h = '<div class="dgrid"><div class="gal"><div class="main"><img id="gimg" src="' + url(pics[0]) + '">' +
      (pics.length > 1 ? '<button class="nb l" id="gprev">&#8249;</button><button class="nb r" id="gnext">&#8250;</button><span class="cnt" id="gcnt">1 / ' + pics.length + '</span>' : '') + '</div>' +
      (pics.length > 1 ? '<div class="thumbs" id="gth">' + pics.map(function (u, i) { return '<img loading="lazy" data-i="' + i + '" src="' + url(u) + '"' + (i ? '' : ' class="on"') + '>'; }).join("") + '</div>' : '') + '</div>' +
      '<div class="info"><h2>' + esc(d.title) + '</h2>' + (badges ? '<div class="row">' + badges + '</div>' : '') +
      '<div class="designer" id="ddes">' + (d.creator.avatar ? '<img src="' + url(d.creator.avatar) + '">' : '') + '<div><b>' + esc(d.creator.name) + '</b>' +
      '<div class="small">' + num(d.creator.fans) + ' followers &middot; see all their models</div></div></div>' +
      '<div class="stats"><div><b>' + num(s.likes) + '</b><span>likes</span></div><div><b>' + num(s.collects) + '</b><span>collected</span></div>' +
      '<div><b>' + num(s.downloads) + '</b><span>downloads</span></div><div><b>' + num(s.prints) + '</b><span>prints</span></div>' +
      '<div><b>' + (s.rating ? s.rating + '★' : '-') + '</b><span>' + (s.ratingCount ? num(s.ratingCount) + ' ratings' : 'no ratings') + '</span></div>' +
      '<div><b>' + num(s.comments) + '</b><span>comments</span></div></div>' +
      '<div class="row"><span class="lbl">Published:</span> ' + esc(d.created) + (d.updated && d.updated !== d.created ? ' &middot; <span class="lbl">updated</span> ' + esc(d.updated) : '') + '</div>' +
      (d.license ? '<div class="row"><span class="lbl">License:</span> ' + esc(d.license) + '</div>' : '') +
      (d.categories.length ? '<div class="row"><span class="lbl">Category:</span> ' + d.categories.map(function (c) { return '<a class="link dcat" data-id="' + esc(c.id) + '">' + esc(c.name) + '</a>'; }).join(", ") + '</div>' : '') +
      (d.pickReason ? '<div class="row"><span class="lbl">Why staff picked it:</span> ' + esc(d.pickReason) + '</div>' : '') +
      '<div class="row"><button id="dopen">Open in Orca</button> <span class="small">' + d.profiles.length + ' print profile' + (d.profiles.length === 1 ? '' : 's') + ' below</span></div>' +
      '</div></div>';
    if (d.tags.length) h += '<h3 class="sec">Tags</h3><div class="chips tagrow">' + d.tags.map(function (t) { return '<button class="chip dtag">' + esc(t) + '</button>'; }).join("") + '</div>';
    h += '<h3 class="sec">Print profiles (' + d.profiles.length + ')</h3>';
    if (!d.profiles.length) h += '<div class="empty">This model has no print profiles to download.</div>';
    d.profiles.forEach(function (p, i) {
      var facts = [];
      if (p.printer) facts.push('<span><b>' + esc(p.printer) + '</b>' + (p.nozzle ? ' ' + esc(p.nozzle) + ' mm nozzle' : '') + '</span>');
      facts.push('<span>' + p.plates + (p.plates === 1 ? ' plate' : ' plates') + '</span>');
      if (p.minutes) facts.push('<span>' + esc(fmtMin(p.minutes)) + '</span>');
      if (p.grams) facts.push('<span>' + esc(p.grams) + ' g filament</span>');
      if (p.colors) facts.push('<span>' + p.colors + (p.colors === 1 ? ' colour' : ' colours') + (p.ams ? ' (needs AMS)' : '') + '</span>');
      if (p.rating) facts.push('<span>' + esc(p.rating) + '★ (' + num(p.ratingCount) + ')</span>');
      facts.push('<span>' + num(p.downloads) + ' downloads &middot; ' + num(p.prints) + ' prints</span>');
      var fil = p.filaments.map(function (f) { return '<span class="fil"><i style="background:' + esc(f.color || "transparent") + '"></i>' + esc(f.type) + (f.grams ? ' ' + esc(f.grams) + ' g' : '') + '</span>'; }).join("");
      var plates = p.plateList.length > 1 || (p.plateList[0] && p.plateList[0].thumb) ? '<div class="plates">' + p.plateList.map(function (pl) {
        return '<div>' + (pl.thumb ? '<img loading="lazy" src="' + url(pl.thumb) + '">' : '') + esc(pl.name) + (pl.minutes ? '<br>' + esc(fmtMin(pl.minutes)) : '') + (pl.grams ? ' &middot; ' + esc(pl.grams) + ' g' : '') + '</div>'; }).join("") + '</div>' : '';
      h += '<div class="pcard"><img loading="lazy" src="' + url(p.cover) + '"><div class="pb"><div class="pt">' + (i === 0 ? '<span class="badge inline acc">Recommended</span>' : '') + esc(p.title) + '</div>' +
        '<div class="small">' + (p.author ? 'Profile by ' + esc(p.author) : '') + (p.published ? ' &middot; ' + esc(p.published) : '') + (p.official ? ' &middot; official' : '') + (p.size ? ' &middot; ' + fmtBytes(p.size) : '') + '</div>' +
        '<div class="facts">' + facts.join("") + '</div>' +
        (p.others.length ? '<div class="small">Also fits: ' + esc(p.others.join(", ")) + '</div>' : '') +
        (p.nozzleTypes.length ? '<div class="small">Nozzle type: ' + esc(p.nozzleTypes.join(", ")) + '</div>' : '') +
        (fil ? '<div style="margin-top:4px">' + fil + '</div>' : '') +
        (p.note && p.note !== p.title ? '<div class="small" style="margin-top:4px">' + esc(p.note) + '</div>' : '') + plates +
        '</div><div class="pa"><button class="popen" data-id="' + esc(p.id) + '">Open in Orca</button></div></div>';
    });
    h += '<h3 class="sec">Description</h3><div class="desc" id="ddesc">' + (d.summary || '<span class="small">No description.</span>') + '</div>';
    b.innerHTML = h;
    // gallery
    var gi = 0;
    function pic(i) { gi = (i + pics.length) % pics.length; $("gimg").src = pics[gi]; if ($("gcnt")) $("gcnt").textContent = (gi + 1) + " / " + pics.length;
      Array.prototype.forEach.call(b.querySelectorAll("#gth img"), function (t, j) { t.className = j === gi ? "on" : ""; }); }
    if ($("gprev")) { $("gprev").addEventListener("click", function () { pic(gi - 1); }); $("gnext").addEventListener("click", function () { pic(gi + 1); }); }
    Array.prototype.forEach.call(b.querySelectorAll("#gth img"), function (t) { t.addEventListener("click", function () { pic(+t.dataset.i); }); });
    $("ddes").addEventListener("click", function () { if (!d.creator.uid) return; closeDetail(); go({name: "designer", uid: d.creator.uid, dname: d.creator.name}, true); });
    Array.prototype.forEach.call(b.querySelectorAll(".dcat"), function (a) { a.addEventListener("click", function () { closeDetail(); go({name: "models", f: {cat: +a.dataset.id}}, true); }); });
    Array.prototype.forEach.call(b.querySelectorAll(".dtag"), function (a) { a.addEventListener("click", function () { closeDetail(); $("q").value = a.textContent; $("navsearch").style.display = "inline-block"; go({name: "search", q: a.textContent}); }); });
    $("dopen").addEventListener("click", function () {
      if (d.profiles.length > 1) showProfiles({design: d.id, title: d.title, profiles: d.profiles, selected: d.profiles[0].id});
      else if (d.profiles.length) openProfile(d, d.profiles[0].id);
    });
    Array.prototype.forEach.call(b.querySelectorAll(".popen"), function (bt) { bt.addEventListener("click", function () { openProfile(d, +bt.dataset.id); }); });
    $("ddesc").addEventListener("click", function (e) {
      var a = e.target.closest ? e.target.closest("a") : null; if (!a) return;
      e.preventDefault(); var hr = a.getAttribute("href"); if (!hr) return;
      var mm = /makerworld\.com\/[a-z-]*\/?models\/(\d+)/.exec(hr);
      if (mm) { openDetail(+mm[1], ""); return; }
      toast("Link (open it in your browser): " + hr);
    });
  }
  function openProfile(d, pid) {
    if (busy) { toast("Still working on the last model - one at a time."); return; }
    busy = true;
    var p = d.profiles.filter(function (x) { return x.id === pid; })[0];
    step("Fetching model...", "Asking MakerWorld for the file of \"" + (p ? p.title : d.title) + "\"", {progress: "busy"});
    orca.postMessage({cmd: "download", design: d.id, instance: pid, title: d.title});
  }
  $("dback").addEventListener("click", closeDetail);
  document.addEventListener("keydown", function (e) {
    if ($("ov").className === "show") return;
    if ($("detail").className === "show") {
      if (e.key === "Escape") closeDetail();
      else if ((e.key === "ArrowLeft" || e.key === "ArrowRight") && $("gprev")) $(e.key === "ArrowLeft" ? "gprev" : "gnext").click();
    }
  });
  // ---------- open flow ----------
  function startOpen(o) {
    if (busy) { toast("Still working on the last model - one at a time."); return; }
    busy = true;
    step("Fetching model...", o.title ? '"' + o.title + '"' : "Reading the MakerWorld link", {progress: "busy"});
    orca.postMessage({cmd: "prepare", design: o.design || 0, url: o.url || ""});
  }
  function showProfiles(m) {
    busy = true;
    var sel = m.selected;
    function draw() {
      var html = '<h3>Choose a print profile</h3><div class="small">' + esc(m.title) + ' has ' + m.profiles.length + ' print profiles. The first one is the recommended one.</div>';
      m.profiles.forEach(function (p) {
        var bits = [];
        if (p.printer) bits.push(p.printer + (p.nozzle ? " " + p.nozzle + " mm nozzle" : ""));
        if (p.plates) bits.push(p.plates + (p.plates === 1 ? " plate" : " plates"));
        if (p.minutes) bits.push(fmtMin(p.minutes));
        if (p.grams) bits.push(p.grams + " g");
        if (p.colors) bits.push(p.colors + (p.colors === 1 ? " colour" : " colours") + (p.ams ? " (needs AMS)" : ""));
        if (p.rating) bits.push("rated " + p.rating + "/5");
        html += '<div class="prof' + (p.id === sel ? ' sel' : '') + '" data-id="' + esc(p.id) + '"><img src="' + url(p.cover) + '"><div><div class="pt">' +
          esc(p.title) + '</div><div class="small">' + esc(bits.join(" · ")) + '</div>' +
          (p.others && p.others.length ? '<div class="small">Also fits: ' + esc(p.others.join(", ")) + '</div>' : '') + '</div></div>';
      });
      var list = [{label: "Cancel", quiet: true, fn: closeOverlay}, {label: "Download and open", fn: function () {
        var p = m.profiles.filter(function (x) { return x.id === sel; })[0];
        step("Fetching model...", "Asking MakerWorld for the file of \"" + (p ? p.title : "") + "\"", {progress: "busy"});
        orca.postMessage({cmd: "download", design: m.design, instance: sel, title: m.title});
      }}];
      overlay(html + btns(list)); wire(list);
      Array.prototype.forEach.call($("box").querySelectorAll(".prof"), function (e) {
        e.addEventListener("click", function () { sel = parseInt(e.dataset.id, 10); draw(); });
      });
    }
    draw();
  }
  function onProgress(m) {
    var f = $("pbf"), t = $("pbt");
    if (!f) { step("Downloading...", '"' + m.name + '"', {progress: true, buttons: [cancelBtn], small: "Saving to " + m.dir}); f = $("pbf"); t = $("pbt"); }
    if (m.total) { f.style.width = Math.min(100, 100 * m.done / m.total).toFixed(1) + "%"; t.textContent = fmtBytes(m.done) + " of " + fmtBytes(m.total) + " (" + Math.floor(100 * m.done / m.total) + "%)"; }
    else { t.textContent = fmtBytes(m.done) + " downloaded"; }
  }
  orca.onMessage(function (m) {
    if (!m) return;
    switch (m.type) {
      case "results": renderResults(m); break;
      case "detail": renderDetail(m); break;
      case "config": cfg = m.config; var w = cfgWaiters; cfgWaiters = []; w.forEach(function (fn) { fn(cfg); }); break;
      case "navs": navs = m.navs; var w2 = navWaiters; navWaiters = []; w2.forEach(function (fn) { fn(navs); }); break;
      case "account": setAccount(m); if (m.closeLogin) closeOverlay(); if (m.toast) toast(m.toast); break;
      case "loginmsg": if ($("lmsg")) { $("lmsg").className = "msg " + (m.error ? "err" : ""); $("lmsg").textContent = m.text; } else toast(m.text, m.error); break;
      case "needlogin": busy = false; showLogin("Downloads need a MakerWorld sign-in. Sign in, then click Open in Orca again."); break;
      case "profiles": showProfiles(m); break;
      case "dlstart": step("Downloading...", '"' + m.name + '"', {progress: true, buttons: [cancelBtn], small: "Saving to " + m.dir}); break;
      case "progress": onProgress(m); break;
      case "opening": step("Opening in Orca...", '"' + m.name + '" downloaded (' + fmtBytes(m.size) + '). Handing it to the Orca window...', {progress: "busy", small: m.path}); break;
      case "done": step("Opened in Orca", m.text, {cls: "ok", small: m.path, buttons: [closeBtn]}); busy = false; break;
      case "error": step(m.title || "Something went wrong", m.text, {cls: "err", buttons: [closeBtn]}); busy = false; break;
      case "toast": toast(m.text, m.error); break;
    }
  });
  // ---------- wiring ----------
  $("go").addEventListener("click", function () {
    var v = $("q").value.trim();
    if (!v) { go({name: "home"}); return; }
    if (/makerworld\.com/.test(v) || /^\d+$/.test(v)) { var mm = /models\/(\d+)/.exec(v) || /^(\d+)$/.exec(v);
      if (mm && !/profileId/.test(v)) { openDetail(+mm[1], ""); return; } startOpen({url: v}); return; }
    $("navsearch").style.display = "inline-block"; closeDetail(); go({name: "search", q: v});
  });
  $("q").addEventListener("keydown", function (e) { if (e.key === "Enter") $("go").click(); });
  Array.prototype.forEach.call($("nav").children, function (b) {
    if (!b.dataset.v) return;
    b.addEventListener("click", function () {
      var k = b.dataset.v;
      if (k === "search") { var v = $("q").value.trim(); if (v) go({name: "search", q: v}); return; }
      go({name: k});
    });
  });
  $("ov").addEventListener("click", function (e) { if (e.target === $("ov") && !$("pb")) closeOverlay(); });
  orca.postMessage({cmd: "ready"});
  go({name: "home"});
})();
</script>
""".replace("__TABS__", json.dumps(SNAP_TABS))


class MakerWorldPage(orca.pages.PagesPluginCapabilityBase):
    def __init__(self):
        super().__init__()
        self._q = queue.Queue()          # browse: lists, details, menus, sign-in
        self._oq = queue.Queue()         # open: prepare + download + hand-off
        self._workers = {}
        self._cancel = threading.Event()
        self._tfa_key = None
        self._list_seq = 0
        self._cfg = None
        self._navs = None
        self._seed = random.randint(1, 2 ** 31)
        self._uid = None

    def get_name(self):
        return "MakerWorld"

    def get_ui(self):
        log("get_ui called (tab built) v%s; downloads folder: %s (%s)" % (VERSION, DL_DIR, DL_REASON))
        return PAGE

    # ---- UI thread ----
    def on_message(self, data):
        try:
            if isinstance(data, str):
                data = json.loads(data)
            cmd = (data or {}).get("cmd")
            safe = {k: v for k, v in (data or {}).items() if k not in ("password", "code")}
            log("on_message %s" % json.dumps(safe)[:300])
            if cmd == "cancel":
                self._cancel.set()
                return
            if cmd == "list":
                self._list_seq = max(self._list_seq, int(data.get("seq") or 0))
            if cmd in ("prepare", "download"):
                self._ensure_worker("open", self._oq)
                self._oq.put(data)
            else:
                self._ensure_worker("browse", self._q)
                self._q.put(data)
        except Exception:
            log("on_message error\n" + traceback.format_exc())

    def _ensure_worker(self, name, q):
        w = self._workers.get(name)
        if w is None or not w.is_alive():
            w = threading.Thread(target=self._run, args=(q,), name="makerworld-" + name, daemon=True)
            self._workers[name] = w
            w.start()

    # ---- worker threads ----
    def _post(self, **kw):
        try:
            self.post_message(kw)
        except Exception:
            log("post_message failed\n" + traceback.format_exc())

    def _run(self, q):
        while True:
            data = q.get()
            cmd = data.get("cmd")
            try:
                if cmd == "ready":
                    self._report_login()
                elif cmd == "list":
                    self._list(data)
                elif cmd == "detail":
                    self._detail(data)
                elif cmd == "config":
                    self._config()
                elif cmd == "navs":
                    self._navs_cmd()
                elif cmd == "prepare":
                    self._prepare(data)
                elif cmd == "download":
                    self._download(data)
                elif cmd == "login":
                    self._login(data)
                elif cmd == "signout":
                    self._signout()
            except Exception as e:
                log("worker error\n" + traceback.format_exc())
                if cmd in ("prepare", "download"):
                    self._post(type="error", text="Unexpected problem: %s" % e)
                elif cmd == "list":
                    self._post(type="results", seq=data.get("seq"), error="Unexpected problem: %s" % e)
                elif cmd == "detail":
                    self._post(type="detail", design=data.get("design"), error="Unexpected problem: %s" % e)
                else:
                    self._post(type="toast", text="Unexpected problem: %s" % e, error=True)

    # ---- account ----
    def _report_login(self, toast=None, close_login=False):
        tok = saved_token()
        if not tok:
            self._post(type="account", signedIn=False, dlDir=DL_DIR, toast=toast, closeLogin=close_login)
            return
        code, d = http_json(MWAPI + "user-service/my/profile", {"token": tok})
        log("profile -> %s name=%r" % (code, d.get("name") if isinstance(d, dict) else None))
        if code == 200 and isinstance(d, dict) and d.get("uid"):
            self._uid = d.get("uid")
            self._post(type="account", signedIn=True, name=d.get("name") or d.get("account"), dlDir=DL_DIR,
                       toast=toast, closeLogin=close_login)
        elif code in (401, 403):
            self._post(type="account", signedIn=False, expired=True, dlDir=DL_DIR)
        else:
            # Network trouble: keep the token and let downloads try it.
            self._post(type="account", signedIn=True, name="(could not reach MakerWorld)", dlDir=DL_DIR)

    def _signout(self):
        try:
            os.remove(TOKEN_FILE)
            log("signed out (token file removed)")
        except FileNotFoundError:
            pass
        self._post(type="account", signedIn=False, dlDir=DL_DIR, toast="Signed out.")

    def _login(self, data):
        email, pw, code = data.get("email"), data.get("password"), data.get("code")
        tok = None
        if code and self._tfa_key:
            c, d = http_post("https://bambulab.com/api/sign-in/tfa", {"tfaKey": self._tfa_key, "tfaCode": code},
                             want_cookies=True)
            tok = d.get("_cookie_token") or d.get("accessToken") or d.get("token")
            log("tfa login -> %s token=%s" % (c, bool(tok)))
        elif code:
            c, d = http_post(BAMBU_API + "/v1/user-service/user/login", {"account": email, "code": code})
            tok = d.get("accessToken")
            log("code login -> %s loginType=%r token=%s msg=%s" % (c, d.get("loginType"), bool(tok), api_message(d)[:120] if not tok else ""))
        elif not pw:
            c2, d2 = http_post(BAMBU_API + "/v1/user-service/user/sendemail/code", {"email": email, "type": "codeLogin"})
            log("code-only: sent email code -> %s" % c2)
            if c2 == 200:
                self._post(type="loginmsg", text="Bambu has emailed you a code. Type it in the code box and press Sign in again.")
            else:
                self._post(type="loginmsg", error=True, text="Could not send a code: %s" % api_message(d2))
            return
        else:
            c, d = http_post(BAMBU_API + "/v1/user-service/user/login", {"account": email, "password": pw, "apiError": ""})
            tok = d.get("accessToken")
            lt = d.get("loginType")
            log("password login -> %s loginType=%r token=%s" % (c, lt, bool(tok)))
            if not tok and lt == "verifyCode":
                http_post(BAMBU_API + "/v1/user-service/user/sendemail/code", {"email": email, "type": "codeLogin"})
                self._post(type="loginmsg", text="Bambu has emailed you a code. Type it in the code box and press Sign in again.")
                return
            if not tok and lt == "tfa":
                self._tfa_key = d.get("tfaKey")
                self._post(type="loginmsg", text="Type the code from your authenticator app in the code box and press Sign in again.")
                return
        if tok:
            save_token(tok)
            self._tfa_key = None
            self._report_login(toast="Signed in to MakerWorld.", close_login=True)
        else:
            self._post(type="loginmsg", error=True, text="Sign-in failed: %s" % api_message(d))

    # ---- menus ----
    def _navs_cmd(self):
        if self._navs is None:
            code, d = http_json(MWAPI + "search-service/homepage/nav")
            navs = []
            if code == 200 and isinstance(d, dict):
                # Following / For You come back empty from this feed for plugins; Laser & Cut isn't printable.
                navs = [[n["key"], n.get("name") or n["key"]] for n in d.get("navs") or []
                        if n.get("key") and n.get("type") == 2 and n["key"] != "LaserCut"]
            log("navs -> %s, %d" % (code, len(navs)))
            if not navs:
                return self._post(type="navs", navs=SNAP_NAVS)
            self._navs = navs
        self._post(type="navs", navs=self._navs)

    def _config(self):
        if self._cfg is None:
            cfg = None
            try:
                code, page = http_text(MW + "/en/3d-models")
                cfg = parse_site_config(page) if code == 200 else None
                log("site config -> %s, %d bytes, live=%s, %d categories" % (code, len(page), bool(cfg), len((cfg or {}).get("tree") or [])))
            except Exception as e:
                log("site config failed: %r" % e)
            if cfg is None:
                cfg = snapshot_config()
                self._post(type="config", config=cfg)
                return
            self._cfg = cfg
        self._post(type="config", config=self._cfg)

    # ---- lists ----
    def _get(self, path, params=None, auth=None):
        """auth: None = anonymous, 'opt' = send the token if we have one, 'req' = must be signed in."""
        url = MWAPI + path + ("?" + urllib.parse.urlencode(params) if params else "")
        cookies = None
        if auth:
            cookies = mw_cookies() or None
            if auth == "req" and not (cookies or {}).get("token"):
                raise NeedSignIn()
        code, d = http_json(url, cookies)
        if code in (401, 403) and auth == "req":
            self._post(type="account", signedIn=False, expired=True, dlDir=DL_DIR)
            raise NeedSignIn("expired")
        return code, d

    def _list(self, data):
        src, seq = data.get("src") or {}, int(data.get("seq") or 0)
        cursor = int(data.get("cursor") or 0)
        kind = src.get("kind")
        if seq < self._list_seq:
            log("list %s skipped (stale seq %s < %s)" % (kind, seq, self._list_seq))
            return
        try:
            code, d, res = self._fetch_list(kind, src, cursor)
        except NeedSignIn as e:
            self._post(type="results", seq=seq, cursor=cursor, error=("Your MakerWorld sign-in has expired - sign in again."
                                                                     if str(e) == "expired" else "Sign in to see this."))
            return
        if code != 200 or res is None:
            log("list %s src=%s cursor=%d -> %s %s" % (kind, json.dumps(src)[:160], cursor, code, api_message(d)))
            self._post(type="results", seq=seq, cursor=cursor,
                       error="MakerWorld didn't answer (%s): %s" % (code or "no connection", api_message(d)))
            return
        log("list %s src=%s cursor=%d -> %s, %d items, next=%s more=%s total=%s" % (
            kind, json.dumps({k: v for k, v in src.items() if k != "kind" and v})[:160], cursor, code,
            len(res["items"]), res["next"], res["hasMore"], res.get("total")))
        self._post(type="results", seq=seq, cursor=cursor, **res)

    def _paged(self, d, hits_key, cursor, norm, limit=PAGE_SIZE, total_key="total"):
        raw = list((d or {}).get(hits_key) or [])
        total = (d or {}).get(total_key)
        nxt = cursor + len(raw)
        more = bool(raw) and (nxt < total if isinstance(total, int) and total else len(raw) >= limit)
        return {"items": [norm(h) for h in raw], "next": nxt, "hasMore": more, "total": total}

    def _fetch_list(self, kind, src, cursor):
        page = {"limit": PAGE_SIZE, "offset": cursor}
        if kind == "nav":
            code, d = self._get("search-service/select/design/nav2", {"navKey": src.get("key") or "Trending", "limit": PAGE_SIZE, "seed": cursor}, "opt")
            if code != 200 or not isinstance(d, dict):
                return code, d, None
            hits = [norm_hit(h["design"]) for h in d.get("hits") or [] if h.get("type") == 0 and h.get("design")]
            nxt = d.get("seed") if isinstance(d.get("seed"), int) else cursor + len(d.get("hits") or [])
            return code, d, {"items": hits, "next": nxt, "hasMore": bool(d.get("hasMore")) and bool(d.get("hits")), "total": None}
        if kind == "following":
            code, d = self._get("search-service/homepage/following", page, "req")
            if code != 200 or not isinstance(d, dict):
                return code, d, None
            res = self._paged(d.get("feed") or {}, "hits", cursor, norm_hit)
            res["emptyText"] = "Nothing new from the designers you follow."
            return code, d, res
        if kind == "models":
            p = dict(page, keyword=src.get("q") or "", orderBy=src.get("orderBy") or ("score" if src.get("q") else "hotScore"))
            for k_src, k_api in (("since", "designCreateSince"), ("cat", "categories"), ("tag", "model_tag"),
                                 ("license", "licenses"), ("contestId", "contestId")):
                if src.get(k_src):
                    p[k_api] = src[k_src]
            code, d = self._get("search-service/select/design2", p, "opt")
            if code != 200 or not isinstance(d, dict):
                return code, d, None
            res = self._paged(d, "hits", cursor, norm_hit)
            if res["next"] >= 10000:
                res["hasMore"] = False
            res["emptyText"] = "No models match." if not src.get("contestId") else "No entries yet."
            return code, d, res
        if kind == "makerlab":
            code, d = self._get("search-service/select/design/makerlab", page, "opt")
            return code, d, (self._paged(d, "hits", cursor, norm_hit) if code == 200 and isinstance(d, dict) else None)
        if kind == "awarded":
            code, d = self._get("design-service/design/awarded", {"contestId": int(src.get("contestId") or 0), "limit": 100, "offset": 0})
            if code != 200 or not isinstance(d, dict):
                return code, d, None
            items = []
            for grp in d.get("hits") or []:
                for h in grp.get("hits") or []:
                    it = norm_hit(h)
                    it["rank"] = grp.get("name") or ("Rank %s" % grp.get("rank"))
                    items.append(it)
            return code, d, {"items": items, "next": len(items), "hasMore": False, "total": len(items),
                             "emptyText": "No winners announced yet."}
        if kind == "contests":
            code, d = self._get("design-service/contest/list", page)
            return code, d, (self._paged(d, "hits", cursor, norm_contest) if code == 200 and isinstance(d, dict) else None)
        if kind == "curated":
            code, d = self._get("design-service/favorites-collections/tab", dict(page, key=src.get("key") or SNAP_TABS[0][0], seed=self._seed))
            return code, d, (self._paged(d, "hits", cursor, norm_collection) if code == 200 and isinstance(d, dict) else None)
        if kind == "collection":
            code, d = self._get("design-service/favorites/%d/designs" % int(src.get("id") or 0), page, "opt")
            return code, d, (self._paged(d, "hits", cursor, norm_hit) if code == 200 and isinstance(d, dict) else None)
        if kind == "designer":
            uid = int(src.get("uid") or 0)
            p = dict(page)
            if src.get("orderBy"):
                p["orderBy"] = src["orderBy"]
            code, d = self._get("design-service/published/%d/design" % uid, p, "opt")
            if code != 200 or not isinstance(d, dict):
                return code, d, None
            res = self._paged(d, "hits", cursor, norm_hit)
            res["emptyText"] = "This designer has no public models."
            if cursor == 0:
                c2, pr = self._get("design-user-service/user/profile/%d" % uid, None, "opt")
                if c2 == 200 and isinstance(pr, dict):
                    res["profile"] = {"uid": uid, "name": pr.get("name") or "", "handle": pr.get("handle") or "",
                                      "avatar": https(pr.get("avatar")), "fans": pr.get("fanCount") or 0,
                                      "likes": pr.get("likeCount") or 0, "downloads": pr.get("downloadCount") or 0,
                                      "bio": plain_text((pr.get("personal") or {}).get("bio"), 400)}
            return code, d, res
        if kind == "liked":
            code, d = self._get("design-service/my/design/like", page, "req")
            return code, d, (self._paged(d, "hits", cursor, norm_hit) if code == 200 and isinstance(d, dict) else None)
        if kind == "downloads":
            code, d = self._get("design-service/my/favorites/download/designs", page, "req")
            return code, d, (self._paged(d, "hits", cursor, norm_hit) if code == 200 and isinstance(d, dict) else None)
        if kind == "mycollections":
            code, d = self._get("design-service/my/favorites/listlite", None, "req")
            if code != 200 or not isinstance(d, dict):
                return code, d, None
            raw = list(d.get("hits") or [])
            dflt = d.get("default")
            if dflt and not any(h.get("id") == dflt.get("id") for h in raw):
                raw.insert(0, dflt)
            items = [norm_collection(h) for h in raw]
            log("collections: %s" % ["%s(%s)" % (i["title"], i["count"]) for i in items])
            return code, d, {"items": items, "next": len(items), "hasMore": False, "total": len(items),
                             "emptyText": "You have no collections yet."}
        return 400, {"error": "unknown list %r" % kind}, None

    # ---- model details ----
    def _detail(self, data):
        design = int(data.get("design") or 0)
        code, d = self._get("design-service/design/%d" % design, None, "opt")
        if code != 200 or not isinstance(d, dict) or not d.get("id"):
            log("detail %s -> %s %s" % (design, code, api_message(d)))
            self._post(type="detail", design=data.get("design"),
                       error="Couldn't read that model. MakerWorld said %s: %s" % (code or "no connection", api_message(d)))
            return
        det = norm_detail(d)
        size = len(json.dumps(det))
        log("detail %s -> %s title=%r pics=%d profiles=%d desc=%d bytes msg=%d bytes" % (
            design, code, det["title"][:60], len(det["pictures"]), len(det["profiles"]), len(det["summary"]), size))
        self._post(type="detail", design=data.get("design"), detail=det)

    # ---- open: step 1, pick a print profile ----
    def _prepare(self, data):
        design, want_profile = int(data.get("design") or 0), None
        url = (data.get("url") or "").strip()
        if url:
            m = re.search(r"/models/(\d+)", url) or re.match(r"^(\d+)$", url)
            if not m:
                self._post(type="error", title="Not a MakerWorld link", text="That doesn't look like a MakerWorld model link.")
                return
            design = int(m.group(1))
            p = re.search(r"profileId[-=](\d+)", url)
            if p:
                want_profile = int(p.group(1))
        if not mw_cookies().get("token"):
            self._post(type="needlogin")
            return
        code, d = http_json("%sdesign-service/design/%s" % (MWAPI, design))
        if code != 200 or not isinstance(d, dict):
            log("design %s -> %s %s" % (design, code, api_message(d)))
            self._post(type="error", title="Couldn't read that model",
                       text="MakerWorld said %s: %s" % (code or "no connection", api_message(d)))
            return
        title = d.get("title") or str(design)
        if not d.get("instances"):
            self._post(type="error", title="No printable file", text="\"%s\" has no print profiles to download." % title)
            return
        profs = sorted_profiles(d)
        selected = profs[0]["id"]
        if want_profile:
            for p in profs:
                if p["profileId"] == want_profile or p["id"] == want_profile:
                    selected = p["id"]
        log("prepare design=%s title=%r profiles=%d selected=%s" % (design, title, len(profs), selected))
        if len(profs) == 1 or want_profile:
            self._download({"design": design, "instance": selected, "title": title})
        else:
            self._post(type="profiles", design=design, title=title, profiles=profs, selected=selected)

    # ---- open: step 2, download + hand to Orca ----
    def _download(self, data):
        design, instance, title = data.get("design"), int(data.get("instance") or 0), data.get("title") or ""
        cookies = mw_cookies()
        if not cookies.get("token"):
            self._post(type="needlogin")
            return
        code, f = http_json("%sdesign-service/instance/%s/f3mf?type=download" % (MWAPI, instance), cookies)
        log("f3mf design=%s instance=%s -> %s keys=%s" % (design, instance, code, list(f.keys()) if isinstance(f, dict) else type(f)))
        if code in (401, 403):
            self._post(type="account", signedIn=False, expired=True, dlDir=DL_DIR)
            self._post(type="error", title="Sign-in expired", text="MakerWorld refused the download because your sign-in has expired. Click Sign in at the top right and try again.")
            return
        if code != 200 or not f.get("url"):
            log("f3mf body: %s" % api_message(f))
            self._post(type="error", title="MakerWorld refused the download", text="MakerWorld said %s: %s" % (code or "no connection", api_message(f)))
            return
        name = safe_filename(f.get("name"), title or instance)
        folder, note = DL_DIR, ""
        try:
            path = unique_path(folder, name)
            out = open(path + ".part", "wb")
        except OSError as e:
            log("cannot write in %s (%r); falling back to %s" % (folder, e, FALLBACK_DL_DIR))
            folder, note = FALLBACK_DL_DIR, " (Orca's downloads folder was not writable, so it went to the plugin folder)"
            os.makedirs(folder, exist_ok=True)
            path = unique_path(folder, name)
            out = open(path + ".part", "wb")
        self._cancel.clear()
        self._post(type="dlstart", name=os.path.basename(path), dir=folder)
        done, total, last = 0, 0, 0.0
        try:
            req = urllib.request.Request(f["url"], headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=120) as r, out:
                total = int(r.headers.get("Content-Length") or 0)
                while True:
                    if self._cancel.is_set():
                        raise InterruptedError("cancelled")
                    chunk = r.read(1 << 16)
                    if not chunk:
                        break
                    out.write(chunk)
                    done += len(chunk)
                    now = time.monotonic()
                    if now - last > 0.15:
                        last = now
                        self._post(type="progress", done=done, total=total, name=os.path.basename(path), dir=folder)
            if total and done != total:
                raise IOError("download stopped early (%d of %d bytes)" % (done, total))
            os.replace(path + ".part", path)
        except BaseException as e:
            try:
                out.close()
                os.remove(path + ".part")
            except Exception:
                pass
            if isinstance(e, InterruptedError):
                log("download cancelled at %d bytes" % done)
                self._post(type="error", title="Download cancelled", text="Nothing was saved.")
                return
            log("download failed: %r" % e)
            self._post(type="error", title="Download failed", text="The download broke off: %s" % e)
            return
        self._post(type="progress", done=done, total=total or done, name=os.path.basename(path), dir=folder)
        log("downloaded %s (%d bytes)" % (path, done))
        self._post(type="opening", name=os.path.basename(path), size=done, path=path)
        env = dict(os.environ, MW_TAB_FILE=path)
        try:
            p = subprocess.Popen(OPEN_ARGV, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            try:
                o, _ = p.communicate(timeout=60)
                log("entrypoint hand-off exit=%s out=%s" % (p.returncode, o.decode("utf-8", "replace")[-400:].strip()))
            except subprocess.TimeoutExpired:
                log("entrypoint still running after 60s (probably opened a NEW window - is single instance on?)")
        except Exception as e:
            log("hand-off failed: %r" % e)
            self._post(type="error", title="Downloaded, but couldn't open it",
                       text="The file is saved at %s, but Orca wouldn't take it: %s. Open it with File > Open." % (path, e))
            return
        self._post(type="done", path=path,
                   text="\"%s\" was sent to Orca%s. Switch to the Prepare tab to see it. If nothing appeared, turn on "
                        "Preferences > \"Allow only one Orca instance\" and try again." % (os.path.basename(path), note))


@orca.plugin
class MakerWorldTabPlugin(orca.base):
    def register_capabilities(self):
        global DL_DIR, DL_REASON
        DL_DIR, DL_REASON = read_orca_download_dir()
        log("v%s load: downloads folder %s (%s)" % (VERSION, DL_DIR, DL_REASON))
        perms = dict(
            network_http=["https://makerworld.com", "https://makerworld.bblmw.com", "https://api.bambulab.com", "https://bambulab.com"],
            process=[ORCA_BIN, OPEN_ARGV[0], str(OPEN_ARGV)],
            threading=True,
        )
        if not DL_DIR.startswith(DATA_DIR + os.sep):
            perms["fs_read"] = [DL_DIR + "/*"]
            perms["fs_readwrite"] = [DL_DIR + "/*"]
        try:
            orca.request_permissions(**perms)
        except Exception:
            log("request_permissions failed, retrying without folder grants\n" + traceback.format_exc())
            perms.pop("fs_read", None)
            perms.pop("fs_readwrite", None)
            orca.request_permissions(**perms)
        orca.register_capability(MakerWorldPage)
