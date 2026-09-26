"""Small local lab for tracing website, API, cache and player requests."""
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote
from urllib.request import Request, urlopen
import json
import re
import sqlite3
import threading
import time

ROOT = Path(__file__).parent
LOCK = threading.Lock()
FETCH_LOCK = threading.Lock()
SERIES_GUARD = threading.Lock()
SERIES_LOCKS = {}
SERIES_LAST = {}
EVENT_LOCK = threading.Lock()
EVENTS = []
DISCOVERY_LOCK = threading.Lock()
DISCOVERY_CACHE = {}
MEDIA_CACHE = {}
SCHEDULE_CACHE = {}
METRICS_LOCK = threading.Lock()
METRICS = {"catalog_reads": 0, "series_reads": 0, "supplier_calls": 0, "cache_hits": 0, "shared_fetches": 0, "supplier_failures": 0}
DB = ROOT / "catalog-cache.sqlite3"
SOURCE = []
CACHE = {"titles": [], "updated_at": None, "version": 0, "next_page": 2}
SUPPLIER_IDS = {"ani_id": {}, "mal_id": {}}
INTERVAL = 600
SERIES_TTL = 600
PROVIDER = "https://anikotoapi.site"
ANILIST = "https://graphql.anilist.co"
MEDIA_FIELDS = "id idMal title { english romaji } description(asHtml: false) coverImage { large } genres format status seasonYear popularity updatedAt episodes nextAiringEpisode { episode }"
DISCOVER_QUERY = "query($page:Int,$search:String,$genre:String,$format:MediaFormat,$status:MediaStatus,$sort:[MediaSort]) { Page(page:$page,perPage:24) { pageInfo { hasNextPage currentPage } media(type:ANIME,isAdult:false,search:$search,genre:$genre,format:$format,status:$status,sort:$sort) { " + MEDIA_FIELDS + " } } }"
DETAIL_QUERY = "query($id:Int) { Media(id:$id,type:ANIME) { " + MEDIA_FIELDS + " } }"
SIMPLE_DISCOVER_QUERY = "query { Page(page:1,perPage:24) { pageInfo { hasNextPage currentPage } media(type:ANIME,isAdult:false,sort:[POPULARITY_DESC]) { " + MEDIA_FIELDS + " } } }"
SEARCH_QUERY = "query($page:Int!,$search:String!) { Page(page:$page,perPage:24) { pageInfo { hasNextPage currentPage } media(type:ANIME,isAdult:false,search:$search) { " + MEDIA_FIELDS + " } } }"
SCHEDULE_QUERY = "query($page:Int!,$start:Int!,$end:Int!) { Page(page:$page,perPage:25) { pageInfo { hasNextPage } airingSchedules(airingAt_greater:$start,airingAt_lesser:$end,sort:[TIME]) { airingAt episode media { id type isAdult title { english romaji } coverImage { large } } } } }"
CACHE.update(source="sample", error=None, details={})


def record(stage, detail):
    with EVENT_LOCK:
        EVENTS.append({"time": int(time.time()), "stage": stage, "detail": detail})
        del EVENTS[:-60]


def count(name):
    with METRICS_LOCK:
        METRICS[name] += 1


def episode_sources(raw):
    """Expose documented HTTPS player embeds from the expected provider only."""
    embeds = raw.get("embed_url") or raw.get("embed_urls") or {}
    if not isinstance(embeds, dict):
        embeds = {}
    result = {}
    for language in ("sub", "dub"):
        value = embeds.get(language)
        if isinstance(value, dict):
            value = value.get("url") or value.get("embed")
        parsed = urlparse(value) if isinstance(value, str) else None
        valid = bool(parsed and parsed.scheme == "https" and parsed.hostname == "megaplay.buzz" and parsed.path.startswith("/stream/") and not parsed.username and not parsed.password)
        result[language] = {"available": valid, "domain": parsed.hostname if valid else None, "embed_url": value if valid else None}
    return result


def episode_number(value):
    """Return a positive episode count or None for unknown/non-numeric values."""
    try:
        number = int(value)
        return number if not isinstance(value, bool) and number > 0 else None
    except (ValueError, TypeError):
        return None


def candidate_episodes(item):
    """Numbered player attempts, never claimed as confirmed episodes."""
    anime_id = episode_number(item.get("ani_id"))
    if not anime_id:
        return []
    released = item.get("released_episodes")
    count = released if released is not None else (1 if item.get("status") not in ("NOT_YET_RELEASED", "CANCELLED") else 0)
    candidates = []
    for number in range(1, min(count, 2000) + 1):
        base = f"https://megaplay.buzz/stream/ani/{anime_id}/{number}/"
        candidates.append({"number": number, "title": f"Episode {number}", "availability": "unverified",
                           "sources": {language: {"available": False, "embed_url": base + language}
                                       for language in ("sub", "dub")}})
    return candidates


def save_cache():
    """Call with LOCK held. SQLite commits the complete snapshot atomically."""
    snapshot = {key: CACHE[key] for key in ("titles", "details", "updated_at", "version", "source", "next_page")}
    with sqlite3.connect(DB) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute("INSERT INTO cache(key, value) VALUES ('snapshot', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (json.dumps(snapshot),))


def clean_titles():
    """Drop demonstration titles left by earlier versions of the lab."""
    old = {"orbit-garden", "river-signal", "paper-moon"}
    CACHE["titles"] = [item for item in CACHE["titles"] if item.get("id") not in old]
    CACHE["details"] = {key: detail for key, detail in CACHE["details"].items() if key not in old}


def anilist_request(query, variables):
    body = json.dumps({"query": query, "variables": variables}).encode()
    request = Request(ANILIST, data=body, headers={"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "CatalogLearningLab/2.0"}, method="POST")
    with urlopen(request, timeout=8) as response:
        payload = json.load(response)
    if payload.get("errors") or not payload.get("data"):
        raise ValueError("Anime catalog returned an error")
    return payload["data"]


def normalize_media(media):
    title = media.get("title") or {}
    raw = media.get("description") or ""
    description = re.sub(r"<[^>]+>", " ", raw)
    description = re.sub(r"\s+", " ", description).strip()[:900]
    poster = (media.get("coverImage") or {}).get("large")
    total = episode_number(media.get("episodes"))
    next_airing = episode_number((media.get("nextAiringEpisode") or {}).get("episode"))
    released = next_airing - 1 if next_airing else total if media.get("status") == "FINISHED" else None
    return {"id": "ani:" + str(media["id"]), "ani_id": str(media["id"]), "mal_id": str(media["idMal"]) if media.get("idMal") else None,
            "title": title.get("english") or title.get("romaji") or "Untitled", "description": description,
            "poster": poster if isinstance(poster, str) and poster.startswith("https://") else None,
            "genres": media.get("genres") or [], "format": media.get("format"), "status": media.get("status"),
            "year": media.get("seasonYear"), "popularity": media.get("popularity") or 0, "updated_at": media.get("updatedAt"),
            "total_episodes": total, "released_episodes": released}


def upcoming_schedule(params):
    now = int(time.time())
    start = int(params.get("start", [str(now)])[0])
    end = int(params.get("end", [str(start + 86400)])[0])
    page = int(params.get("page", ["1"])[0])
    if not now - 15 * 86400 <= start <= now + 30 * 86400 or not 0 < end - start <= 90000 or not 1 <= page <= 20:
        return {"items": [], "has_next": False, "warning": "Choose a date within the schedule range."}
    key = (start, end, page)
    with DISCOVERY_LOCK:
        saved = SCHEDULE_CACHE.get(key)
        if saved and now - saved[0] < 600:
            return saved[1]
    try:
        payload = anilist_request(SCHEDULE_QUERY, {"page": page, "start": start - 1, "end": end})["Page"]
        items = []
        for row in payload.get("airingSchedules") or []:
            media = row.get("media") or {}
            timestamp = row.get("airingAt")
            if media.get("type") != "ANIME" or media.get("isAdult") or not isinstance(timestamp, int) or not start <= timestamp < end:
                continue
            title = media.get("title") or {}
            image = (media.get("coverImage") or {}).get("large")
            items.append({"id": "ani:" + str(media["id"]), "title": title.get("english") or title.get("romaji") or "Untitled",
                          "poster": image if isinstance(image, str) and image.startswith("https://") else None,
                          "episode": episode_number(row.get("episode")), "airing_at": timestamp})
        items.sort(key=lambda item: (item["airing_at"], item["title"].casefold()))
        result = {"items": items, "page": page, "has_next": bool((payload.get("pageInfo") or {}).get("hasNextPage")),
                  "source": "AniList estimated airing times"}
        with DISCOVERY_LOCK:
            SCHEDULE_CACHE[key] = (now, result)
            if len(SCHEDULE_CACHE) > 100:
                del SCHEDULE_CACHE[min(SCHEDULE_CACHE, key=lambda item: SCHEDULE_CACHE[item][0])]
        return result
    except Exception:
        if saved:
            return {**saved[1], "warning": "Showing saved schedule times."}
        return {"items": [], "page": page, "has_next": False, "source": "AniList estimated airing times",
                "warning": "Schedule is unavailable right now."}


def media_detail(anime_id):
    with DISCOVERY_LOCK:
        saved = MEDIA_CACHE.get(anime_id)
        if saved and time.time() - saved[0] < 600:
            return saved[1]
        for fetched_at, page in DISCOVERY_CACHE.values():
            if time.time() - fetched_at < 600:
                match = next((item for item in page["items"] if item["ani_id"] == str(anime_id)), None)
                if match:
                    MEDIA_CACHE[anime_id] = (fetched_at, match)
                    return match
    item = normalize_media(anilist_request(DETAIL_QUERY, {"id": anime_id})["Media"])
    with DISCOVERY_LOCK:
        MEDIA_CACHE[anime_id] = (time.time(), item)
        if len(MEDIA_CACHE) > 120:
            oldest = min(MEDIA_CACHE, key=lambda key: MEDIA_CACHE[key][0])
            del MEDIA_CACHE[oldest]
    return item


def supplier_match(item):
    with LOCK:
        return (SUPPLIER_IDS["ani_id"].get(str(item.get("ani_id"))) if item.get("ani_id") else None) or (SUPPLIER_IDS["mal_id"].get(str(item.get("mal_id"))) if item.get("mal_id") else None)


def rebuild_supplier_ids():
    """Call with LOCK held; keys are metadata IDs, values are supplier titles."""
    for key in SUPPLIER_IDS:
        SUPPLIER_IDS[key] = {str(item[key]): item for item in CACHE["titles"] if item.get(key)}


def discovery_request(page, filters):
    """Include only active GraphQL filters; preserve the requested page."""
    declarations = ["$page:Int!", "$sort:[MediaSort]"]
    arguments = ["type:ANIME", "isAdult:false", "sort:$sort"]
    variables = {"page": page, "sort": [filters["sort"]]}
    for name, graphql_type in (("search", "String"), ("genre", "String"),
                               ("format", "MediaFormat"), ("status", "MediaStatus")):
        if filters[name]:
            declarations.append(f"${name}:{graphql_type}")
            arguments.append(f"{name}:${name}")
            variables[name] = filters[name]
    query = (f"query({','.join(declarations)}) {{ Page(page:$page,perPage:24) "
             f"{{ pageInfo {{ hasNextPage currentPage }} media({','.join(arguments)}) "
             "{ " + MEDIA_FIELDS + " } } }")
    return anilist_request(query, variables)["Page"]


def discover(params):
    page = max(1, min(200, int(params.get("page", ["1"])[0])))
    filters = {"search": params.get("q", [""])[0].strip()[:100] or None,
               "genre": params.get("genre", [""])[0].strip()[:50] or None,
               "format": params.get("format", [""])[0], "status": params.get("status", [""])[0],
               "sort": params.get("sort", ["POPULARITY_DESC"])[0]}
    if filters["format"] not in ("", "TV", "MOVIE", "OVA", "ONA", "SPECIAL", "TV_SHORT"): filters["format"] = ""
    if filters["status"] not in ("", "RELEASING", "FINISHED", "NOT_YET_RELEASED"): filters["status"] = ""
    if filters["sort"] not in ("POPULARITY_DESC", "TRENDING_DESC", "UPDATED_AT_DESC", "START_DATE_DESC"): filters["sort"] = "POPULARITY_DESC"
    key = (page, *filters.values())
    with DISCOVERY_LOCK:
        saved = DISCOVERY_CACHE.get(key)
        if saved and time.time() - saved[0] < 600:
            return saved[1]
    try:
        payload = discovery_request(page, filters)
        if filters["search"] and not payload.get("media"):
            record("AniList", "Empty search page; retrying without sort and optional filters")
            retry = anilist_request(SEARCH_QUERY, {"page": page, "search": filters["search"]})["Page"]
            if retry.get("media"):
                rows = retry["media"]
                if filters["genre"]:
                    rows = [row for row in rows if filters["genre"] in (row.get("genres") or [])]
                if filters["format"]:
                    rows = [row for row in rows if row.get("format") == filters["format"]]
                if filters["status"]:
                    rows = [row for row in rows if row.get("status") == filters["status"]]
                if rows:
                    payload = {**retry, "media": rows}
        if page == 1 and filters["sort"] == "POPULARITY_DESC" and not any((filters["search"], filters["genre"], filters["format"], filters["status"])) and not payload.get("media"):
            record("AniList", "Empty unfiltered page; retrying a minimal catalog query")
            payload = anilist_request(SIMPLE_DISCOVER_QUERY, {})["Page"]
            if not payload.get("media"):
                raise ValueError("AniList returned an empty first page twice")
    except Exception as exc:
        record("AniList", f"Discovery failed ({type(exc).__name__}: {str(exc)[:180]}); checking saved results")
        if saved:
            return {**saved[1], "warning": "AniList is unavailable; showing an earlier saved result for this search."}
        with LOCK:
            recent = list(CACHE["titles"])
        matches = [item for item in recent
                   if (not filters["search"] or filters["search"].casefold() in item["title"].casefold())
                   and (not filters["genre"] or filters["genre"] in item.get("genres", []))
                   and (not filters["format"] or filters["format"] == item.get("format"))
                   and not filters["status"]]
        if filters["sort"] == "POPULARITY_DESC":
            matches.sort(key=lambda item: item["title"].casefold())
        start = (page - 1) * 24
        return {"items": matches[start:start + 24], "page": page,
                "has_next": len(matches) > start + 24, "source": "saved recent supplier titles",
                "warning": "AniList is unavailable; showing matching saved recent titles only."
                           if recent else "AniList is unavailable and no recent titles have been saved. Check the server connection and refresh the recent feed."}
    items = [normalize_media(row) for row in payload.get("media") or []]
    for item in items:
        match = supplier_match(item)
        if match:
            item["supplier_id"] = match["id"]
    has_next = bool((payload.get("pageInfo") or {}).get("hasNextPage"))
    if len(items) == 24 and not has_next and page < 200:
        # Some upstream responses omit or misreport pageInfo. Check the next page
        # before disabling the Next button on a full result page.
        try:
            has_next = bool(discovery_request(page + 1, filters).get("media"))
        except Exception:
            pass
    result = {"items": items,
              "page": page, "has_next": has_next, "source": "AniList metadata"}
    with DISCOVERY_LOCK:
        DISCOVERY_CACHE[key] = (time.time(), result)
        if len(DISCOVERY_CACHE) > 120:
            oldest = min(DISCOVERY_CACHE, key=lambda item: DISCOVERY_CACHE[item][0])
            del DISCOVERY_CACHE[oldest]
    return result


def record_view(item):
    try:
        with sqlite3.connect(DB, timeout=2) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS views (id TEXT NOT NULL, viewed_at INTEGER NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS view_titles (id TEXT PRIMARY KEY, title TEXT NOT NULL, poster TEXT)")
            connection.execute("INSERT INTO views VALUES (?, ?)", (item["id"], int(time.time())))
            connection.execute("INSERT INTO view_titles VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET title=excluded.title, poster=excluded.poster",
                               (item["id"], item.get("title") or item["id"], item.get("poster")))
            connection.execute("DELETE FROM views WHERE viewed_at < ?", (int(time.time()) - 31 * 86400,))
    except sqlite3.Error as exc:
        record("SQLite", f"View counter skipped ({type(exc).__name__})")


def top_views(period):
    seconds = {"today": 86400, "week": 7 * 86400, "month": 30 * 86400}.get(period, 86400)
    with sqlite3.connect(DB) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS views (id TEXT NOT NULL, viewed_at INTEGER NOT NULL)")
        connection.execute("CREATE TABLE IF NOT EXISTS view_titles (id TEXT PRIMARY KEY, title TEXT NOT NULL, poster TEXT)")
        counts = connection.execute("SELECT id, COUNT(*) FROM views WHERE viewed_at >= ? GROUP BY id ORDER BY COUNT(*) DESC LIMIT 10", (int(time.time()) - seconds,)).fetchall()
        saved_titles = {identifier: {"id": identifier, "title": title, "poster": poster}
                        for identifier, title, poster in connection.execute("SELECT id, title, poster FROM view_titles")}
    with LOCK:
        known = {item["id"]: item for item in CACHE["titles"]}
    with DISCOVERY_LOCK:
        for _, data in DISCOVERY_CACHE.values():
            known.update({item["id"]: item for item in data["items"]})
        known.update({item["id"]: item for _, item in MEDIA_CACHE.values()})
    return [{**known.get(identifier, saved_titles.get(identifier, {"id": identifier, "title": identifier})), "views": views}
            for identifier, views in counts]


def load_cache():
    with sqlite3.connect(DB) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        row = connection.execute("SELECT value FROM cache WHERE key='snapshot'").fetchone()
    if row:
        data = json.loads(row[0])
        if isinstance(data.get("titles"), list) and isinstance(data.get("details"), dict):
            CACHE.update({key: data[key] for key in ("titles", "details", "updated_at", "version", "source")})
            CACHE["next_page"] = max(2, int(data.get("next_page") or 2))
            CACHE["error"] = None
            clean_titles()
            with LOCK:
                rebuild_supplier_ids()
            outdated = [key for key, detail in CACHE["details"].items() if not isinstance(detail, dict) or any(not isinstance(episode, dict) or "sources" not in episode or "embed_id" not in episode or "embed_url" not in episode.get("sources", {}).get("sub", {}) for episode in detail.get("episodes", []))]
            if outdated:
                for key in outdated:
                    del CACHE["details"][key]
                with LOCK:
                    save_cache()
            with LOCK:
                save_cache()
            record("SQLite", f"Restored saved catalog version {CACHE['version']}")
            return
    CACHE.update(titles=[], updated_at=int(time.time()), version=1)
    with LOCK:
        save_cache()
    record("SQLite", "Created empty supplier catalog")


def refresh():
    if not FETCH_LOCK.acquire(blocking=False):
        with LOCK:
            CACHE["error"] = "A refresh is already running"
        return False
    try:
        return _refresh()
    finally:
        FETCH_LOCK.release()


def _refresh():
    """Refresh page one and two deeper pages per run, retaining older discoveries."""
    record("Supplier", "Fetching recent catalog on Python server")
    try:
        items = []
        with LOCK:
            next_page = CACHE["next_page"]
        for page in (1, next_page, next_page + 1):
            count("supplier_calls")
            request = Request(PROVIDER + f"/recent-anime?page={page}&per_page=30", headers={"Accept": "application/json", "User-Agent": "CatalogLearningLab/1.1"})
            with urlopen(request, timeout=8) as response:
                payload = json.load(response)
            if isinstance(payload, dict) and payload.get("ok") is False:
                raise ValueError("Provider returned ok=false")
            records = payload if isinstance(payload, list) else next((payload.get(k) for k in ("anime", "items", "results", "data", "recent_anime") if isinstance(payload.get(k), list)), [])
            if not records:
                if page == 1: raise ValueError("Provider returned no recent titles")
                next_page = 2
                break
            for row in records:
                if not isinstance(row, dict):
                    continue
                name = row.get("title") or row.get("name")
                if isinstance(name, dict):
                    name = name.get("english") or name.get("romaji")
                identifier = row.get("id") or row.get("anime_id") or row.get("slug")
                poster = row.get("poster") or row.get("image") or row.get("cover")
                if isinstance(poster, dict):
                    poster = poster.get("large") or poster.get("url")
                if identifier and name:
                    terms = row.get("terms_by_type") or {}
                    released = [episode_number(row.get(key)) for key in ("is_sub", "is_dub")]
                    released = max((number for number in released if number is not None), default=None)
                    items.append({"id": str(identifier), "title": str(name), "description": str(row.get("description") or row.get("synopsis") or ""), "poster": poster if isinstance(poster, str) and poster.startswith("https://") else None,
                                  "ani_id": str(row.get("ani_id")) if row.get("ani_id") else None, "mal_id": str(row.get("mal_id")) if row.get("mal_id") else None,
                                  "genres": terms.get("genre", []) if isinstance(terms, dict) else [], "format": (terms.get("type") or [None])[0] if isinstance(terms, dict) else None,
                                  "released_episodes": released, "total_episodes": episode_number(row.get("episodes"))})
            if page != 1:
                next_page = page + 1
        if not items:
            raise ValueError("Provider returned no recognizable titles")
    except Exception as exc:
        count("supplier_failures")
        with LOCK:
            CACHE["error"] = f"{type(exc).__name__}: {exc}"
        record("Supplier", f"Fetch failed ({type(exc).__name__}); kept saved catalog")
        return False
    with LOCK:
        combined = {item["id"]: item for item in items}
        combined.update({item["id"]: item for item in CACHE["titles"] if item["id"] not in combined})
        CACHE["titles"] = list(combined.values())
        CACHE["next_page"] = next_page
        rebuild_supplier_ids()
        CACHE["updated_at"] = int(time.time())
        CACHE["version"] += 1
        CACHE["source"] = "Anikoto"
        CACHE["error"] = None
        save_cache()
        record("SQLite", f"Saved {len(items)} supplier titles in version {CACHE['version']}")
    return True


load_cache()


def scheduler():
    while True:
        time.sleep(INTERVAL)
        refresh()


class Handler(BaseHTTPRequestHandler):
    def reply(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def serve_series(self, series_id, force=False):
        count("series_reads")
        if series_id.startswith("ani:"):
            try:
                anime_id = int(series_id.removeprefix("ani:"))
                if anime_id < 1:
                    raise ValueError("Invalid anime ID")
                item = media_detail(anime_id)
                match = supplier_match(item)
                if match:
                    return self._serve_series(match["id"], force, fallback=item)
                if force:
                    return self.reply(400, {"error": "No supplier episode record for this title"})
                record_view(item)
                # AniList supplies counts, not playback availability. These are
                # candidate episode numbers for MegaPlay's AniList-ID mapping.
                return self.reply(200, {**item, "episodes": candidate_episodes(item), "_origin": "AniList metadata",
                                        "_metadata_only": True, "_stale": False})
            except Exception as exc:
                return self.reply(502, {"error": f"Metadata unavailable: {type(exc).__name__}"})
        with LOCK:
            exists = any(entry["id"] == series_id for entry in CACHE["titles"])
            cached = CACHE["details"].get(series_id)
            supplier = CACHE["source"] == "Anikoto" and series_id not in {entry["id"] for entry in SOURCE}
        if not exists or not supplier or (cached and not force and time.time() - cached.get("_fetched_at", 0) < SERIES_TTL):
            return self._serve_series(series_id, force)
        with SERIES_GUARD:
            series_lock = SERIES_LOCKS.setdefault(series_id, threading.Lock())
            previous = SERIES_LAST.get(series_id)
        waiting = not series_lock.acquire(blocking=False)
        if waiting:
            record("Supplier", "Another request is already fetching this series; waiting")
            series_lock.acquire()
        try:
            with SERIES_GUARD:
                latest = SERIES_LAST.get(series_id)
            if waiting and latest is not None and latest is not previous:
                count("shared_fetches")
                code, result = latest
                if code == 200 and not result.get("_stale"):
                    result = {**result, "_origin": "saved cache"}
                record("SQLite", "Shared the result of the in-progress series request")
                return self.reply(code, result)
            return self._serve_series(series_id, force)
        finally:
            series_lock.release()

    def _serve_series(self, series_id, force=False, fallback=None):
        with LOCK:
            item = next((entry for entry in CACHE["titles"] if entry["id"] == series_id), None)
            cached = CACHE["details"].get(series_id)
            source = CACHE["source"]
        if not item:
            return self.reply(404, {"error": "Series not found"})
        is_demo = series_id in {entry["id"] for entry in SOURCE}
        if force and (is_demo or source != "Anikoto"):
            return self.reply(400, {"error": "Only supplier series can be refreshed"})
        is_stale = bool(cached and time.time() - cached.get("_fetched_at", 0) >= SERIES_TTL)
        if source == "Anikoto" and not is_demo and (force or not cached or is_stale):
            record("Supplier", "Manually refreshing series detail" if force else "Refreshing stale series detail" if cached else "Fetching new series detail")
            count("supplier_calls")
            try:
                from urllib.parse import quote
                request = Request(PROVIDER + "/series/" + quote(series_id, safe=""), headers={"Accept": "application/json", "User-Agent": "CatalogLearningLab/1.1"})
                with urlopen(request, timeout=8) as response:
                    detail = json.load(response)
                if isinstance(detail, dict) and isinstance(detail.get("data"), dict):
                    detail = detail["data"]
                episodes = detail.get("episodes", []) if isinstance(detail, dict) else []
                if not isinstance(detail, dict):
                    raise ValueError("Unexpected series response")
                if not isinstance(episodes, list):
                    episodes = []
                if isinstance(detail.get("anime"), dict):
                    detail = detail["anime"]
                item = {
                    **item,
                    "description": detail.get("description") or detail.get("synopsis") or item["description"],
                    "listed_episode_count": detail.get("episodes"),
                    "mal_id": detail.get("mal_id"),
                    "anilist_id": detail.get("ani_id"),
                    "episodes": [
                        {"title": e.get("title") or e.get("name") or f"Episode {i + 1}",
                         "number": e.get("number") or i + 1,
                         "catalog_episode_id": e.get("id"),
                         "embed_id": e.get("episode_embed_id"),
                         "sources": episode_sources(e)}
                        for i, e in enumerate(episodes) if isinstance(e, dict)
                    ],
                    "_fetched_at": int(time.time()),
                }
                with LOCK:
                    CACHE["details"][series_id] = item
                    save_cache()
                record("SQLite", f"Saved {len(item['episodes'])} episode records")
                origin = "supplier"
            except Exception as exc:
                count("supplier_failures")
                record("Supplier", f"Series fetch failed ({type(exc).__name__})")
                if cached:
                    result = {**cached, "_stale": True, "_origin": "saved cache", "_refresh_error": f"{type(exc).__name__}: supplier unavailable"}
                    if fallback and not result.get("episodes"):
                        result = {**fallback, "episodes": candidate_episodes(fallback), "_origin": "AniList metadata",
                                  "_metadata_only": True, "_stale": True, "_refresh_error": "Supplier episode list unavailable"}
                    with SERIES_GUARD:
                        SERIES_LAST[series_id] = (200, result)
                    return self.reply(200, result)
                if fallback and not force:
                    return self.reply(200, {**fallback, "episodes": candidate_episodes(fallback),
                                            "_origin": "AniList metadata", "_metadata_only": True,
                                            "_stale": True, "_refresh_error": "Supplier episode list unavailable"})
                result = {"error": f"Series supplier unavailable: {type(exc).__name__}"}
                with SERIES_GUARD:
                    SERIES_LAST[series_id] = (502, result)
                return self.reply(502, result)
        elif cached:
            count("cache_hits")
            item = cached
            origin = "saved cache"
            record("SQLite", "Series detail served from saved cache")
        else:
            origin = "local demo"
            record("Website", "Demo series served from local data")
        result = {**item, "_stale": False, "_origin": origin}
        if fallback and not result.get("episodes"):
            result = {**fallback, "episodes": candidate_episodes(fallback), "_origin": "AniList metadata",
                      "_metadata_only": True, "_stale": False}
        elif fallback:
            result = {**result, "ani_id": fallback.get("ani_id"),
                      "mal_id": fallback.get("mal_id") or result.get("mal_id")}
        if origin == "supplier":
            with SERIES_GUARD:
                SERIES_LAST[series_id] = (200, result)
        if not force:
            record_view(result)
        self.reply(200, result)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/api/catalog":
            count("catalog_reads")
            count("cache_hits")
            params = parse_qs(parsed.query)
            query = params.get("q", [""])[0].casefold().strip()
            letter = params.get("letter", [""])[0].upper()
            if letter and letter not in "#ABCDEFGHIJKLMNOPQRSTUVWXYZ":
                return self.reply(400, {"error": "Invalid catalog letter"})
            try:
                page = max(1, min(200, int(params.get("page", ["1"])[0])))
            except ValueError:
                return self.reply(400, {"error": "Invalid catalog page"})
            with LOCK:
                results = [item for item in CACHE["titles"] if query in item["title"].casefold()
                           and (not letter or (item["title"][:1].upper() not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                               if letter == "#" else item["title"][:1].upper() == letter))]
                meta = {"updated_at": CACHE["updated_at"], "version": CACHE["version"], "source": CACHE["source"], "error": CACHE["error"]}
            record("Website", f"Search served {len(results)} record(s) from cache")
            if letter:
                results.sort(key=lambda item: item["title"].casefold())
                start = (page - 1) * 24
                return self.reply(200, {"items": results[start:start + 24], "page": page,
                                        "total": len(results), "has_next": len(results) > start + 24, **meta})
            self.reply(200, {"items": results, **meta})
            return
        if path == "/api/discover":
            try:
                return self.reply(200, discover(parse_qs(parsed.query)))
            except Exception as exc:
                record("AniList", f"Discovery failed ({type(exc).__name__})")
                return self.reply(502, {"error": f"Catalog search unavailable: {type(exc).__name__}"})
        if path == "/api/schedule":
            try:
                return self.reply(200, upcoming_schedule(parse_qs(parsed.query)))
            except (ValueError, OverflowError):
                return self.reply(400, {"error": "Invalid schedule date or page"})
        if path == "/api/top":
            period = parse_qs(parsed.query).get("period", ["today"])[0]
            return self.reply(200, {"items": top_views(period), "period": period})
        if path.startswith("/api/series/"):
            series_id = unquote(path.removeprefix("/api/series/"))
            if not re.fullmatch(r"(?:ani:[1-9][0-9]*|[A-Za-z0-9_-]+)", series_id):
                return self.reply(400, {"error": "Invalid series ID"})
            return self.serve_series(series_id)
        if path in ("/", "/index.html", "/browse", "/watch"):
            file = ROOT / "index.html"
            body = file.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.reply(404, {"error": "Not found"})

    def do_POST(self):
        self.reply(404, {"error": "Not found"})


if __name__ == "__main__":
    threading.Thread(target=refresh, daemon=True).start()
    threading.Thread(target=scheduler, daemon=True).start()
    print("Open http://127.0.0.1:8765")
    ThreadingHTTPServer(("127.0.0.1", 8765), Handler).serve_forever()
