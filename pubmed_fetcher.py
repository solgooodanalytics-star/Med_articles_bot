import os
import re
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args, **_kwargs):
        return False

import config

BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
PUBMED_URL = "https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
DOI_URL = "https://doi.org/{doi}"
MONTH_TO_NUM = {
    "jan": "01",
    "feb": "02",
    "mar": "03",
    "apr": "04",
    "may": "05",
    "jun": "06",
    "jul": "07",
    "aug": "08",
    "sep": "09",
    "sept": "09",
    "oct": "10",
    "nov": "11",
    "dec": "12",
}


def _get_api_key() -> str | None:
    load_dotenv()
    return os.getenv("PUBMED_API_KEY")


def _date_range_last_days(days_back: int) -> tuple[str, str]:
    """
    Returns (mindate, maxdate) in YYYY/MM/DD, excluding today in Europe/Paris.
    For days_back=3 on Feb 18 -> mindate=Feb 15, maxdate=Feb 17
    """
    tz = ZoneInfo("Europe/Paris")
    today = datetime.now(tz).date()
    max_day = today - timedelta(days=1)
    min_day = today - timedelta(days=days_back)
    return min_day.strftime("%Y/%m/%d"), max_day.strftime("%Y/%m/%d")


def _build_journal_query(journals: list[str]) -> str:
    parts = [f'"{j}"[jour]' for j in journals]
    return "(" + " OR ".join(parts) + ")"


def _normalize_year(value: str | None) -> str | None:
    if not value:
        return None
    m = re.search(r"\d{4}", value.strip())
    return m.group(0) if m else None


def _normalize_month(value: str | None) -> str | None:
    if not value:
        return None

    raw = value.strip()
    if raw.isdigit():
        n = int(raw)
        return f"{n:02d}" if 1 <= n <= 12 else None

    key = raw.lower()[:4].rstrip(".")
    if key in MONTH_TO_NUM:
        return MONTH_TO_NUM[key]

    key3 = raw.lower()[:3]
    if key3 in MONTH_TO_NUM:
        return MONTH_TO_NUM[key3]

    return None


def _normalize_day(value: str | None) -> str | None:
    if not value:
        return None
    m = re.search(r"\d{1,2}", value.strip())
    if not m:
        return None
    n = int(m.group(0))
    return f"{n:02d}" if 1 <= n <= 31 else None


def _normalize_medline_date(value: str | None) -> str | None:
    if not value:
        return None

    text = value.strip()
    year = _normalize_year(text)
    if not year:
        return None

    month_match = re.search(r"\b([A-Za-z]{3,9})\b", text)
    if not month_match:
        return year

    month = _normalize_month(month_match.group(1))
    if not month:
        return year

    day_match = re.search(r"\b(\d{1,2})\b", text[month_match.end():])
    day = _normalize_day(day_match.group(1)) if day_match else None
    return f"{year}-{month}-{day}" if day else f"{year}-{month}"


def _esearch_all_pmids(term: str, mindate: str, maxdate: str, datetype: str, api_key: str | None) -> list[str]:
    pmids: list[str] = []
    retstart = 0

    while True:
        params = {
            "db": "pubmed",
            "term": term,
            "retmode": "json",
            "retmax": config.ESEARCH_PAGE_SIZE,
            "retstart": retstart,
            "datetype": datetype,
            "mindate": mindate,
            "maxdate": maxdate,
        }
        if api_key:
            params["api_key"] = api_key

        r = requests.get(BASE_URL + "esearch.fcgi", params=params, timeout=(5, 40))
        r.raise_for_status()
        data = r.json()["esearchresult"]

        count = int(data.get("count", "0"))
        batch = data.get("idlist", [])

        pmids.extend(batch)
        retstart += len(batch)

        time.sleep(config.REQUEST_DELAY_SEC)

        if retstart >= count or not batch:
            break

    return pmids


def _chunk(lst: list[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _parse_abstract(pubmed_article: ET.Element) -> str | None:
    parts = []
    for a in pubmed_article.findall(".//Article/Abstract/AbstractText"):
        label = a.attrib.get("Label")
        txt = "".join(a.itertext()).strip() if a is not None else ""
        if txt:
            parts.append(f"{label}: {txt}" if label else txt)
    return "\n".join(parts) if parts else None


def _extract_date(pubmed_article: ET.Element) -> str | None:
    """
    Best-effort full date string.
    Prefers ArticleDate (often Y/M/D), else Journal PubDate.
    Output examples: '2026-02-17', '2026-02', '2026'
    """
    ad = pubmed_article.find(".//ArticleDate")
    if ad is not None:
        y = _normalize_year(ad.findtext("Year"))
        m = _normalize_month(ad.findtext("Month"))
        d = _normalize_day(ad.findtext("Day"))
        if y and m and d:
            return f"{y}-{m}-{d}"
        if y and m:
            return f"{y}-{m}"
        if y:
            return y

    pd = pubmed_article.find(".//JournalIssue/PubDate")
    if pd is not None:
        y = _normalize_year(pd.findtext("Year"))
        m = _normalize_month(pd.findtext("Month"))
        d = _normalize_day(pd.findtext("Day"))
        medline = pd.findtext("MedlineDate")
        if y and m and d:
            return f"{y}-{m}-{d}"
        if y and m:
            return f"{y}-{m}"
        if y:
            return y
        if medline:
            return _normalize_medline_date(medline) or medline

    return None


def _parse_article(pubmed_article: ET.Element) -> dict:
    pmid = pubmed_article.findtext(".//MedlineCitation/PMID")
    title = pubmed_article.findtext(".//Article/ArticleTitle")
    journal = pubmed_article.findtext(".//Article/Journal/Title")
    date = _extract_date(pubmed_article)
    abstract = _parse_abstract(pubmed_article)

    authors = []
    for au in pubmed_article.findall(".//Article/AuthorList/Author"):
        fore = au.findtext("ForeName")
        last = au.findtext("LastName")
        name = " ".join([x for x in [fore, last] if x]) or None
        if name:
            authors.append(name)

    doi = None
    for aid in pubmed_article.findall(".//PubmedData/ArticleIdList/ArticleId"):
        if aid.attrib.get("IdType") == "doi":
            val = "".join(aid.itertext()).strip()
            if val:
                doi = val
                break

    return {
        "pmid": pmid,
        "title": title,
        "journal": journal,
        "publication_date": date,
        "authors": authors,
        "abstract": abstract,
        "doi": doi,
        "links": {
            "pubmed": PUBMED_URL.format(pmid=pmid) if pmid else None,
            "doi": DOI_URL.format(doi=doi) if doi else None,
        },
    }


def _efetch_articles(pmids: list[str], api_key: str | None) -> list[dict]:
    results: list[dict] = []

    for batch in _chunk(pmids, config.EFETCH_BATCH_SIZE):
        params = {
            "db": "pubmed",
            "id": ",".join(batch),
            "retmode": "xml",
        }
        if api_key:
            params["api_key"] = api_key

        r = requests.get(BASE_URL + "efetch.fcgi", params=params, timeout=(5, 60))
        r.raise_for_status()

        root = ET.fromstring(r.text)
        for art in root.findall(".//PubmedArticle"):
            results.append(_parse_article(art))

        time.sleep(config.REQUEST_DELAY_SEC)

    return results


def fetch_top_journal_articles_last_days(days_back: int | None = None) -> list[dict]:
    """
    Fetch articles from TOP_JOURNALS for the last N days (excluding today),
    keeping ONLY those with a PubMed abstract.
    """
    days = days_back if days_back is not None else config.DAYS_BACK
    api_key = _get_api_key()

    mindate, maxdate = _date_range_last_days(days)
    term = _build_journal_query(config.TOP_JOURNALS)

    pmids = _esearch_all_pmids(
        term=term,
        mindate=mindate,
        maxdate=maxdate,
        datetype=config.DATE_TYPE,
        api_key=api_key,
    )

    if not pmids:
        return []

    articles = _efetch_articles(pmids, api_key=api_key)

    # KEEP ONLY articles with abstracts
    articles = [a for a in articles if a.get("abstract")]

    return articles
