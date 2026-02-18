# config.py

TOP_JOURNALS = [
    "Nature",
    "Science",
    "Cell",
    "Nature Medicine",
    "The Lancet",
    "JAMA",
    "New England Journal of Medicine",
    "BMJ",
]

# Use "edat" for "added to PubMed" (best for daily ingestion)
# Use "pdat" for "published date"
DATE_TYPE = "pdat"

DAYS_BACK = 1  # last 3 days (excluding today), e.g. yesterday + 2 days before
ESEARCH_PAGE_SIZE = 500
EFETCH_BATCH_SIZE = 200

REQUEST_DELAY_SEC = 0.12
