from summarize_ru import run_pipeline


def main() -> None:
    stats = run_pipeline(limit=200)
    reasons = stats.get("fail_reasons") or {}
    reasons_str = ", ".join(f"{k}:{v}" for k, v in sorted(reasons.items())) if reasons else "none"
    print(
        "Pipeline finished | "
        f"fetched_new={stats['fetched']} | "
        f"fetched_raw={stats.get('fetched_total', 0)} | "
        f"skipped_existing={stats.get('skipped_existing', 0)} | "
        f"pending={stats['pending']} | "
        f"summarized={stats['summarized']} | "
        f"failed={stats['failed']} | "
        f"tokens_in={stats.get('tokens_input', 0)} | "
        f"tokens_out={stats.get('tokens_output', 0)} | "
        f"tokens_total={stats.get('tokens_total', 0)} | "
        f"elapsed_sec={stats.get('elapsed_sec', 0)} | "
        f"fail_reasons={reasons_str}"
    )


if __name__ == "__main__":
    main()
