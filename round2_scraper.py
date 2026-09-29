import os
import asyncio
import re
import json
import logging
import urllib.parse
import sys
from datetime import datetime, timezone
import pandas as pd
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Import established helpers, rotator, headers, and database functions from main
from main import (
    ProxyRotator,
    load_webshare_proxies,
    mask_proxy,
    HEADERS,
    clean_description,
    parse_location_details,
    parse_salary,
    parse_job_level,
    parse_experience,
    parse_emails,
    parse_h1b,
    is_within_24_hours,
    fetch_job_detail as fetch_dice_job_detail,
    save_to_neon,
    get_existing_db_job_ids,
    NEON_TABLE,
    CRM_BACKEND_URL,
    INTERNAL_SERVICE_API_KEY,
    LOCATIONS
)

logger = logging.getLogger("round2")
logger.setLevel(logging.INFO)

# Round 2 Configuration
MAX_JOBS_PER_DOMAIN = int(os.getenv("ROUND2_MAX_JOBS_PER_DOMAIN", "50"))
KEYWORD_THRESHOLD = 5  # The 5-Keyword Job Description Verification Rule
ROUND2_SOURCES = os.getenv("ROUND2_SOURCES", "all").strip().lower()  # 'dice', 'linkedin', or 'all'


def is_keyword_in_text(keyword: str, text: str) -> bool:
    """
    Boundary-aware case-insensitive keyword matcher.
    Correctly handles standard words (React, TypeScript) and technical
    symbols (C++, C#, .NET, Node.js, React.js) without false substring matches.
    """
    if not keyword or not text:
        return False
    kw_clean = str(keyword).strip()
    if not kw_clean:
        return False

    escaped_kw = re.escape(kw_clean)
    left_b = r'(?<![a-zA-Z0-9])' if kw_clean[0].isalnum() else ''
    right_b = r'(?![a-zA-Z0-9])' if kw_clean[-1].isalnum() else ''
    pattern = rf'{left_b}{escaped_kw}{right_b}'
    return bool(re.search(pattern, text, re.IGNORECASE))


def verify_5_keywords(description: str, keywords: list) -> tuple:
    """
    Checks if AT LEAST 5 keywords (>= 5) appear in the job description.
    Returns (is_qualified, matched_keywords_list).
    """
    if not description or not keywords:
        return False, []

    matched = []
    for kw in keywords:
        if is_keyword_in_text(kw, description):
            matched.append(kw)

    return (len(matched) >= KEYWORD_THRESHOLD), matched


async def fetch_active_domains_targets() -> list:
    """
    Fetches active clients from CRM endpoint: https://api.applyus.org/api/clients/active-domains
    Deduplication Rule:
    Strictly groups clients by (domain.strip().lower(), country.strip().lower()).
    If multiple clients share the same domain in the same country, merges their keywords
    and ensures each unique (domain, country) is scraped ONLY ONCE.
    """
    endpoint = f"{CRM_BACKEND_URL}/api/clients/active-domains"
    headers = {
        "x-api-key": INTERNAL_SERVICE_API_KEY,
        "Content-Type": "application/json"
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.get(endpoint, headers=headers)
            if res.status_code != 200:
                logger.error(f"CRM active-domains error HTTP {res.status_code}: {res.text[:200]}")
                return []

            result = res.json()
            items = result.get("data", result) if isinstance(result, dict) and "data" in result else result
            if not isinstance(items, list):
                items = [items]

            grouped = {}
            for item in items:
                if not isinstance(item, dict):
                    continue

                raw_domain = item.get("domain") or "Software Engineer"
                raw_country = item.get("country") or (LOCATIONS[0] if LOCATIONS else "United States")
                domain_clean = raw_domain.strip()
                country_clean = raw_country.strip()

                norm_key = (domain_clean.lower(), country_clean.lower())

                if norm_key not in grouped:
                    grouped[norm_key] = {
                        "domain": domain_clean,
                        "country": country_clean,
                        "desired_job_titles": [],
                        "keywords": [],
                        "client_names": []
                    }

                client_name = item.get("full_name") or f"{item.get('first_name', '')} {item.get('last_name', '')}".strip()
                if client_name and client_name not in grouped[norm_key]["client_names"]:
                    grouped[norm_key]["client_names"].append(client_name)

                # Merge desired_job_titles
                for title in (item.get("desired_job_titles") or []):
                    t_str = str(title).strip()
                    if t_str and not any(t_str.lower() == existing.lower() for existing in grouped[norm_key]["desired_job_titles"]):
                        grouped[norm_key]["desired_job_titles"].append(t_str)

                # Merge keywords preserving case and deduplicating
                for kw in (item.get("keywords") or []):
                    kw_str = str(kw).strip()
                    if kw_str and not any(kw_str.lower() == existing.lower() for existing in grouped[norm_key]["keywords"]):
                        grouped[norm_key]["keywords"].append(kw_str)

            targets = list(grouped.values())
            logger.info(f"✅ Ingested {len(items)} active client(s), strictly deduplicated into {len(targets)} unique (domain, country) target(s).")
            for idx, t in enumerate(targets):
                logger.info(f"   [{idx+1}] Domain: '{t['domain']}' | Country: '{t['country']}' | Clients: {t['client_names']} | Keywords: {len(t['keywords'])}")
            return targets

    except Exception as e:
        logger.error(f"Error fetching active domains from CRM: {e}")
        return []


async def search_dice_candidates(client, domain: str, country: str, keywords: list, page: int = 1) -> list:
    """
    Search Dice for candidate jobs posted in the last 24 hours.
    Combines domain with top technical keywords.
    """
    query_parts = [domain]
    for kw in keywords[:3]:
        if len(kw) <= 15 and kw.lower() not in domain.lower():
            query_parts.append(kw)

    search_query = " ".join(query_parts)
    search_url = (
        f"https://www.dice.com/jobs?"
        f"q={urllib.parse.quote_plus(search_query)}&"
        f"location={urllib.parse.quote_plus(country)}&"
        f"filters.postedDate=ONE&page={page}"
    )

    res = await client.get(search_url)
    if res.status_code != 200:
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/job-detail/" in href:
            full_url = href if href.startswith("http") else "https://www.dice.com" + href
            if full_url not in links:
                links.append(full_url)
    return links


async def search_linkedin_candidates(client, domain: str, country: str, keywords: list, start: int = 0) -> list:
    """
    Search LinkedIn Guest Job Postings for recent jobs (posted in the last 24 hours / f_TPR=r86400).
    """
    query_parts = [domain]
    for kw in keywords[:2]:
        if len(kw) <= 15 and kw.lower() not in domain.lower():
            query_parts.append(kw)

    search_query = " ".join(query_parts)
    url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    params = {
        "keywords": search_query,
        "location": country,
        "f_TPR": "r86400",
        "start": start
    }

    res = await client.get(url, params=params)
    if res.status_code != 200:
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    candidate_ids = []
    for card in soup.find_all("div", class_=re.compile(r"base-card")):
        urn = card.get("data-entity-urn", "")
        if urn and ":" in urn:
            job_id = urn.split(":")[-1].strip()
            if job_id and job_id not in candidate_ids:
                candidate_ids.append(job_id)
        else:
            link_el = card.find("a", href=True)
            if link_el:
                href = link_el["href"]
                m = re.search(r'view/(\d+)', href) or re.search(r'view/([a-zA-Z0-9-]+)', href)
                if m and m.group(1) not in candidate_ids:
                    candidate_ids.append(m.group(1))

    return candidate_ids


async def fetch_linkedin_job_detail(client, job_id: str, domain: str, country: str) -> dict:
    """
    Fetches and parses LinkedIn job detail matching public.links table schema.
    """
    url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
    res = await client.get(url)
    if res.status_code != 200:
        return None

    soup = BeautifulSoup(res.text, "html.parser")
    title_el = soup.find("h2", class_=re.compile(r"top-card-layout__title|topcard__title")) or soup.find("h1")
    title = title_el.get_text(strip=True) if title_el else "N/A"

    comp_el = soup.find("a", class_=re.compile(r"topcard__org-name-link")) or soup.find("span", class_=re.compile(r"topcard__flavor"))
    company_name = comp_el.get_text(strip=True) if comp_el else "Unknown Company"
    company_url = comp_el.get("href") if (comp_el and comp_el.get("href")) else None

    loc_el = soup.find("span", class_=re.compile(r"topcard__flavor--bullet"))
    raw_loc = loc_el.get_text(strip=True) if loc_el else country
    loc_city, loc_state, loc_country, location_display = parse_location_details(raw_loc)
    if country and (not loc_country or loc_country in ["USA", "United States"]):
        if country.lower() not in ["usa", "united states", "us"]:
            loc_country = country

    desc_el = soup.find("div", class_=re.compile(r"show-more-less-html__markup|description__text"))
    raw_desc = desc_el.decode_contents() if desc_el else ""
    description = clean_description(raw_desc)

    apply_el = soup.find("a", class_=re.compile(r"apply-button|sign-in-modal__outlet-btn"))
    job_url = f"https://www.linkedin.com/jobs/view/{job_id}"
    apply_url = apply_el.get("href") if apply_el else job_url

    now_iso = datetime.now(timezone.utc).isoformat()
    date_posted = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Parsing heuristics
    min_sal, max_sal, curr, interval, salary_text = parse_salary(description, {})
    job_level = parse_job_level(title)
    emails = parse_emails(description)
    experience = parse_experience(description)
    h1b = parse_h1b(description)
    is_remote = "remote" in title.lower() or "remote" in description[:300].lower()

    return {
        "job_id": f"li-{job_id}",
        "title": title,
        "company_name": company_name,
        "company_url": company_url,
        "company_logo": None,
        "location_city": loc_city,
        "location_state": loc_state,
        "location_country": loc_country,
        "location_display": location_display,
        "description": description,
        "date_posted": date_posted,
        "scraped_at": now_iso,
        "job_url": job_url,
        "apply_url": apply_url,
        "job_type": "FULL_TIME",
        "job_level": job_level,
        "company_industry": None,
        "job_function": domain,
        "is_remote": is_remote,
        "is_easy_apply": False,
        "compensation_min": min_sal,
        "compensation_max": max_sal,
        "compensation_currency": curr,
        "compensation_interval": interval,
        "emails": emails,
        "search_keyword": domain,
        "experience": experience,
        "salary_text": salary_text,
        "created_at": now_iso,
        "skills": None,
        "sponsorship_h1b": h1b,
        "source": "linkedin_round2"
    }


async def execute_with_proxy_or_direct(proxy_rotator: ProxyRotator, request_coro_fn):
    """
    Executes a network request with proxy rotation.
    If the Webshare proxy fails with 402 (Payment Required / Bandwidth limit) or 407 (Auth),
    it permanently removes the failing proxy and immediately falls back to Direct IP connection.
    """
    max_attempts = min(3, proxy_rotator.total()) if proxy_rotator.has_proxies() else 1
    
    for attempt in range(max_attempts):
        if not proxy_rotator.has_proxies():
            break
        client, proxy_label = await proxy_rotator.get_client()
        try:
            return await request_coro_fn(client)
        except Exception as e:
            err_str = str(e).lower()
            if "402" in err_str or "payment required" in err_str:
                proxy_rotator.remove_failing_proxy(proxy_label, "Webshare 402 Payment Required - Bandwidth/Plan Expired")
                break
            elif "407" in err_str or "authentication" in err_str:
                proxy_rotator.remove_failing_proxy(proxy_label, "Proxy 407 Auth Failed")
                break
            elif attempt < max_attempts - 1:
                logger.warning(f"Request error via {proxy_label}: {e}. Retrying with next proxy...")
                await asyncio.sleep(1)
            else:
                logger.warning(f"Proxy request failed after {max_attempts} attempts: {e}")

    # Seamless Direct IP fallback if no valid proxies remaining or all failed
    direct_client, direct_label = await proxy_rotator.get_direct_client()
    try:
        return await request_coro_fn(direct_client)
    except Exception as direct_err:
        logger.error(f"Direct connection request error: {direct_err}")
        return None


async def run_round2(existing_job_ids: set = None):
    """
    Main execution pipeline for Round 2:
    1. Ingest active clients from CRM (active-domains).
    2. Strictly group by (domain, country) and merge technical keywords.
    3. Initialize duplicate protection using existing_job_ids from Round 1 and Neon DB.
    4. Search candidate jobs on Dice and/or LinkedIn Guest.
    5. Fetch full job descriptions.
    6. The 5-Keyword Gatekeeper Rule:
       - Match >= 5 keywords: ACCEPT, set skills metadata, save to CSV and Neon DB.
       - Match < 5 keywords: DISQUALIFY & SKIP.
    7. Guarantee ZERO duplicate rows written to CSV or database.
    """
    logger.info("=" * 75)
    logger.info("⚡ STARTING ROUND 2: CLIENT KEYWORDS & 5-KEYWORD GATEKEEPER PIPELINE")
    logger.info("=" * 75)

    targets = await fetch_active_domains_targets()
    if not targets:
        logger.warning("No active domain targets found. Round 2 finished.")
        return

    limits = httpx.Limits(max_keepalive_connections=30, max_connections=50)
    webshare_proxies = load_webshare_proxies()
    proxy_rotator = ProxyRotator(webshare_proxies, headers=HEADERS, limits=limits)

    if proxy_rotator.has_proxies():
        logger.info(f"Loaded {proxy_rotator.total()} Webshare rotational proxy configuration(s). (Direct IP fallback enabled on 402/error)")
    else:
        logger.info("No proxy configured in .env. Using Direct IP connection.")

    output_csv = f"dice_jobs_round2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    all_qualified_jobs = []
    
    # Initialize seen_job_ids with Round 1 jobs + all existing DB jobs to strictly prevent duplicates
    seen_job_ids = set(existing_job_ids) if existing_job_ids else set()
    db_job_ids = get_existing_db_job_ids()
    if db_job_ids:
        logger.info(f"🛡️ Loaded {len(db_job_ids)} pre-existing job(s) from Neon DB table '{NEON_TABLE}' to prevent duplicate rows.")
        seen_job_ids.update(db_job_ids)

    written_csv_job_ids = set()

    try:
        for t_idx, target in enumerate(targets):
            domain = target["domain"]
            country = target["country"]
            keywords = target["keywords"]

            logger.info("\n" + "-" * 70)
            logger.info(f"🎯 Processing Round 2 Target [{t_idx+1}/{len(targets)}]: '{domain}' in '{country}'")
            logger.info(f"   Candidates: {target['client_names']}")
            logger.info(f"   Keyword Gatekeeper Pool: {len(keywords)} technical keywords")
            logger.info("-" * 70)

            target_qualified = 0
            scanned_jobs = 0

            # -------------------------------------------------------------
            # A. Search Candidate Jobs on Dice (if enabled)
            # -------------------------------------------------------------
            if ROUND2_SOURCES in ["all", "dice"]:
                logger.info(f"🔎 Scanning Dice search route for candidate jobs for '{domain}' in '{country}'...")
                dice_page = 1
                max_dice_pages = 3

                while dice_page <= max_dice_pages and target_qualified < MAX_JOBS_PER_DOMAIN:
                    dice_links = await execute_with_proxy_or_direct(
                        proxy_rotator,
                        lambda c: search_dice_candidates(c, domain, country, keywords, page=dice_page)
                    )
                    if not dice_links:
                        break

                    logger.info(f"   [Dice Page {dice_page}] Found {len(dice_links)} candidate job links.")
                    
                    # Fetch and verify each candidate job
                    for link in dice_links:
                        scanned_jobs += 1
                        m_jid = re.search(r'/job-detail/([a-f0-9-]+)', link)
                        raw_jid = m_jid.group(1) if m_jid else link
                        if raw_jid in seen_job_ids:
                            continue

                        # Detail fetch with proxy rotation or direct IP fallback
                        job_record = await execute_with_proxy_or_direct(
                            proxy_rotator,
                            lambda c: fetch_dice_job_detail(c, link, search_keyword=domain, search_country=country)
                        )

                        if not job_record:
                            continue

                        jid = job_record.get("job_id", raw_jid)
                        if jid in seen_job_ids:
                            continue

                        # 5-Keyword Gatekeeper Verification
                        desc = job_record.get("description", "")
                        is_qual, matched_kws = verify_5_keywords(desc, keywords)

                        if is_qual:
                            target_qualified += 1
                            seen_job_ids.add(jid)
                            job_record["source"] = "dice_round2"
                            job_record["search_keyword"] = domain
                            job_record["skills"] = f"Matched ({len(matched_kws)}): {', '.join(matched_kws)}"
                            all_qualified_jobs.append(job_record)

                            # Strictly check that this job_id hasn't been written to CSV yet
                            if jid not in written_csv_job_ids:
                                written_csv_job_ids.add(jid)
                                df_temp = pd.DataFrame([job_record])
                                df_temp.to_csv(output_csv, mode="a", header=not os.path.exists(output_csv), index=False)

                            logger.info(f"   ✅ [QUALIFIED] [{job_record['job_id']}] '{job_record['title']}' @ {job_record['company_name']} -> {job_record['skills']}")
                        else:
                            logger.info(f"   ❌ [DISQUALIFIED] [{job_record['job_id']}] '{job_record['title']}': matched {len(matched_kws)}/5 keywords ({matched_kws}). Skipped.")

                        if target_qualified >= MAX_JOBS_PER_DOMAIN:
                            break

                    dice_page += 1
                    await asyncio.sleep(2)

            # -------------------------------------------------------------
            # B. Search Candidate Jobs on LinkedIn Guest (if enabled)
            # -------------------------------------------------------------
            if ROUND2_SOURCES in ["all", "linkedin"] and target_qualified < MAX_JOBS_PER_DOMAIN:
                logger.info(f"🔎 Scanning LinkedIn Guest search for candidate jobs for '{domain}' in '{country}'...")
                li_start = 0
                max_li_start = 50

                while li_start <= max_li_start and target_qualified < MAX_JOBS_PER_DOMAIN:
                    li_job_ids = await execute_with_proxy_or_direct(
                        proxy_rotator,
                        lambda c: search_linkedin_candidates(c, domain, country, keywords, start=li_start)
                    )
                    if not li_job_ids:
                        break

                    logger.info(f"   [LinkedIn Start={li_start}] Found {len(li_job_ids)} candidate job postings.")

                    for jid in li_job_ids:
                        scanned_jobs += 1
                        full_jid = f"li-{jid}"
                        if full_jid in seen_job_ids:
                            continue

                        job_record = await execute_with_proxy_or_direct(
                            proxy_rotator,
                            lambda c: fetch_linkedin_job_detail(c, jid, domain, country)
                        )
                        if not job_record:
                            continue

                        real_jid = job_record.get("job_id", full_jid)
                        if real_jid in seen_job_ids:
                            continue

                        # 5-Keyword Gatekeeper Verification
                        desc = job_record.get("description", "")
                        is_qual, matched_kws = verify_5_keywords(desc, keywords)

                        if is_qual:
                            target_qualified += 1
                            seen_job_ids.add(real_jid)
                            job_record["source"] = "linkedin_round2"
                            job_record["search_keyword"] = domain
                            job_record["skills"] = f"Matched ({len(matched_kws)}): {', '.join(matched_kws)}"
                            all_qualified_jobs.append(job_record)

                            # Strictly check that this job_id hasn't been written to CSV yet
                            if real_jid not in written_csv_job_ids:
                                written_csv_job_ids.add(real_jid)
                                df_temp = pd.DataFrame([job_record])
                                df_temp.to_csv(output_csv, mode="a", header=not os.path.exists(output_csv), index=False)

                            logger.info(f"   ✅ [QUALIFIED] [{job_record['job_id']}] '{job_record['title']}' @ {job_record['company_name']} -> {job_record['skills']}")
                        else:
                            logger.info(f"   ❌ [DISQUALIFIED] [{job_record['job_id']}] '{job_record['title']}': matched {len(matched_kws)}/5 keywords ({matched_kws}). Skipped.")

                        if target_qualified >= MAX_JOBS_PER_DOMAIN:
                            break

                    li_start += 25
                    await asyncio.sleep(2)

            logger.info(f"🏁 Finished Target '{domain}' in '{country}': Scanned {scanned_jobs} candidates, {target_qualified} qualified.")

            # Cooldown between targets
            if t_idx < len(targets) - 1:
                await asyncio.sleep(3)

    finally:
        await proxy_rotator.close_all()

    # Final Persistence Phase for Round 2: Guarantee ZERO duplicate rows
    if all_qualified_jobs:
        # Strictly deduplicate in-memory by job_id
        df_all = pd.DataFrame(all_qualified_jobs)
        df_all = df_all.drop_duplicates(subset=["job_id"], keep="last").copy()
        df_all.to_csv(output_csv, index=False)
        logger.info(f"\n✅ ROUND 2 CSV COMPLETE: {len(df_all)} unique qualified jobs stored in '{output_csv}'")

        deduped_records = df_all.to_dict(orient="records")
        logger.info(f"🚀 Upserting all {len(deduped_records)} unique Round 2 verified jobs into Neon DB table '{NEON_TABLE}'...")
        await save_to_neon(deduped_records)
        logger.info(f"🎉 Round 2 database persistence completed successfully!")
    else:
        logger.info("ℹ️ Round 2 finished. No jobs met the 5-keyword gatekeeper requirement.")


if __name__ == "__main__":
    asyncio.run(run_round2())
