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

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

# Load environment variables from .env (checks script directory and current working directory)
_script_dir = os.path.dirname(os.path.abspath(__file__))
_env_path = os.path.join(_script_dir, ".env")
if os.path.exists(_env_path):
    load_dotenv(dotenv_path=_env_path)
load_dotenv()

# Ensure stdout supports UTF-8 on Windows consoles
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# Configure logging with real-time unbuffered flushing (essential for Google Colab and consoles)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Constants & Configuration
SEARCH_TERMS = ["software engineer"]
LOCATIONS = ["San Francisco, CA"]
# Scrape limit per keyword: defaults to 150 (configurable via MAX_JOBS_PER_KEYWORD or MAX_JOBS env var)
_env_max = os.getenv("MAX_JOBS_PER_KEYWORD", os.getenv("MAX_JOBS", "150")).strip()
MAX_JOBS_PER_KEYWORD = int(_env_max) if _env_max.isdigit() and int(_env_max) > 0 else (None if _env_max.lower() in ["none", "0", "unlimited"] else 150)
MAX_JOBS = MAX_JOBS_PER_KEYWORD

# Max active client members to process from CRM: configurable via MAX_MEMBERS env var (defaults to 15)
_env_members = os.getenv("MAX_MEMBERS", "15").strip()
MAX_MEMBERS = int(_env_members) if _env_members.isdigit() and int(_env_members) > 0 else None

# Max search keywords to scrape: None means scrape all keywords (configurable via MAX_KEYWORDS env var)
_env_keywords = os.getenv("MAX_KEYWORDS", "").strip()
MAX_KEYWORDS = int(_env_keywords) if _env_keywords.isdigit() and int(_env_keywords) > 0 else None

# Filter for jobs posted or updated within the last 24 hours only (defaults to True)
_env_24h = os.getenv("ONLY_LAST_24_HOURS", "true").strip().lower()
ONLY_LAST_24_HOURS = _env_24h not in ["false", "0", "no"]
OUTPUT_FILE = f"dice_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# 1. SOURCE API: Fetch active client desired job titles from ApplyUS CRM API
CRM_BACKEND_URL = os.getenv("CRM_BACKEND_URL", "https://api.applyus.org").rstrip("/")
INTERNAL_SERVICE_API_KEY = os.getenv("INTERNAL_SERVICE_API_KEY") or os.getenv("CRM_API_KEY", "my_secure_applyus_secret_2026")

# Fallback: Source Database (Supabase)
SOURCE_SUPABASE_URL = os.getenv("SOURCE_SUPABASE_URL", "").rstrip("/")
SOURCE_SUPABASE_KEY = os.getenv("SOURCE_SUPABASE_KEY", "")
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "onboarding_submissions")

# 2. DESTINATION DATABASE (NEON DB): Store scraped jobs / links in Neon PostgreSQL
NEON_DATABASE_URL = (
    os.getenv("NEON_DATABASE_URL")
    or os.getenv("DATABASE_URL")
    or os.getenv("DEST_DATABASE_URL", "")
).strip()
NEON_TABLE = os.getenv("NEON_TABLE") or os.getenv("DEST_TABLE", "links")

# Legacy Supabase Destination Settings (kept for fallback)
DEST_SUPABASE_URL = (os.getenv("DEST_SUPABASE_URL") or os.getenv("SUPABASE_URL", "")).rstrip("/")
DEST_SUPABASE_KEY = os.getenv("DEST_SUPABASE_KEY") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
DEST_TABLE = os.getenv("DEST_TABLE", "links")

# ==============================================================================
# 3. WEBSHARE ROTATIONAL PROXY CONFIGURATION & HELPERS
# ==============================================================================
def normalize_proxy(p: str) -> str:
    """Normalize raw proxy string to http://user:pass@host:port format."""
    p = p.strip().strip("'\"")
    if not p:
        return ""
    if p.startswith("http://") or p.startswith("https://") or p.startswith("socks5://"):
        return p
    parts = p.split(":")
    if len(parts) == 4:
        # host:port:username:password (common Webshare export)
        host, port, user, pwd = parts
        return f"http://{user}:{pwd}@{host}:{port}"
    if "@" in p:
        return f"http://{p}"
    return f"http://{p}"

def mask_proxy(proxy_url: str) -> str:
    """Mask proxy credentials for safe logging."""
    if not proxy_url:
        return "Direct (No Proxy)"
    try:
        parsed = urllib.parse.urlparse(proxy_url)
        if parsed.password:
            return proxy_url.replace(f":{parsed.password}@", ":****@")
        return proxy_url
    except Exception:
        return proxy_url

def load_webshare_proxies() -> list:
    """Load and normalize proxies from environment variables."""
    raw_list = []
    
    # Check WEBSHARE_PROXIES / PROXIES (comma or newline separated)
    multi_env = os.getenv("WEBSHARE_PROXIES") or os.getenv("PROXIES", "")
    if multi_env:
        for p in re.split(r'[\r\n,;]+', multi_env):
            p = p.strip()
            if p:
                raw_list.append(p)

    # Check WEBSHARE_PROXY / PROXY_URL / HTTP_PROXY
    single_env = os.getenv("WEBSHARE_PROXY") or os.getenv("PROXY_URL") or os.getenv("HTTP_PROXY", "")
    if single_env.strip():
        for p in re.split(r'[\r\n,;]+', single_env):
            p = p.strip()
            if p and p not in raw_list:
                raw_list.append(p)

    # Check individual credentials
    w_user = os.getenv("WEBSHARE_USERNAME", "").strip()
    w_pass = os.getenv("WEBSHARE_PASSWORD", "").strip()
    w_host = os.getenv("WEBSHARE_HOST", "p.webshare.io").strip()
    w_port = os.getenv("WEBSHARE_PORT", "80").strip()
    if w_user and w_pass:
        proxy_str = f"http://{w_user}:{w_pass}@{w_host}:{w_port}"
        if proxy_str not in raw_list:
            raw_list.append(proxy_str)

    # Check PROXY_FILE (optional text file)
    proxy_file = os.getenv("PROXY_FILE", "").strip()
    if proxy_file and os.path.exists(proxy_file):
        try:
            with open(proxy_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        raw_list.append(line)
        except Exception as e:
            logger.warning(f"Error reading PROXY_FILE '{proxy_file}': {e}")

    normalized = []
    for item in raw_list:
        norm = normalize_proxy(item)
        if norm and norm not in normalized:
            normalized.append(norm)

    return normalized

class ProxyRotator:
    """Manages Webshare proxies with round-robin rotation, client pooling, and safe fallback."""
    def __init__(self, proxies: list, headers: dict, limits: httpx.Limits):
        self.proxies = list(proxies)
        self.index = 0
        self.headers = headers
        self.limits = limits
        self._clients = {}
        self._direct_client = None

    def has_proxies(self) -> bool:
        return bool(self.proxies)

    def total(self) -> int:
        return len(self.proxies)

    async def get_direct_client(self):
        """Returns direct connection client (no proxy)."""
        if self._direct_client is None or self._direct_client.is_closed:
            self._direct_client = httpx.AsyncClient(
                headers=self.headers,
                limits=self.limits,
                follow_redirects=True,
                timeout=30.0
            )
        return self._direct_client, "Direct (No Proxy)"

    def remove_failing_proxy(self, proxy_label: str, reason: str = ""):
        """Permanently remove an exhausted or dead proxy from rotation and fallback to direct."""
        to_remove = []
        for p in self.proxies:
            if mask_proxy(p) == proxy_label or proxy_label in mask_proxy(p) or p in proxy_label:
                to_remove.append(p)
        for p in to_remove:
            if p in self.proxies:
                self.proxies.remove(p)
            if p in self._clients:
                try:
                    client = self._clients.pop(p)
                    asyncio.create_task(client.aclose())
                except Exception:
                    pass
            logger.warning(f"⚠️ Removed proxy {mask_proxy(p)} from rotation ({reason}). {len(self.proxies)} proxy(ies) remaining.")
        if not self.proxies:
            logger.warning("⚠️ No valid proxies remaining in rotation. Automatically switching to Direct Connection (No Proxy)!")

    async def get_client(self):
        """Returns (client, proxy_label) with round-robin rotation."""
        if not self.proxies:
            return await self.get_direct_client()

        proxy = self.proxies[self.index % len(self.proxies)]
        self.index += 1

        if proxy not in self._clients or self._clients[proxy].is_closed:
            self._clients[proxy] = httpx.AsyncClient(
                proxy=proxy,
                headers=self.headers,
                limits=self.limits,
                follow_redirects=True,
                timeout=30.0
            )

        return self._clients[proxy], mask_proxy(proxy)

    async def close_all(self):
        """Cleanly close all proxy and direct clients."""
        if self._direct_client and not self._direct_client.is_closed:
            try:
                await self._direct_client.aclose()
            except Exception:
                pass
        for client in list(self._clients.values()):
            if client and not client.is_closed:
                try:
                    await client.aclose()
                except Exception:
                    pass
        self._clients.clear()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1"
}

def parse_salary(text, json_ld):
    """Extract compensation details and text."""
    min_sal = None
    max_sal = None
    currency = "USD"
    interval = "yearly"
    salary_str = None
    
    # 1. Try structured baseSalary in JSON-LD
    if "baseSalary" in json_ld and isinstance(json_ld["baseSalary"], dict):
        bs = json_ld["baseSalary"]
        currency = bs.get("currency", "USD")
        val = bs.get("value", {})
        if isinstance(val, dict):
            min_sal = val.get("minValue")
            max_sal = val.get("maxValue") or val.get("value")
            unit = val.get("unitText", "").lower()
            if "hour" in unit:
                interval = "hourly"
            elif "month" in unit:
                interval = "monthly"
            elif "year" in unit:
                interval = "yearly"
        elif isinstance(val, (int, float)):
            min_sal = float(val)
            max_sal = float(val)

    # 2. Regex fallback on description text
    if text:
        sal_match = re.search(r'(\$\s*\d{1,3}(?:,\d{3})*(?:\.\d+)?(?:\s*[kK])?)\s*(?:-|to)\s*(\$\s*\d{1,3}(?:,\d{3})*(?:\.\d+)?(?:\s*[kK])?)\s*(?:(?:per|\/)\s*(hour|hr|year|yr|annum|month|mo))?', text)
        if sal_match:
            salary_str = sal_match.group(0).strip()
            if min_sal is None:
                try:
                    v1_raw = sal_match.group(1).replace('$', '').replace(',', '').strip()
                    v2_raw = sal_match.group(2).replace('$', '').replace(',', '').strip()
                    mult1 = 1000 if 'k' in v1_raw.lower() else 1
                    mult2 = 1000 if 'k' in v2_raw.lower() else 1
                    min_sal = float(re.sub(r'[^\d.]', '', v1_raw)) * mult1
                    max_sal = float(re.sub(r'[^\d.]', '', v2_raw)) * mult2
                except:
                    pass
            if sal_match.group(3):
                u = sal_match.group(3).lower()
                if u in ['hour', 'hr']:
                    interval = 'hourly'
                elif u in ['month', 'mo']:
                    interval = 'monthly'
                elif u in ['year', 'yr', 'annum']:
                    interval = 'yearly'
                    
    return min_sal, max_sal, currency, interval, salary_str

def parse_job_level(title):
    """Infer job seniority level from title."""
    t_lower = title.lower()
    if any(w in t_lower for w in ['principal', 'distinguished']):
        return 'Principal'
    if any(w in t_lower for w in ['staff']):
        return 'Staff'
    if any(w in t_lower for w in ['lead', 'tech lead']):
        return 'Lead'
    if any(w in t_lower for w in ['director', 'vp', 'head']):
        return 'Director'
    if any(w in t_lower for w in ['senior', 'sr.', 'sr ', 'iii', 'iv', 'v']):
        return 'Senior'
    if any(w in t_lower for w in ['entry', 'junior', 'jr.', 'jr ', 'associate', 'intern']):
        return 'Entry Level'
    return 'Mid Level'

def parse_experience(text):
    """Extract years of experience from description text."""
    if not text:
        return None
    match = re.search(r'(\d+\+?\s*(?:-\s*\d+)?\s*(?:years?|yrs?)(?:\s+of)?\s+(?:experience|exp))', text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None

def parse_emails(text):
    """Extract email addresses from description."""
    if not text:
        return None
    emails = re.findall(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', text)
    valid = [e for e in set(emails) if not any(x in e.lower() for x in ['example.com', 'sentry.io', 'domain.com', 'w3.org'])]
    return ', '.join(valid) if valid else None

def parse_location_details(raw_display):
    """
    Extract city, state, country and generate a unified polished location_display
    combining location and country into a single field.
    """
    city, state, country = None, None, "USA"
    if not raw_display or raw_display == "N/A":
        return city, state, country, "USA"
    
    clean_disp = raw_display.strip()
    
    if "Remote" in clean_disp:
        city = "Remote"
        state = "Remote"
        country = "USA"
        display_combined = "Remote, USA"
        return city, state, country, display_combined
    
    parts = [p.strip() for p in clean_disp.split(',') if p.strip()]
    if len(parts) >= 3:
        city = parts[0]
        state = parts[1]
        country = parts[2]
        display_combined = clean_disp
    elif len(parts) == 2:
        city = parts[0]
        state = parts[1]
        country = "USA"
        display_combined = f"{city}, {state}, {country}"
    elif len(parts) == 1:
        city = parts[0]
        country = "USA"
        display_combined = f"{city}, {country}"
    else:
        display_combined = f"{clean_disp}, USA"
        
    return city, state, country, display_combined

def parse_h1b(text):
    """Detect H1B sponsorship mentions."""
    if not text:
        return "Not Specified"
    if re.search(r'\b(h1b|h-1b|visa sponsorship|sponsorship)\b', text, re.IGNORECASE):
        if re.search(r'\b(no|not|cannot|unable to|without)\s+(?:provide\s+)?(?:sponsor|h1b|h-1b|visa)', text, re.IGNORECASE):
            return "No Sponsorship"
        return "Sponsorship Available"
    return "Not Specified"

def clean_description(raw_html):
    """Convert raw HTML description into clean, readable plain text."""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, 'html.parser')
    for tag in soup(['script', 'style', 'noscript', 'img', 'svg', 'iframe']):
        tag.decompose()
    for li in soup.find_all('li'):
        li.insert_before('\n- ')
    for el in soup.find_all(['br', 'p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'tr']):
        el.append('\n')
    text = soup.get_text()
    text = text.replace('\xa0', ' ').replace('\r', '')
    lines = [line.strip() for line in text.split('\n')]
    cleaned = '\n'.join(lines)
    return re.sub(r'\n{3,}', '\n\n', cleaned).strip()

def is_within_24_hours(posted_text, updated_text, json_ld_posted_iso=None):
    """
    Returns (True, reason) if either:
    1. The job was posted within the last 24 hours.
    2. The job was posted earlier but updated within the last 24 hours.
    Returns (False, reason) if both posted and updated are older than 24 hours.
    """
    def check_relative(text):
        if not text:
            return None
        t = text.lower().strip()
        # Definite recent markers (< 24 hours)
        if any(w in t for w in ['just now', 'today', 'moments ago', 'minute', 'min', 'second', 'sec']):
            return True
        # Hour markers (e.g. "5 hours ago", "22 hours ago")
        if 'hour' in t or 'hr' in t:
            m = re.search(r'(\d+)\s*(?:hour|hr)', t)
            if m:
                return int(m.group(1)) <= 24
            return True
        # Explicit days / weeks / months / years ago (> 24 hours)
        if any(w in t for w in ['day', 'week', 'month', 'year']):
            return False
        return None

    # 1. If updated text indicates within 24 hours, immediately accept!
    u_fresh = check_relative(updated_text)
    if u_fresh is True:
        return True, f"Updated within last 24h: '{updated_text}'"

    # 2. If posted text indicates within 24 hours, accept!
    p_fresh = check_relative(posted_text)
    if p_fresh is True:
        return True, f"Posted within last 24h: '{posted_text}'"

    # 3. Check exact ISO timestamp from JSON-LD if available
    if json_ld_posted_iso:
        try:
            iso_clean = json_ld_posted_iso.replace('Z', '+00:00')
            dt = datetime.fromisoformat(iso_clean)
            now = datetime.now(timezone.utc)
            delta = now - dt
            if delta.total_seconds() <= 86400: # 24 hours
                return True, f"Posted {delta.total_seconds()/3600:.1f}h ago (ISO timestamp)"
            else:
                # If posted is > 24 hours ago, and updated text is NOT fresh:
                if u_fresh is not True:
                    return False, f"Posted {delta.days} day(s) ago and not updated in last 24h"
        except Exception:
            pass

    # 4. If updated text is definitely > 24 hours (e.g. "5 days ago", "2 days ago"):
    if u_fresh is False:
        return False, f"Both posted and updated > 24h ago (Updated: '{updated_text}')"

    # 5. If posted text is definitely > 24 hours and no fresh update was found:
    if p_fresh is False:
        return False, f"Posted > 24h ago ('{posted_text}') and no update within 24h"

    # If completely indeterminate, allow by default
    return True, "Indeterminate date, allowed by default"

async def fetch_job_detail(client, job_url, search_keyword, search_country=None):
    """Fetch and parse all job fields directly from job-detail route."""
    try:
        res = await client.get(job_url)
        if res.status_code != 200:
            logger.warning(f"Failed to fetch {job_url} - HTTP {res.status_code}")
            return None
        
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 1. Job ID
        job_id_match = re.search(r'/job-detail/([a-f0-9-]+)', job_url)
        job_id = job_id_match.group(1) if job_id_match else job_url
        
        # 2. JSON-LD structured data
        ld_script = soup.find('script', {'type': 'application/ld+json'})
        json_ld = {}
        if ld_script and ld_script.string:
            try:
                json_ld = json.loads(ld_script.string)
            except:
                pass
        
        # Title
        title = json_ld.get('title')
        if not title:
            h1 = soup.find('h1')
            title = h1.get_text(strip=True) if h1 else 'N/A'
            
        # Company Info
        hiring_org = json_ld.get('hiringOrganization', {})
        company_name = None
        company_url = None
        company_logo = None
        if isinstance(hiring_org, dict):
            company_name = hiring_org.get('name')
            company_url = hiring_org.get('sameAs')
            company_logo = hiring_org.get('logo')
            
        if not company_name:
            comp_link = soup.find('a', href=re.compile(r'/company-profile/|companyname='))
            if comp_link:
                company_name = comp_link.get_text(strip=True)
                if not company_url and comp_link.get('href'):
                    href = comp_link['href']
                    company_url = href if href.startswith('http') else 'https://www.dice.com' + href
        
        # Description
        raw_description = json_ld.get('description')
        if not raw_description:
            desc_el = soup.find('div', class_=re.compile(r'job-description|description|job-details'))
            raw_description = desc_el.decode_contents() if desc_el else ''
            
        description = clean_description(raw_description)
        clean_desc_text = description
        
        # Location Parsing (combines location_display & location_country into single polished field)
        is_remote = (
            json_ld.get('jobLocationType') == 'TELECOMMUTE' or
            'remote' in title.lower() or
            'remote' in clean_desc_text[:300].lower()
        )
        
        raw_loc = "Remote" if is_remote else "San Francisco, CA"
        loc_el = soup.find('p', class_=re.compile(r'text-foreground-light'))
        if loc_el and loc_el.get_text(strip=True):
            raw_loc = loc_el.get_text(strip=True).split('\n')[0].strip()
        elif soup.find('a', href=re.compile(r'location=')):
            loc_a = soup.find('a', href=re.compile(r'location='))
            m = re.search(r'location=([^&]+)', loc_a.get('href', ''))
            if m:
                raw_loc = urllib.parse.unquote_plus(m.group(1))
                
        loc_city, loc_state, loc_country, location_display = parse_location_details(raw_loc)
        if search_country and (not loc_country or loc_country in ["USA", "United States"]):
            if search_country.lower() not in ["usa", "united states", "us"]:
                loc_country = search_country
        
        # Apply URL
        apply_url = job_url
        apply_a = soup.find('a', href=re.compile(r'http'), string=re.compile(r'Apply', re.I))
        if apply_a and 'dice.com' not in apply_a.get('href', ''):
            apply_url = apply_a['href']
            
        # Easy Apply detection
        is_easy_apply = bool(soup.find(string=re.compile(r'Easy Apply', re.I)))
        
        # Dates & Timestamps
        date_posted_raw = json_ld.get('datePosted')
        date_posted = date_posted_raw[:10] if date_posted_raw else datetime.now(timezone.utc).strftime('%Y-%m-%d')
        now_iso = datetime.now(timezone.utc).isoformat()
        
        # Extract relative posted and updated text for 24-hour freshness verification
        posted_str = None
        updated_str = None
        for span in soup.find_all('span'):
            t = span.get_text(strip=True)
            if 'posted' in t.lower() and ('ago' in t.lower() or 'today' in t.lower() or 'yesterday' in t.lower()):
                m = re.search(r'posted\s*([^•·|,\n]+)', t, re.I)
                if m:
                    posted_str = m.group(1).strip()
            if 'updated' in t.lower() and ('ago' in t.lower() or 'today' in t.lower() or 'yesterday' in t.lower()):
                m = re.search(r'updated\s*([^•·|,\n]+)', t, re.I)
                if m:
                    updated_str = m.group(1).strip()

        # Strict 24-Hour Freshness Filter
        if ONLY_LAST_24_HOURS:
            is_fresh, freshness_reason = is_within_24_hours(posted_str, updated_str, date_posted_raw)
            if not is_fresh:
                logger.info(f"⏭️ Skipping stale job [{job_id}] '{title}': {freshness_reason} (Posted: '{posted_str or date_posted}', Updated: '{updated_str or 'None'}')")
                return None
        
        # Job Type & Level
        job_type = json_ld.get('employmentType', 'FULL_TIME')
        job_level = parse_job_level(title)
        
        # Salary
        min_sal, max_sal, curr, interval, salary_text = parse_salary(clean_desc_text, json_ld)
        
        # Emails, Experience, Sponsorship
        emails = parse_emails(clean_desc_text)
        experience = parse_experience(clean_desc_text)
        h1b = parse_h1b(clean_desc_text)
        
        # Skills
        skills_list = []
        for skill_badge in soup.find_all(['span', 'div', 'li'], class_=re.compile(r'skill|chip|badge|tag')):
            st = skill_badge.get_text(strip=True)
            if st and len(st) < 30 and st not in skills_list:
                skills_list.append(st)
        skills_str = ', '.join(skills_list[:15]) if skills_list else None
        
        # Build complete schema dictionary matching public.links
        record = {
            "job_id": job_id,
            "title": title,
            "company_name": company_name,
            "company_url": company_url,
            "company_logo": company_logo,
            "location_city": loc_city,
            "location_state": loc_state,
            "location_country": loc_country,
            "location_display": location_display,
            "description": description,
            "date_posted": date_posted,
            "scraped_at": now_iso,
            "job_url": job_url,
            "apply_url": apply_url,
            "job_type": job_type,
            "job_level": job_level,
            "company_industry": None,
            "job_function": search_keyword if search_keyword else "Software Engineering",
            "is_remote": is_remote,
            "is_easy_apply": is_easy_apply,
            "compensation_min": min_sal,
            "compensation_max": max_sal,
            "compensation_currency": curr,
            "compensation_interval": interval,
            "emails": emails,
            "search_keyword": search_keyword,
            "experience": experience,
            "salary_text": salary_text,
            "created_at": now_iso,
            "skills": skills_str,
            "sponsorship_h1b": h1b,
            "source": "dice"
        }
        
        logger.info(f"Extracted: [{record['job_id']}] {record['title']} @ {record['company_name']} ({record['location_display']})")
        return record

    except httpx.RequestError as e:
        logger.warning(f"Network / proxy issue fetching {job_url}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing route {job_url}: {e}")
        return None

async def fetch_desired_job_titles_from_active_clients():
    """
    Fetch target job titles paired with country from the active clients / lightweight endpoint.
    Deduplicates by (country, keyword) so multiple clients from the same country and same
    domain/title are only scraped ONCE.
    """
    default_terms = SEARCH_TERMS
    default_country = LOCATIONS[0] if LOCATIONS else "United States"
    default_targets = [{"keyword": term, "country": default_country} for term in default_terms]
    
    api_key = INTERNAL_SERVICE_API_KEY
    if not api_key:
        logger.warning("INTERNAL_SERVICE_API_KEY not configured in .env. Using default SEARCH_TERMS.")
        return default_targets
        
    endpoint_path = os.getenv("CRM_ENDPOINT_PATH", "/api/clients/active").strip()
    if not endpoint_path.startswith("/"):
        endpoint_path = "/" + endpoint_path
    endpoint = f"{CRM_BACKEND_URL}{endpoint_path}"
    
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json"
    }
    
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.get(endpoint, headers=headers)
            if res.status_code == 200:
                result = res.json()
                
                # Unwrap if wrapped in {"success": true, "data": ...}
                payload_data = result.get("data", result) if isinstance(result, dict) and "data" in result else result
                
                # Normalize payload_data to list of dictionaries
                items = payload_data if isinstance(payload_data, list) else [payload_data]
                
                if MAX_MEMBERS and len(items) > MAX_MEMBERS:
                    logger.info(f"Limiting active CRM members to first {MAX_MEMBERS} (out of {len(items)} active members).")
                    items = items[:MAX_MEMBERS]
                else:
                    logger.info(f"Processing {len(items)} active member(s) from CRM.")

                search_targets = []
                seen_pairs = set()
                
                for item in items:
                    if not isinstance(item, dict):
                        continue
                        
                    # 1. Extract country / location
                    raw_country = item.get("country") or item.get("country_name") or item.get("location") or default_country
                    clean_country = raw_country.strip() if isinstance(raw_country, str) and raw_country.strip() else default_country
                    
                    # 2. Extract desired_job_titles (or fallback to domain)
                    titles = item.get("desired_job_titles") or []
                    if not titles and item.get("domain"):
                        titles = [item.get("domain")]
                    if isinstance(titles, str):
                        titles = [titles]
                        
                    if isinstance(titles, list):
                        for title in titles:
                            if isinstance(title, str) and title.strip():
                                clean_title = title.strip()
                                # Deduplicate by (keyword, country) pair:
                                pair_key = (clean_title.lower(), clean_country.lower())
                                if pair_key not in seen_pairs:
                                    seen_pairs.add(pair_key)
                                    search_targets.append({
                                        "keyword": clean_title,
                                        "country": clean_country
                                    })
                
                if search_targets:
                    logger.info(f"✅ Successfully prepared {len(search_targets)} unique search target(s) across countries: {search_targets}")
                    return search_targets
                else:
                    logger.warning("No search targets found in response. Using default SEARCH_TERMS.")
                    return default_targets
            else:
                logger.error(f"Failed to fetch from {endpoint}: HTTP {res.status_code} - {res.text}. Using default SEARCH_TERMS.")
                return default_targets
    except Exception as e:
        logger.error(f"Error connecting to endpoint at {endpoint}: {e}. Using default SEARCH_TERMS.")
        return default_targets

# Maintain backward compatibility alias
fetch_primary_functions_from_supabase = fetch_desired_job_titles_from_active_clients

# ---------------------------------------------------------------------------
# Neon PostgreSQL Database Handler (Destination Storage)
# ---------------------------------------------------------------------------
_neon_conn = None

def get_neon_connection():
    """Get or establish an active connection to Neon PostgreSQL."""
    global _neon_conn
    conn_str = NEON_DATABASE_URL
    if not conn_str:
        return None

    if not HAS_PSYCOPG2:
        logger.error("psycopg2 is not installed. Please install: pip install psycopg2-binary")
        return None

    # Check existing connection liveness
    try:
        if _neon_conn is not None and not _neon_conn.closed:
            with _neon_conn.cursor() as cur:
                cur.execute("SELECT 1;")
            return _neon_conn
    except Exception:
        try:
            if _neon_conn:
                _neon_conn.close()
        except Exception:
            pass
        _neon_conn = None

    try:
        # Establish connection with Neon DB
        _neon_conn = psycopg2.connect(conn_str, connect_timeout=15)
        _neon_conn.autocommit = False
        _init_neon_table(_neon_conn, NEON_TABLE)
        return _neon_conn
    except Exception as e:
        logger.error(f"Failed to connect to Neon DB: {e}")
        _neon_conn = None
        return None


def close_neon_connection():
    """Close active connection to Neon PostgreSQL."""
    global _neon_conn
    if _neon_conn is not None:
        try:
            _neon_conn.close()
        except Exception:
            pass
        _neon_conn = None


def _init_neon_table(conn, table_name):
    """Ensure the target table and unique index exist in Neon DB."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    job_id TEXT UNIQUE,
                    title TEXT,
                    company_name TEXT,
                    company_url TEXT,
                    company_logo TEXT,
                    location_city TEXT,
                    location_state TEXT,
                    location_country TEXT,
                    location_display TEXT,
                    description TEXT,
                    date_posted TEXT,
                    scraped_at TEXT,
                    job_url TEXT,
                    apply_url TEXT,
                    job_type TEXT,
                    job_level TEXT,
                    company_industry TEXT,
                    job_function TEXT,
                    is_remote BOOLEAN,
                    is_easy_apply BOOLEAN,
                    compensation_min NUMERIC,
                    compensation_max NUMERIC,
                    compensation_currency TEXT,
                    compensation_interval TEXT,
                    emails TEXT,
                    search_keyword TEXT,
                    experience TEXT,
                    salary_text TEXT,
                    created_at TEXT,
                    skills TEXT,
                    sponsorship_h1b TEXT,
                    source TEXT DEFAULT 'dice'
                );
            """)
            cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_job_id ON {table_name}(job_id);")
            conn.commit()
    except Exception as e:
        conn.rollback()
        logger.warning(f"Notice on Neon table initialization for '{table_name}': {e}")


def _sync_save_to_neon(records):
    """Synchronous worker to upsert records into Neon PostgreSQL."""
    if not records:
        return

    if not NEON_DATABASE_URL:
        logger.warning("Neon Database URL not configured in .env (NEON_DATABASE_URL or DATABASE_URL). Skipping Neon upload.")
        return

    conn = get_neon_connection()
    if not conn:
        logger.error("Could not obtain Neon DB connection. Skipping database save.")
        return

    try:
        saved_count = 0
        for record in records:
            job_id = record.get("job_id")
            if not job_id:
                continue

            cols = list(record.keys())
            vals = [record[c] for c in cols]

            update_assignments = [f"{c} = EXCLUDED.{c}" for c in cols if c != "job_id"]
            placeholders = ", ".join(["%s"] * len(cols))
            query = f"""
                INSERT INTO {NEON_TABLE} ({', '.join(cols)})
                VALUES ({placeholders})
                ON CONFLICT (job_id) DO UPDATE SET
                    {', '.join(update_assignments)}
            """

            try:
                with conn.cursor() as cur:
                    cur.execute(query, vals)
                conn.commit()
                saved_count += 1
            except Exception as e:
                conn.rollback()
                err_str = str(e).lower()

                # Dynamic column self-healing (if a column doesn't exist yet)
                if "column" in err_str and "does not exist" in err_str:
                    m = re.search(r'column "([^"]+)" of relation', err_str)
                    if m:
                        missing_col = m.group(1)
                        try:
                            with conn.cursor() as cur:
                                cur.execute(f"ALTER TABLE {NEON_TABLE} ADD COLUMN IF NOT EXISTS {missing_col} TEXT;")
                            conn.commit()
                            logger.info(f"Added missing column '{missing_col}' to Neon table '{NEON_TABLE}'. Retrying...")
                            with conn.cursor() as cur:
                                cur.execute(query, vals)
                            conn.commit()
                            saved_count += 1
                            continue
                        except Exception as retry_err:
                            conn.rollback()
                            logger.error(f"Error adding column '{missing_col}' in Neon DB: {retry_err}")

                # Fallback if table lacks unique constraint on job_id
                if "no unique or exclusion constraint" in err_str:
                    try:
                        with conn.cursor() as cur:
                            cur.execute(f"SELECT id FROM {NEON_TABLE} WHERE job_id = %s", (job_id,))
                            exists = cur.fetchone()
                            if exists:
                                upd_cols = [c for c in cols if c != "job_id"]
                                set_expr = ", ".join([f"{c} = %s" for c in upd_cols])
                                cur.execute(
                                    f"UPDATE {NEON_TABLE} SET {set_expr} WHERE job_id = %s",
                                    [record[c] for c in upd_cols] + [job_id]
                                )
                            else:
                                cur.execute(f"INSERT INTO {NEON_TABLE} ({', '.join(cols)}) VALUES ({placeholders})", vals)
                        conn.commit()
                        saved_count += 1
                        continue
                    except Exception as fb_err:
                        conn.rollback()
                        logger.error(f"Fallback upsert failed for job {job_id} in Neon DB: {fb_err}")
                else:
                    logger.error(f"Failed to upsert job {job_id} into Neon DB: {e}")

        if saved_count > 0:
            logger.info(f"Successfully upserted {saved_count} job(s) into Neon DB '{NEON_TABLE}' table!")
    finally:
        close_neon_connection()


async def save_to_neon(records):
    """Upsert records into Neon PostgreSQL asynchronously."""
    if not records:
        return
    await asyncio.to_thread(_sync_save_to_neon, records)


# Maintain backwards compatibility
save_to_supabase = save_to_neon

async def main():
    logger.info("Starting Dice Route Scraper (HTTP Route Access)...")
    
    search_targets = await fetch_desired_job_titles_from_active_clients()
    if MAX_KEYWORDS and len(search_targets) > MAX_KEYWORDS:
        search_targets = search_targets[:MAX_KEYWORDS]
        logger.info(f"Limited search targets to first {MAX_KEYWORDS}: {search_targets}")
    else:
        logger.info(f"Target scraping targets ({len(search_targets)}): {search_targets}")
    
    all_results = []
    limits = httpx.Limits(max_keepalive_connections=30, max_connections=50)
    webshare_proxies = load_webshare_proxies()
    proxy_rotator = ProxyRotator(webshare_proxies, headers=HEADERS, limits=limits)

    if proxy_rotator.has_proxies():
        logger.info(f"Loaded {proxy_rotator.total()} Webshare rotational proxy configuration(s).")
    else:
        logger.info("No Webshare proxy configured in .env. Using direct connection.")

    async def fetch_job_with_retry(link, term, country):
        """Fetch a single job detail with proxy rotation, 402 detection, and direct fallback."""
        max_job_attempts = min(3, proxy_rotator.total()) if proxy_rotator.has_proxies() else 1
        for attempt in range(max_job_attempts):
            client, proxy_label = await proxy_rotator.get_client()
            try:
                job_data = await fetch_job_detail(client, link, term, country)
                if job_data:
                    return job_data
                elif attempt < max_job_attempts - 1:
                    logger.warning(f"Fetch empty with {proxy_label}, rotating proxy for retry ({attempt+1}/{max_job_attempts})...")
                    await asyncio.sleep(1)
            except httpx.RequestError as req_err:
                err_str = str(req_err)
                if "402" in err_str or "payment required" in err_str.lower():
                    proxy_rotator.remove_failing_proxy(proxy_label, "Webshare 402 Payment Required - Bandwidth/Plan Expired")
                    break
                elif "407" in err_str or "authentication" in err_str.lower():
                    proxy_rotator.remove_failing_proxy(proxy_label, "Proxy 407 Auth Failed")
                    break
                elif attempt < max_job_attempts - 1:
                    logger.warning(f"Proxy network error with {proxy_label}: {req_err}. Rotating proxy...")
                    await asyncio.sleep(1)
                else:
                    logger.error(f"Failed to fetch {link} after {max_job_attempts} attempts: {req_err}")
            except Exception as unexpected:
                logger.error(f"Unexpected error parsing {link}: {unexpected}")
                break

        # Fallback to direct connection if proxies failed or exhausted
        if not proxy_rotator.has_proxies():
            direct_client, direct_label = await proxy_rotator.get_direct_client()
            try:
                return await fetch_job_detail(direct_client, link, term, country)
            except Exception as direct_err:
                logger.error(f"Direct fetch failed for {link}: {direct_err}")
        return None

    try:
        for target_idx, target in enumerate(search_targets):
            if isinstance(target, dict):
                term = target.get("keyword")
                country = target.get("country") or (LOCATIONS[0] if LOCATIONS else "United States")
            else:
                term = target
                country = LOCATIONS[0] if LOCATIONS else "United States"

            logger.info(f"=== Starting scrape for keyword [{target_idx+1}/{len(search_targets)}]: '{term}' in '{country}' (Target: up to {MAX_JOBS_PER_KEYWORD or 'unlimited'} jobs) ===")
            term_count = 0
            page_num = 1
            pending_links = []
            no_more_pages = False

            while term_count < (MAX_JOBS_PER_KEYWORD or float('inf')):
                # Fetch more search pages until we have at least 15 pending links
                while len(pending_links) < 15 and not no_more_pages:
                    date_filter = "&filters.postedDate=ONE" if ONLY_LAST_24_HOURS else ""
                    search_url = f"https://www.dice.com/jobs?q={urllib.parse.quote_plus(term)}&location={urllib.parse.quote_plus(country)}{date_filter}&page={page_num}"
                    
                    res = None
                    max_search_attempts = min(3, proxy_rotator.total()) if proxy_rotator.has_proxies() else 1
                    for attempt in range(max_search_attempts):
                        client, proxy_label = await proxy_rotator.get_client()
                        logger.info(f"Accessing Search Route for '{term}' in '{country}' (Page {page_num}) via {proxy_label}: {search_url}")
                        try:
                            r = await client.get(search_url)
                            if r.status_code == 200:
                                res = r
                                break
                            elif r.status_code in [403, 429] and attempt < max_search_attempts - 1:
                                logger.warning(f"Search route HTTP {r.status_code} with {proxy_label}. Rotating proxy and retrying...")
                                await asyncio.sleep(2)
                            elif r.status_code == 402:
                                proxy_rotator.remove_failing_proxy(proxy_label, "Webshare 402 Payment Required - Bandwidth/Plan Expired")
                                break
                            else:
                                logger.error(f"Search route returned status {r.status_code}")
                                res = r
                                break
                        except httpx.RequestError as e:
                            err_str = str(e)
                            if "402" in err_str or "payment required" in err_str.lower():
                                proxy_rotator.remove_failing_proxy(proxy_label, "Webshare 402 Payment Required - Bandwidth/Plan Expired")
                                break
                            elif "407" in err_str or "authentication" in err_str.lower():
                                proxy_rotator.remove_failing_proxy(proxy_label, "Proxy 407 Auth Failed")
                                break
                            else:
                                logger.warning(f"Connection error on search route with {proxy_label}: {e}. Rotating proxy...")
                            await asyncio.sleep(1)

                    # Fallback to direct connection if proxies failed or exhausted
                    if (not res or res.status_code != 200) and not proxy_rotator.has_proxies():
                        direct_client, direct_label = await proxy_rotator.get_direct_client()
                        logger.info(f"Attempting search route via {direct_label} for '{term}' in '{country}' (Page {page_num})...")
                        try:
                            r = await direct_client.get(search_url)
                            if r.status_code == 200:
                                res = r
                                logger.info(f"✅ Successfully fetched search page via {direct_label}!")
                        except Exception as direct_err:
                            logger.error(f"Direct connection search request failed: {direct_err}")

                    if not res or res.status_code != 200:
                        no_more_pages = True
                        break

                    soup = BeautifulSoup(res.text, 'html.parser')
                    page_job_links = []
                    for a in soup.find_all('a', href=True):
                        href = a['href']
                        if '/job-detail/' in href:
                            if not href.startswith('http'):
                                href = 'https://www.dice.com' + href
                            if href not in page_job_links and href not in pending_links:
                                page_job_links.append(href)

                    if not page_job_links:
                        logger.info(f"No more job links found on page {page_num} for '{term}' in '{country}'.")
                        no_more_pages = True
                        break

                    pending_links.extend(page_job_links)
                    logger.info(f"Found {len(page_job_links)} jobs on page {page_num}. Total queued for batching: {len(pending_links)}")
                    page_num += 1
                    await asyncio.sleep(1)

                if not pending_links:
                    logger.info(f"No more job links available for '{term}' in '{country}'. Completed keyword search.")
                    break

                # Fetch 15 jobs per batch (or remaining needed up to 15)
                needed = (MAX_JOBS_PER_KEYWORD - term_count) if MAX_JOBS_PER_KEYWORD else 15
                current_batch_size = min(15, needed, len(pending_links))
                batch = pending_links[:current_batch_size]
                pending_links = pending_links[current_batch_size:]

                logger.info(f"[{term} @ {country}] ⚡ Fetching batch of {len(batch)} jobs (Target: 15 jobs per 5s)...")
                batch_results = await asyncio.gather(*[fetch_job_with_retry(l, term, country) for l in batch])

                for job_data in batch_results:
                    if job_data:
                        all_results.append(job_data)
                        term_count += 1
                        if MAX_JOBS_PER_KEYWORD and term_count >= MAX_JOBS_PER_KEYWORD:
                            break

                if all_results:
                    df = pd.DataFrame(all_results)
                    df.to_csv(OUTPUT_FILE, index=False)
                    logger.info(f"[{term} @ {country}] Saved {term_count} / {MAX_JOBS_PER_KEYWORD or 'all'} jobs (Total scraped: {len(all_results)}) to CSV: {OUTPUT_FILE}")

                if MAX_JOBS_PER_KEYWORD and term_count >= MAX_JOBS_PER_KEYWORD:
                    logger.info(f"🎯 Reached target limit of {MAX_JOBS_PER_KEYWORD} jobs for keyword '{term}' in '{country}'.")
                    break

                if pending_links or not no_more_pages:
                    logger.info("⏱️ Batch finished. Pausing 5 seconds before fetching next 15 jobs...")
                    await asyncio.sleep(5)

            # Keyword finished: stop/pause up to 30 seconds before next keyword
            if target_idx < len(search_targets) - 1:
                logger.info(f"⏸️ Finished keyword '{term}' in '{country}' ({term_count} jobs). Pausing 30 seconds before next keyword...")
                await asyncio.sleep(30)

    finally:
        await proxy_rotator.close_all()

    if all_results:
        # Phase 1 complete: Final CSV Save with all scraped job links
        df = pd.DataFrame(all_results)
        df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"✅ Scraping phase complete: Total {len(all_results)} jobs stored in CSV: {OUTPUT_FILE}")
        
        # Phase 2: Store all scraped job links ONCE into Neon DB
        logger.info(f"🚀 Storing all {len(all_results)} scraped jobs at once into Neon DB table '{NEON_TABLE}'...")
        await save_to_neon(all_results)
        logger.info(f"✅ All {len(all_results)} jobs stored in Neon DB table '{NEON_TABLE}' successfully!")
    else:
        logger.warning("No jobs were extracted.")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "running event loop" in str(e).lower() or "event loop is already running" in str(e).lower():
            try:
                import nest_asyncio
                nest_asyncio.apply()
            except ImportError:
                pass
            loop = asyncio.get_event_loop()
            loop.run_until_complete(main())
        else:
            raise
