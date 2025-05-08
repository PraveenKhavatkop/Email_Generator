import streamlit as st
import smtplib
import dns.resolver
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor
from io import StringIO

# ------------------- Email Patterns ------------------------

def generate_email_formats(first_name, last_name, domain):
    first = first_name.lower().replace(' ', '')
    last = last_name.lower().replace(' ', '')

    return [
        f"{first}.{last}@{domain}",
        f"{first}{last}@{domain}",
        f"{first[0]}{last}@{domain}",
        f"{first}{last[0]}@{domain}",
        f"{first}_{last}@{domain}",
        f"{first[0]}.{last}@{domain}",
        f"{first}.{last[0]}@{domain}",
        f"{first[0]}_{last}@{domain}",
        f"{last}.{first}@{domain}",
        f"{last}{first}@{domain}",
        f"{first}@{domain}",
        f"{last}@{domain}",
        f"{last}{first[0]}@{domain}",
        f"{first[0]}{last[0]}@{domain}",
        f"{first[0]}_{last[0]}@{domain}",
        f"{last[0]}{first}@{domain}",
    ]

# ------------------- MX Lookup with Caching ------------------------

mx_cache = {}

def get_mx_record(domain):
    if domain in mx_cache:
        return mx_cache[domain]
    try:
        records = dns.resolver.resolve(domain, 'MX')
        mx_record = str(records[0].exchange)
        mx_cache[domain] = mx_record
        return mx_record
    except Exception:
        mx_cache[domain] = None
        return None

# ------------------- SMTP Check ------------------------

def verify_email_smtp(email, mx_record, retries=2):
    for attempt in range(retries):
        try:
            server = smtplib.SMTP(mx_record, timeout=10)
            server.connect(mx_record)
            server.helo()
            server.mail('verify@test.com')
            code, _ = server.rcpt(email)
            server.quit()
            return code in [250, 251]
        except smtplib.SMTPServerDisconnected:
            time.sleep(1)
        except Exception:
            return False
    return False

# ------------------- Find Valid Email ------------------------

def find_valid_email(first_name, last_name, domain):
    email_patterns = generate_email_formats(first_name, last_name, domain)
    mx_record = get_mx_record(domain)

    if not mx_record:
        return None, email_patterns, 'MX lookup failed'

    for email in email_patterns[:5]:  # Check only top 5 patterns for speed
        if verify_email_smtp(email, mx_record):
            return email, email_patterns, 'Deliverable found'
        time.sleep(0.02)
    return None, email_patterns, 'No deliverable email found'

# ------------------- Streamlit UI ------------------------

st.set_page_config(page_title="📧 Bulk Email Generator & Verifier", page_icon="📧")
st.title("📧 Bulk Email Generator & Verifier")

mode = st.radio("Choose Mode:", ["Generate All Email Patterns (Fast)", "Verify Best Email (Slower)"])

uploaded_file = st.file_uploader("Upload CSV (columns: First Name, Last Name, Domain)", type="csv")

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.write(f"📄 Uploaded {df.shape[0]} records")

    results = []
    start_time = time.time()

    def process_row(row):
        fname = str(row.get("First Name", "")).strip()
        lname = str(row.get("Last Name", "")).strip()
        domain = str(row.get("Domain", "")).strip()
        company = str(row.get("Company Name", "")).strip()

        if not (fname and lname and domain):
            return {
                "First Name": fname,
                "Last Name": lname,
                "Domain": domain,
                "Company Name": company,
                "Verified Email": "Invalid Input",
                "All Patterns": "",
                "Status": "Missing First/Last Name or Domain"
            }

        try:
            patterns = generate_email_formats(fname, lname, domain)

            if mode == "Generate All Email Patterns (Fast)":
                return {
                    "First Name": fname,
                    "Last Name": lname,
                    "Domain": domain,
                    "Company Name": company,
                    "Verified Email": "Skipped",
                    "All Patterns": ", ".join(patterns),
                    "Status": "Generated"
                }
            else:
                email, tried, status = find_valid_email(fname, lname, domain)
                return {
                    "First Name": fname,
                    "Last Name": lname,
                    "Domain": domain,
                    "Company Name": company,
                    "Verified Email": email if email else "Not Found",
                    "All Patterns": ", ".join(tried),
                    "Status": status
                }
        except Exception as e:
            return {
                "First Name": fname,
                "Last Name": lname,
                "Domain": domain,
                "Company Name": company,
                "Verified Email": "Error",
                "All Patterns": "",
                "Status": f"Exception: {e}"
            }

    with st.spinner("🔍 Processing emails. Please wait..."):
        if mode == "Verify Best Email (Slower)":
            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(process_row, [row for _, row in df.iterrows()]))
        else:
            results = [process_row(row) for _, row in df.iterrows()]

    end_time = time.time()
    elapsed = round(end_time - start_time, 2)

    result_df = pd.DataFrame(results)
    st.success(f"✅ Completed in {elapsed} seconds")

    st.dataframe(result_df)

    csv = result_df.to_csv(index=False)
    st.download_button("⬇️ Download Results CSV", csv, file_name="email_results.csv", mime="text/csv")
