# render_franchise_agreement.py
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

# 1) Load the template from a file on disk
#    Save your Jinja template as franchise_agreement_template.html in the templates folder
env = Environment(
    loader=FileSystemLoader(Path(__file__).parent / "templates"),
    autoescape=select_autoescape(["html"]),
)

tpl = env.get_template("franchise_agreement.html")

# 2) Example A. rely on defaults from the template
html_default = tpl.render()
Path("agreement_default.html").write_text(html_default, encoding="utf-8")

# 3) Example B. Burger King with custom values
context = {
    "franchisor_name": "Burger King Corporation",
    "brand_name": "Burger King",
    "franchisee_company": "ACME Operations LLC",
    "authorized_signer": "Alex Morgan",
    "execution_date": "June 15, 2025",
    "address": "200 King Way, Miami, FL 33101",
    "email": "contracts@acme.example",
    "phone": "(305) 555-0199",
    "governing_state": "Florida",
    "venue_county": "Miami Dade",
    "currency": "USD",
    "initial_fee": "50,000",
    "royalty_pct": "4.5",
    "ad_fund_pct": "4",
    "term_years": "20",
    # Reorder sections if you want
    "sections_order": [
        "cover",
        "recitals",
        "grant",
        "fees",
        "operating_standards",
        "training_qc",
        "insurance_indemnity",
        "term_and_renewal",
        "confidentiality",
        "termination",
        "post_termination",
        "dispute_resolution",
        "miscellaneous",
        "signatures",
    ],
}

html_bk = tpl.render(**context)
output_path = Path(__file__).parent / "dev-local" / "agreement_burger_king.html"
output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(html_bk, encoding="utf-8")
