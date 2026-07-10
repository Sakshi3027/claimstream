import json
import random
import time
import os
from datetime import datetime, timedelta

random.seed(42)

PAYERS = ["UnitedHealthcare", "Aetna", "Cigna", "BlueCross", "Humana"]
DENIAL_REASONS = [
    "Service not covered under plan",
    "Prior authorization required",
    "Duplicate claim submission",
    "Patient not eligible on date of service",
    "Medical necessity not established",
    "Out of network provider",
    "Claim filed after deadline",
    "Missing or invalid diagnosis code"
]
CPT_CODES = {
    "99213": "Office visit - established patient",
    "99214": "Office visit - complex",
    "93000": "Electrocardiogram",
    "71046": "Chest X-ray",
    "80053": "Comprehensive metabolic panel",
    "99283": "Emergency dept visit - moderate",
    "27447": "Total knee replacement",
    "43239": "Upper GI endoscopy with biopsy"
}
DIAGNOSIS_CODES = ["Z00.00", "I10", "E11.9", "J06.9", "M54.5", "F32.9"]

os.makedirs("data/stream", exist_ok=True)

claim_counter = 1

def generate_claim():
    global claim_counter
    cpt = random.choice(list(CPT_CODES.keys()))
    payer = random.choice(PAYERS)
    denied = random.random() < 0.35
    date = datetime.now() - timedelta(days=random.randint(1, 365))
    billed = round(random.uniform(150, 8000), 2)

    claim = {
        "claim_id": f"CLM{str(claim_counter).zfill(5)}",
        "patient_id": f"PAT{str(random.randint(1, 100)).zfill(4)}",
        "date_of_service": date.strftime("%Y-%m-%d"),
        "cpt_code": cpt,
        "cpt_description": CPT_CODES[cpt],
        "payer": payer,
        "billed_amount": billed,
        "allowed_amount": round(billed * random.uniform(0.4, 0.9), 2) if not denied else 0.0,
        "paid_amount": round(billed * random.uniform(0.3, 0.8), 2) if not denied else 0.0,
        "status": "DENIED" if denied else "PAID",
        "denial_reason": random.choice(DENIAL_REASONS) if denied else None,
        "provider_npi": f"NPI{random.randint(1000000000, 9999999999)}",
        "diagnosis_code": random.choice(DIAGNOSIS_CODES),
        "ingestion_timestamp": datetime.now().isoformat()
    }
    claim_counter += 1
    return claim

def run_producer(num_claims=100, delay=0.1):
    print(f"Starting ClaimStream producer — generating {num_claims} claims...")
    claims = []
    for i in range(num_claims):
        claim = generate_claim()
        claims.append(claim)
        # Write each claim as a separate JSON file (simulates streaming)
        filepath = f"data/stream/claim_{claim['claim_id']}.json"
        with open(filepath, 'w') as f:
            json.dump(claim, f)
        print(f"[{i+1}/{num_claims}] Generated {claim['claim_id']} — {claim['payer']} — {claim['status']}")
        time.sleep(delay)
    print(f"\nProducer complete. {num_claims} claims written to data/stream/")
    return claims

if __name__ == "__main__":
    run_producer(num_claims=100, delay=0.05)
