"""
OMOP concept ID and ICD-10 reference tables for South Asian T2D + CVD conditions.
Provided to Claude as grounding context — reduces hallucination on concept IDs.
"""

OMOP_CONDITIONS = {
    "Type 2 Diabetes Mellitus":          {"omop": "201826",  "icd10": "E11.9"},
    "Type 2 Diabetes with CKD":          {"omop": "201826",  "icd10": "E11.65"},
    "Hypertension":                      {"omop": "320128",  "icd10": "I10"},
    "Coronary Artery Disease":           {"omop": "317576",  "icd10": "I25.10"},
    "Congestive Heart Failure":          {"omop": "316139",  "icd10": "I50.9"},
    "HFrEF":                             {"omop": "316139",  "icd10": "I50.20"},
    "Myocardial Infarction":             {"omop": "4329847", "icd10": "I21.9"},
    "STEMI":                             {"omop": "314666",  "icd10": "I21.3"},
    "Mixed Dyslipidemia":                {"omop": "432867",  "icd10": "E78.5"},
    "Hypercholesterolaemia":             {"omop": "432867",  "icd10": "E78.00"},
    "Chronic Kidney Disease Stage 3b":   {"omop": "443601",  "icd10": "N18.32"},
    "Atrial Fibrillation":               {"omop": "313217",  "icd10": "I48.91"},
    "Stable Angina":                     {"omop": "321318",  "icd10": "I20.9"},
    "Pre-diabetes":                      {"omop": "4193704", "icd10": "R73.09"},
    "Pre-hypertension":                  {"omop": "320128",  "icd10": "R03.0"},
    "Obesity":                           {"omop": "433736",  "icd10": "E66.9"},
}

OMOP_MEDICATIONS = {
    "Metformin":                {"omop": "1503297",  "rxnorm": "6809"},
    "Atorvastatin":             {"omop": "1545958",  "rxnorm": "83367"},
    "Rosuvastatin":             {"omop": "1510813",  "rxnorm": "301542"},
    "Aspirin":                  {"omop": "1112807",  "rxnorm": "1191"},
    "Amlodipine":               {"omop": "1332418",  "rxnorm": "17767"},
    "Ramipril":                 {"omop": "1308216",  "rxnorm": "35208"},
    "Glimepiride":              {"omop": "1597756",  "rxnorm": "25789"},
    "Glyclazide":               {"omop": "1516766",  "rxnorm": "25207"},
    "Insulin Glargine":         {"omop": "40239216", "rxnorm": "274783"},
    "Insulin Lispro":           {"omop": "1516023",  "rxnorm": "86009"},
    "Empagliflozin":            {"omop": "45774751", "rxnorm": "1613396"},
    "Sitagliptin":              {"omop": "1580747",  "rxnorm": "593411"},
    "Furosemide":               {"omop": "956874",   "rxnorm": "4603"},
    "Carvedilol":               {"omop": "1346823",  "rxnorm": "20352"},
    "Bisoprolol":               {"omop": "1338005",  "rxnorm": "19484"},
    "Spironolactone":           {"omop": "974166",   "rxnorm": "9997"},
    "Warfarin":                 {"omop": "1310149",  "rxnorm": "11289"},
    "Clopidogrel":              {"omop": "1322184",  "rxnorm": "32968"},
    "Telmisartan":              {"omop": "1317640",  "rxnorm": "73494"},
    "Metoprolol Succinate":     {"omop": "1307046",  "rxnorm": "866514"},
    "Ezetimibe":                {"omop": "1547504",  "rxnorm": "341248"},
    "Isosorbide Mononitrate":   {"omop": "1361364",  "rxnorm": "41493"},
    "Omeprazole":               {"omop": "923645",   "rxnorm": "7646"},
    "Omega-3 Fatty Acids":      {"omop": "19129655", "rxnorm": "224905"},
}

OMOP_MEASUREMENTS = {
    "HbA1c":                    {"omop": "3004410",  "loinc": "4548-4"},
    "Fasting Blood Glucose":    {"omop": "3004501",  "loinc": "76629-5"},
    "LDL Cholesterol":          {"omop": "3028437",  "loinc": "2089-1"},
    "HDL Cholesterol":          {"omop": "3007070",  "loinc": "2085-9"},
    "Total Cholesterol":        {"omop": "3019900",  "loinc": "2093-3"},
    "Triglycerides":            {"omop": "3022192",  "loinc": "2571-8"},
    "Serum Creatinine":         {"omop": "3016723",  "loinc": "2160-0"},
    "eGFR":                     {"omop": "3049187",  "loinc": "62238-1"},
    "Systolic Blood Pressure":  {"omop": "3004249",  "loinc": "8480-6"},
    "Diastolic Blood Pressure": {"omop": "3012888",  "loinc": "8462-4"},
    "Body Weight":              {"omop": "3025315",  "loinc": "29463-7"},
    "BMI":                      {"omop": "3038553",  "loinc": "39156-5"},
    "Heart Rate":               {"omop": "3027018",  "loinc": "8867-4"},
    "BNP":                      {"omop": "3024929",  "loinc": "42637-9"},
    "Serum Potassium":          {"omop": "3023103",  "loinc": "2823-3"},
    "Serum Sodium":             {"omop": "3019550",  "loinc": "2951-2"},
    "Urine ACR":                {"omop": "3029683",  "loinc": "9318-7"},
    "Haemoglobin":              {"omop": "3000963",  "loinc": "718-7"},
    "Troponin I":               {"omop": "3016931",  "loinc": "10839-9"},
    "INR":                      {"omop": "3023314",  "loinc": "6301-6"},
    "SpO2":                     {"omop": "3016502",  "loinc": "59408-5"},
}


def build_reference_block() -> str:
    """Format reference tables as a compact string for injection into the system prompt."""
    lines = ["=== OMOP CONCEPT ID REFERENCE (use these exact IDs) ===\n"]

    lines.append("--- CONDITIONS ---")
    for name, ids in OMOP_CONDITIONS.items():
        lines.append(f"  {name}: omop={ids['omop']}  icd10={ids['icd10']}")

    lines.append("\n--- MEDICATIONS ---")
    for name, ids in OMOP_MEDICATIONS.items():
        lines.append(f"  {name}: omop={ids['omop']}")

    lines.append("\n--- MEASUREMENTS ---")
    for name, ids in OMOP_MEASUREMENTS.items():
        lines.append(f"  {name}: omop={ids['omop']}  loinc={ids['loinc']}")

    return "\n".join(lines)
