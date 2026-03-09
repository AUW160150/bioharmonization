"""
BioHarmonize — Synthetic South Asian Patient Data Generator
Generates realistic fragmented, multilingual clinical data for 10 patients
Disease focus: Type 2 Diabetes (T2D) + Cardiovascular (mixed)
"""

import os
import csv

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# Intentionally incomplete patients — simulate real-world fragmentation
# Key demo differentiator: BioHarmonize handles missing modalities gracefully
SKIP_FILES = {
    "clinical_note_P003_hindi.txt",    # P003: only lab CSV  (missing note + VCF)
    "lab_results_P006.csv",            # P006: only note     (missing labs + VCF)
    "clinical_note_P009_bengali.txt",  # P009: only lab CSV  (missing note + VCF)
    "lab_results_P010.csv",            # P010: only note     (missing labs + VCF)
}

# ---------------------------------------------------------------------------
# Patient registry
# ---------------------------------------------------------------------------
PATIENTS = [
    {"id": "P001", "name": "Arjun Sharma",      "age": 58, "sex": "M", "lang": "hindi",   "conditions": ["T2D", "CAD"]},
    {"id": "P002", "name": "Fatima Begum",       "age": 52, "sex": "F", "lang": "bengali", "conditions": ["T2D", "HTN"]},
    {"id": "P003", "name": "Rajesh Kumar",       "age": 45, "sex": "M", "lang": "hindi",   "conditions": ["CAD", "DYS"]},
    {"id": "P004", "name": "Ananya Das",         "age": 61, "sex": "F", "lang": "bengali", "conditions": ["T2D", "CHF"]},
    {"id": "P005", "name": "Suresh Verma",       "age": 67, "sex": "M", "lang": "hindi",   "conditions": ["T2D", "CAD", "CKD"]},
    {"id": "P006", "name": "Priya Banerjee",     "age": 49, "sex": "F", "lang": "bengali", "conditions": ["T2D", "DYS"]},
    {"id": "P007", "name": "Mohammad Hossain",   "age": 55, "sex": "M", "lang": "bengali", "conditions": ["MI", "T2D"]},
    {"id": "P008", "name": "Sunita Devi",        "age": 63, "sex": "F", "lang": "hindi",   "conditions": ["T2D", "HTN", "CAD"]},
    {"id": "P009", "name": "Karim Sheikh",       "age": 70, "sex": "M", "lang": "bengali", "conditions": ["CHF", "CAD"]},
    {"id": "P010", "name": "Meena Sharma",       "age": 44, "sex": "F", "lang": "hindi",   "conditions": ["T2D_new"]},
]

# ---------------------------------------------------------------------------
# Clinical notes — Bengali
# ---------------------------------------------------------------------------
BENGALI_NOTES = {
    "P002": """রোগীর তথ্য
নাম: ফাতিমা বেগম
বয়স: ৫২ বছর
লিঙ্গ: মহিলা
তারিখ: ১৫ জানুয়ারি ২০২৪

রোগ নির্ণয়:
- টাইপ ২ ডায়াবেটিস মেলিটাস (৬ বছর ধরে)
- উচ্চ রক্তচাপ (নিয়ন্ত্রণে নেই)

ওষুধ:
- মেটফর্মিন ৫০০ মিগ্রা — দিনে দুইবার (সকাল ও রাতে খাবারের সাথে)
- অ্যামলোডিপিন ৫ মিগ্রা — প্রতিদিন একবার
- গ্লাইক্লাজাইড ৮০ মিগ্রা — সকালে খাবারের আগে

গুরুত্বপূর্ণ শারীরিক পরিমাপ:
রক্তচাপ: ১৫৮/৯৬ মিমিএইচজি
নাড়ির গতি: ৮৮ বার/মিনিট
ওজন: ৭২ কেজি
উচ্চতা: ১৫৫ সেমি
BMI: ২৯.৯

পরীক্ষার ফলাফল (স্থানীয় ল্যাব, ঢাকা):
খালি পেটে রক্ত শর্করা: ১১.২ mmol/L (স্বাভাবিক: ৩.৯–৫.৫)
HbA1c: ৯.৮%
মোট কোলেস্টেরল: ৬.২ mmol/L

চিকিৎসকের মন্তব্য:
রক্তের শর্করা নিয়ন্ত্রণ সন্তোষজনক নয়। ইনসুলিন থেরাপি বিবেচনা করতে হবে।
পরবর্তী ফলো-আপ: ৩ মাস পরে।
""",
    "P004": """রোগী পরিচিতি
নাম: অনন্যা দাস
বয়স: ৬১ বছর
লিঙ্গ: মহিলা
পরীক্ষার তারিখ: ২২ ফেব্রুয়ারি ২০২৪

প্রধান অভিযোগ: শ্বাসকষ্ট, পা ফুলে যাওয়া, ক্লান্তি

রোগ নির্ণয়:
- টাইপ ২ ডায়াবেটিস মেলিটাস (৯ বছর)
- কনজেস্টিভ হার্ট ফেইলিউর (EF ৩৫%)

চলমান ওষুধ:
- ইনসুলিন গ্লার্জিন ২০ ইউনিট — রাতে
- মেটফর্মিন ১০০০ মিগ্রা — দুইবার (সতর্কতার সাথে, eGFR পর্যবেক্ষণ করুন)
- ফুরোসেমাইড ৪০ মিগ্রা — সকালে
- কার্ভেডিলল ৬.২৫ মিগ্রা — দুইবার
- র‍্যামিপ্রিল ২.৫ মিগ্রা — রাতে

শারীরিক পরিমাপ:
রক্তচাপ: ১৩২/৮২ mmHg
হৃদস্পন্দন: ৬৮/মিনিট (অনিয়মিত)
ওজন: ৬৫ কেজি (গত সপ্তাহে ৩ কেজি বৃদ্ধি — তরল জমে)
SpO2: ৯৪% (বায়ুতে)

বিশেষ নির্দেশনা:
প্রতিদিন ওজন পরিমাপ করুন। লবণ গ্রহণ সীমিত করুন (<২ গ্রাম/দিন)।
""",
    "P006": """রোগীর বিবরণ
নাম: প্রিয়া ব্যানার্জি
বয়স: ৪৯ বছর
লিঙ্গ: মহিলা
তারিখ: ০৫ মার্চ ২০২৪

রোগ নির্ণয়:
- টাইপ ২ ডায়াবেটিস (প্রাক-পর্যায় থেকে রূপান্তর, ৩ বছর)
- মিশ্র ডিসলিপিডেমিয়া

ওষুধ:
- মেটফর্মিন ৫০০ মিগ্রা — দিনে একবার (সকালে)
- অ্যাটোরভাস্টাটিন ২০ মিগ্রা — রাতে
- ওমেগা-৩ ফ্যাটি অ্যাসিড ১ গ্রাম — দৈনিক

গুরুত্বপূর্ণ তথ্য:
রক্তচাপ: ১২৮/৮০ mmHg
ওজন: ৬৮ কেজি, উচ্চতা: ১৫৮ সেমি
খালি পেটে গ্লুকোজ: ৭.৮ mmol/L
LDL কোলেস্টেরল: ৪.১ mmol/L (লক্ষ্যমাত্রা <২.৬)
পারিবারিক ইতিহাস: বাবার হার্ট অ্যাটাক (৫৫ বছরে)
""",
    "P007": """রোগী তথ্য পত্র
নাম: মোহাম্মদ হোসেন
বয়স: ৫৫ বছর
লিঙ্গ: পুরুষ
ভর্তির তারিখ: ১০ জানুয়ারি ২০২৪

ইতিহাস:
৬ মাস আগে তীব্র মায়োকার্ডিয়াল ইনফার্কশন (STEMI, LAD অক্লুশন)
PCI করা হয়েছে, ২টি স্টেন্ট স্থাপিত
সম্প্রতি টাইপ ২ ডায়াবেটিস নির্ণয় হয়েছে

চলমান ওষুধ:
- অ্যাসপিরিন ৭৫ মিগ্রা — দৈনিক
- ক্লোপিডোগ্রেল ৭৫ মিগ্রা — দৈনিক (১২ মাস পর্যন্ত)
- অ্যাটোরভাস্টাটিন ৮০ মিগ্রা — রাতে
- মেটোপ্রোলল সাকসিনেট ৫০ মিগ্রা — সকালে
- র‍্যামিপ্রিল ৫ মিগ্রা — রাতে
- মেটফর্মিন ৫০০ মিগ্রা — দৈনিক (ডোজ বাড়ানো হবে)

শারীরিক পরিমাপ:
রক্তচাপ: ১২২/৭৬ mmHg
হৃদস্পন্দন: ৬২/মিনিট
ওজন: ৭৮ কেজি

পরামর্শ: কার্ডিয়াক রিহ্যাবিলিটেশন প্রোগ্রামে যোগ দেওয়ার পরামর্শ দেওয়া হয়েছে।
""",
    "P009": """রোগী পরিচয়
নাম: করিম শেখ
বয়স: ৭০ বছর
লিঙ্গ: পুরুষ
পরীক্ষার তারিখ: ১৮ ফেব্রুয়ারি ২০২৪

রোগ নির্ণয়:
- ক্রনিক হার্ট ফেইলিউর (HFrEF, EF ৩০%)
- করোনারি আর্টারি ডিজিজ (থ্রি-ভেসেল)
- অ্যাট্রিয়াল ফিব্রিলেশন (স্থায়ী)

ওষুধ:
- বিসোপ্রোলল ৫ মিগ্রা — দৈনিক
- স্পাইরোনোল্যাকটোন ২৫ মিগ্রা — দৈনিক
- ফুরোসেমাইড ৮০ মিগ্রা — সকালে
- ওয়ারফারিন ৩ মিগ্রা — দৈনিক (INR লক্ষ্য: ২–৩)
- অ্যাটোরভাস্টাটিন ৪০ মিগ্রা — রাতে
- র‍্যামিপ্রিল ১০ মিগ্রা — দুইবার

শারীরিক পরিমাপ:
রক্তচাপ: ১১০/৭০ mmHg (নিম্নমুখী প্রবণতা)
হৃদস্পন্দন: ৭৮/মিনিট (অনিয়মিত)
ওজন: ৬২ কেজি
JVP: উন্নত (৫ সেমি)
পায়ে পিটিং এডিমা: ++

INR আজকের ফলাফল: ২.৪ (লক্ষ্যমাত্রার মধ্যে)
""",
}

# ---------------------------------------------------------------------------
# Clinical notes — Hindi
# ---------------------------------------------------------------------------
HINDI_NOTES = {
    "P001": """रोगी की जानकारी
नाम: अर्जुन शर्मा
आयु: 58 वर्ष
लिंग: पुरुष
दिनांक: 20 जनवरी 2024

रोग निदान:
- टाइप 2 मधुमेह मेलिटस (12 वर्षों से)
- कोरोनरी आर्टरी डिजीज (2021 में CABG किया गया)

दवाइयाँ:
- मेटफॉर्मिन 1000 मिग्रा — दिन में दो बार (सुबह-शाम, भोजन के साथ)
- एटोरवास्टेटिन 40 मिग्रा — रात को सोते समय
- एस्पिरिन 75 मिग्रा — प्रतिदिन सुबह
- रामिप्रिल 5 मिग्रा — रात को
- ग्लिमेपिराइड 2 मिग्रा — नाश्ते से पहले

महत्वपूर्ण संकेत:
रक्तचाप: 148/88 mmHg
हृदय गति: 72 बार/मिनट
वजन: 84 किग्रा
ऊंचाई: 168 सेमी
BMI: 29.8

प्रयोगशाला परिणाम (सीताराम भरतिया अस्पताल, दिल्ली):
उपवास रक्त शर्करा: 9.4 mmol/L
HbA1c: 8.6%
कुल कोलेस्ट्रॉल: 5.1 mmol/L
LDL: 2.8 mmol/L
HDL: 0.9 mmol/L
ट्राइग्लिसराइड: 3.2 mmol/L
क्रिएटिनिन: 112 µmol/L
eGFR: 58 mL/min/1.73m²

चिकित्सक टिप्पणी:
HbA1c अभी भी लक्ष्य से अधिक है। SGLT2 inhibitor जोड़ने पर विचार करें।
अगली मुलाकात: 3 महीने बाद, eGFR की निगरानी आवश्यक।
""",
    "P003": """रोगी विवरण
नाम: राजेश कुमार
आयु: 45 वर्ष
लिंग: पुरुष
दिनांक: 08 फरवरी 2024

मुख्य शिकायत: सीने में दर्द (परिश्रम पर), सांस फूलना

रोग निदान:
- कोरोनरी आर्टरी डिजीज (स्थिर एनजाइना)
- गंभीर डिसलिपिडेमिया (पारिवारिक हाइपरकोलेस्टेरोलेमिया संभावित)

दवाइयाँ:
- रोसुवास्टेटिन 40 मिग्रा — रात को (उच्च तीव्रता)
- एज़ेटिमिब 10 मिग्रा — सुबह
- एस्पिरिन 75 मिग्रा — प्रतिदिन
- आइसोसोर्बाइड मोनोनाइट्रेट 30 मिग्रा — सुबह
- बिसोप्रोलल 5 मिग्रा — सुबह

महत्वपूर्ण संकेत:
रक्तचाप: 136/84 mmHg
हृदय गति: 68/मिनट
वजन: 76 किग्रा

प्रयोगशाला (AIIMS दिल्ली):
LDL कोलेस्ट्रॉल: 5.6 mmol/L (बहुत अधिक — लक्ष्य <1.4)
ट्राइग्लिसराइड: 2.8 mmol/L
HDL: 0.8 mmol/L
उपवास ग्लूकोज: 5.2 mmol/L (सामान्य)
""",
    "P005": """रोगी सूचना
नाम: सुरेश वर्मा
आयु: 67 वर्ष
लिंग: पुरुष
दिनांक: 12 मार्च 2024

निदान:
- टाइप 2 मधुमेह (15 वर्ष, खराब नियंत्रण)
- कोरोनरी आर्टरी डिजीज
- क्रोनिक किडनी रोग (Stage 3b)

दवाइयाँ (सावधानी — CKD के कारण खुराक समायोजित):
- इंसुलिन ग्लार्जिन 30 यूनिट — रात को
- इंसुलिन लिस्प्रो — भोजन के साथ (8-10 यूनिट)
- एम्पाग्लिफ्लोज़िन 10 मिग्रा — सुबह (eGFR >30 होने पर)
- एटोरवास्टेटिन 20 मिग्रा — रात को (खुराक कम — CKD)
- एस्पिरिन 75 मिग्रा — प्रतिदिन
- ओमेप्राज़ोल 20 मिग्रा — सुबह (गैस्ट्रो-सुरक्षा)

महत्वपूर्ण संकेत:
रक्तचाप: 142/86 mmHg
वजन: 71 किग्रा

प्रयोगशाला परिणाम:
HbA1c: 10.2% (बहुत खराब)
उपवास रक्त शर्करा: 13.8 mmol/L
क्रिएटिनिन: 198 µmol/L
eGFR: 32 mL/min/1.73m²
पोटेशियम: 5.4 mEq/L (सावधानी)
मूत्र में प्रोटीन: ++ (एल्बुमिनूरिया)
""",
    "P008": """रोगी जानकारी
नाम: सुनीता देवी
आयु: 63 वर्ष
लिंग: महिला
दिनांक: 25 जनवरी 2024

रोग निदान:
- टाइप 2 मधुमेह (8 वर्ष)
- उच्च रक्तचाप (नियंत्रित)
- कोरोनरी आर्टरी डिजीज (2022 में Angioplasty)

दवाइयाँ:
- मेटफॉर्मिन 1000 मिग्रा — दो बार
- सीताग्लिप्टिन 100 मिग्रा — एक बार
- अम्लोदिपिन 10 मिग्रा — सुबह
- टेल्मिसार्टन 80 मिग्रा — सुबह
- एटोरवास्टेटिन 40 मिग्रा — रात को
- क्लोपिडोग्रेल 75 मिग्रा — सुबह
- एस्पिरिन 75 मिग्रा — सुबह (dual antiplatelet)

महत्वपूर्ण संकेत:
रक्तचाप: 128/78 mmHg (नियंत्रित)
हृदय गति: 74/मिनट
वजन: 66 किग्रा
BMI: 27.1

प्रयोगशाला:
HbA1c: 7.4% (लक्ष्य के करीब)
उपवास ग्लूकोज: 8.1 mmol/L
LDL: 1.8 mmol/L (लक्ष्य प्राप्त)
क्रिएटिनिन: 88 µmol/L, eGFR: 62
""",
    "P010": """रोगी विवरण
नाम: मीना शर्मा
आयु: 44 वर्ष
लिंग: महिला
दिनांक: 01 मार्च 2024

मुख्य शिकायत: अत्यधिक प्यास, बार-बार पेशाब, थकान (2 महीने से)

रोग निदान:
- टाइप 2 मधुमेह मेलिटस (नया निदान)
- पूर्व-उच्च रक्तचाप

पारिवारिक इतिहास:
माँ: टाइप 2 मधुमेह
पिता: उच्च रक्तचाप + हृदयाघात (62 वर्ष में)

नई दवाइयाँ (आज शुरू):
- मेटफॉर्मिन 500 मिग्रा — सुबह खाने के साथ (2 सप्ताह बाद खुराक बढ़ाएं)

महत्वपूर्ण संकेत:
रक्तचाप: 134/84 mmHg
वजन: 72 किग्रा, ऊंचाई: 160 सेमी, BMI: 28.1

प्रयोगशाला (पहली बार):
उपवास रक्त शर्करा: 10.6 mmol/L
HbA1c: 8.1%
कुल कोलेस्ट्रॉल: 4.8 mmol/L
LDL: 3.1 mmol/L
HDL: 1.1 mmol/L
ट्राइग्लिसराइड: 2.1 mmol/L

जीवनशैली सलाह:
कम ग्लाइसेमिक आहार, 30 मिनट रोज व्यायाम, 3 महीने बाद पुनः HbA1c जांच।
""",
}

# ---------------------------------------------------------------------------
# Lab result data (non-standard column names, local reference ranges, mmol/L)
# ---------------------------------------------------------------------------
LAB_DATA = {
    # Bengali patients — mixed Bengali/English column headers
    "P002": {
        "headers": ["রোগী_আইডি", "তারিখ", "রক্ত_শর্করা_খালিপেটে_mmol", "HbA1c_%", "মোট_কোলেস্টেরল_mmol", "ট্রাইগ্লিসারাইড_mmol", "HDL_mmol", "LDL_mmol", "ক্রিয়েটিনিন_umol", "eGFR_ml_min"],
        "values": ["P002", "2024-01-15", 11.2, 9.8, 6.2, 2.9, 0.8, 4.1, 102, 54],
        "ref":    ["", "", "3.9-5.5", "<7.0", "<5.2", "<1.7", ">1.2(F)", "<2.6", "44-97", ">60"],
    },
    "P004": {
        "headers": ["রোগী_ID", "পরীক্ষার_তারিখ", "FBS_mmol_per_L", "HbA1c_শতাংশ", "Na_mEq_L", "K_mEq_L", "Cr_umol_L", "eGFR", "BNP_pg_mL", "INR"],
        "values": ["P004", "2024-02-22", 8.9, 8.3, 138, 4.1, 124, 44, 820, 1.1],
        "ref":    ["", "", "3.9-5.5", "<7.0", "135-145", "3.5-5.0", "44-97", ">60", "<100", "0.8-1.2"],
    },
    "P006": {
        "headers": ["Pt_ID", "Date", "Glu_F_mmol", "HbA1c_pct", "Chol_T_mmol", "TG_mmol", "HDL_F_mmol", "LDL_calc_mmol", "ALT_U_L", "AST_U_L"],
        "values": ["P006", "2024-03-05", 7.8, 7.2, 5.9, 2.4, 1.0, 3.8, 32, 28],
        "ref":    ["", "", "3.9-5.5", "<7.0", "<5.2", "<1.7", ">1.2(F)", "<2.6", "7-40", "10-40"],
    },
    "P007": {
        "headers": ["রোগী_কোড", "ভর্তি_তারিখ", "উপবাস_গ্লুকোজ_mmol", "HbA1c", "ট্রোপোনিন_I_ng_mL", "CK_MB_U_L", "মোট_কোলেস্টেরল_mmol", "LDL_mmol", "HDL_mmol", "ক্রিয়েটিনিন_µmol"],
        "values": ["P007", "2024-01-10", 9.1, 7.9, 0.04, 18, 5.8, 3.2, 0.9, 96],
        "ref":    ["", "", "3.9-5.5", "<7.0", "<0.04", "<25", "<5.2", "<1.4(high-risk)", ">1.0(M)", "62-106"],
    },
    "P009": {
        "headers": ["ID", "তারিখ", "Na", "K", "Cr_umol", "eGFR", "BNP_pg_mL", "INR", "Hb_g_dL", "WBC_x10_9"],
        "values": ["P009", "2024-02-18", 136, 4.8, 148, 38, 1240, 2.4, 11.2, 7.8],
        "ref":    ["", "", "135-145", "3.5-5.0", "62-106", ">60", "<100", "2.0-3.0(AF)", "13-17(M)", "4-11"],
    },
    # Hindi patients — mixed Hindi/English column headers
    "P001": {
        "headers": ["मरीज_आईडी", "दिनांक", "उपवास_रक्त_शर्करा_mmol", "HbA1c_%", "कुल_कोलेस्ट्रॉल_mmol", "ट्राइग्लिसराइड_mmol", "HDL_mmol", "LDL_mmol", "क्रिएटिनिन_umol", "eGFR"],
        "values": ["P001", "2024-01-20", 9.4, 8.6, 5.1, 3.2, 0.9, 2.8, 112, 58],
        "ref":    ["", "", "3.9-5.5", "<7.0", "<5.2", "<1.7", ">1.0(M)", "<2.6", "62-106", ">60"],
    },
    "P003": {
        "headers": ["Pt_Code", "Date", "LDL_Chol_mmol", "Total_Chol_mmol", "TG_mmol", "HDL_mmol", "FBG_mmol", "HbA1c_%", "Lp_a_nmol_L", "hs_CRP_mg_L"],
        "values": ["P003", "2024-02-08", 5.6, 7.1, 2.8, 0.8, 5.2, 5.4, 210, 4.8],
        "ref":    ["", "", "<1.4(very-high-risk)", "<5.2", "<1.7", ">1.0(M)", "3.9-5.5", "<5.7", "<75", "<1.0"],
    },
    "P005": {
        "headers": ["मरीज_कोड", "जांच_दिनांक", "उपवास_ग्लूकोज", "HbA1c_प्रतिशत", "क्रिएटिनिन_µmol", "eGFR_ml_min", "पोटेशियम_mEq", "एल्ब्यूमिन_g_dL", "Urine_ACR_mg_mmol", "Hb_g_dL"],
        "values": ["P005", "2024-03-12", 13.8, 10.2, 198, 32, 5.4, 3.6, 68, 10.8],
        "ref":    ["", "", "3.9-5.5", "<7.0", "62-106", ">60", "3.5-5.0", "3.5-5.0", "<3.0", "13-17(M)"],
    },
    "P008": {
        "headers": ["Patient_ID", "Visit_Date", "FPG_mmol_L", "HbA1c_%", "BP_Sys_mmHg", "BP_Dia_mmHg", "LDL_mmol_L", "HDL_mmol_L", "Cr_umol_L", "eGFR"],
        "values": ["P008", "2024-01-25", 8.1, 7.4, 128, 78, 1.8, 1.2, 88, 62],
        "ref":    ["", "", "3.9-5.5", "<7.0", "<130", "<80", "<1.4(post-ACS)", ">1.2(F)", "44-97", ">60"],
    },
    "P010": {
        "headers": ["मरीज_ID", "दिनांक", "उपवास_शर्करा_mmol", "HbA1c_%", "कुल_Chol_mmol", "LDL_mmol", "HDL_mmol", "TG_mmol", "BP_सिस्टोलिक", "BP_डायस्टोलिक"],
        "values": ["P010", "2024-03-01", 10.6, 8.1, 4.8, 3.1, 1.1, 2.1, 134, 84],
        "ref":    ["", "", "3.9-5.5", "<7.0", "<5.2", "<2.6", ">1.2(F)", "<1.7", "<130", "<80"],
    },
}

# ---------------------------------------------------------------------------
# Mock VCF data — South Asian relevant variants (T2D + CVD risk)
# Patients P001, P004, P007 have genomic data
# ---------------------------------------------------------------------------
VCF_TEMPLATE = """##fileformat=VCFv4.2
##fileDate=20240115
##source=BioHarmonize_SyntheticGen_v1.0
##reference=GRCh38
##INFO=<ID=GENE,Number=1,Type=String,Description="Gene name">
##INFO=<ID=CLNSIG,Number=1,Type=String,Description="Clinical significance">
##INFO=<ID=AF_SAS,Number=1,Type=Float,Description="Allele frequency in South Asian population">
##INFO=<ID=PHENOTYPE,Number=1,Type=String,Description="Associated phenotype">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t{sample_id}
"""

VCF_VARIANTS = {
    "P001": [
        "10\t114758349\trs7903146\tC\tT\t.\tPASS\tGENE=TCF7L2;CLNSIG=risk_factor;AF_SAS=0.28;PHENOTYPE=Type_2_Diabetes\tGT\t1/1",
        "3\t12393125\trs1801282\tC\tG\t.\tPASS\tGENE=PPARG;CLNSIG=protective;AF_SAS=0.04;PHENOTYPE=Type_2_Diabetes_T2D\tGT\t0/1",
        "9\t22125503\trs2383206\tG\tA\t.\tPASS\tGENE=CDKN2A_CDKN2B;CLNSIG=risk_factor;AF_SAS=0.52;PHENOTYPE=Coronary_Artery_Disease\tGT\t1/1",
        "1\t55505689\trs11206510\tT\tC\t.\tPASS\tGENE=PCSK9;CLNSIG=risk_factor;AF_SAS=0.18;PHENOTYPE=LDL_Cholesterol_CAD\tGT\t0/1",
    ],
    "P004": [
        "10\t114758349\trs7903146\tC\tT\t.\tPASS\tGENE=TCF7L2;CLNSIG=risk_factor;AF_SAS=0.28;PHENOTYPE=Type_2_Diabetes\tGT\t0/1",
        "11\t2159775\trs5219\tC\tT\t.\tPASS\tGENE=KCNJ11;CLNSIG=risk_factor;AF_SAS=0.38;PHENOTYPE=Type_2_Diabetes\tGT\t1/1",
        "6\t20679709\trs9465871\tC\tT\t.\tPASS\tGENE=CDKAL1;CLNSIG=risk_factor;AF_SAS=0.31;PHENOTYPE=Type_2_Diabetes\tGT\t0/1",
        "9\t22125503\trs2383206\tG\tA\t.\tPASS\tGENE=CDKN2A_CDKN2B;CLNSIG=risk_factor;AF_SAS=0.52;PHENOTYPE=Heart_Failure_CAD\tGT\t0/1",
    ],
    "P007": [
        "9\t22125503\trs2383206\tG\tA\t.\tPASS\tGENE=CDKN2A_CDKN2B;CLNSIG=risk_factor;AF_SAS=0.52;PHENOTYPE=Myocardial_Infarction\tGT\t1/1",
        "1\t55505689\trs11206510\tT\tC\t.\tPASS\tGENE=PCSK9;CLNSIG=risk_factor;AF_SAS=0.18;PHENOTYPE=LDL_Cholesterol_MI\tGT\t1/1",
        "10\t114758349\trs7903146\tC\tT\t.\tPASS\tGENE=TCF7L2;CLNSIG=risk_factor;AF_SAS=0.28;PHENOTYPE=Type_2_Diabetes\tGT\t0/1",
        "13\t111348196\trs8050136\tC\tA\t.\tPASS\tGENE=FTO;CLNSIG=risk_factor;AF_SAS=0.22;PHENOTYPE=Obesity_T2D_risk\tGT\t0/1",
    ],
}


def write_clinical_notes():
    print("  Writing clinical notes (Bengali + Hindi)...")
    written = 0
    for pid, note in {**BENGALI_NOTES, **HINDI_NOTES}.items():
        lang = "bengali" if pid in BENGALI_NOTES else "hindi"
        fname = f"clinical_note_{pid}_{lang}.txt"
        if fname in SKIP_FILES:
            print(f"  ↳ Skipping {fname} (intentional missing modality)")
            continue
        path = os.path.join(OUTPUT_DIR, fname)
        with open(path, "w", encoding="utf-8") as f:
            f.write(note.strip())
        written += 1
    print(f"  ✓ {written} clinical notes written ({len(SKIP_FILES)} intentionally skipped)")


def write_lab_csvs():
    print("  Writing lab result CSVs (non-standard column names)...")
    for pid, data in LAB_DATA.items():
        fname = f"lab_results_{pid}.csv"
        if fname in SKIP_FILES:
            print(f"  ↳ Skipping {fname} (intentional missing modality)")
            continue
        path = os.path.join(OUTPUT_DIR, fname)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(data["headers"])
            writer.writerow(data["values"])
            # Write reference range row with marker
            ref_row = ["[REF_RANGE]"] + data["ref"][1:]
            writer.writerow(ref_row)
    print(f"  ✓ {len(LAB_DATA)} lab CSVs written")


def write_vcf_files():
    print("  Writing mock VCF files (P001, P004, P007)...")
    for pid, variants in VCF_VARIANTS.items():
        fname = f"variants_{pid}.vcf"
        path = os.path.join(OUTPUT_DIR, fname)
        with open(path, "w", encoding="utf-8") as f:
            f.write(VCF_TEMPLATE.format(sample_id=pid))
            for v in variants:
                f.write(v + "\n")
    print(f"  ✓ {len(VCF_VARIANTS)} VCF files written")


def main():
    print("\nBioHarmonize — Synthetic Data Generator")
    print("=" * 45)
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Patients: {len(PATIENTS)} | Disease focus: T2D + Cardiovascular (mixed)")
    print(f"Languages: Bengali ({len(BENGALI_NOTES)} patients), Hindi ({len(HINDI_NOTES)} patients)")
    print()

    write_clinical_notes()
    write_lab_csvs()
    write_vcf_files()

    print()
    print("Files generated:")
    files = sorted(os.listdir(OUTPUT_DIR))
    for f in files:
        if f != "generate_synthetic_data.py":
            size = os.path.getsize(os.path.join(OUTPUT_DIR, f))
            print(f"  {f:<50} ({size:,} bytes)")
    print()
    print("✓ Phase 2 complete — synthetic data ready for ingestion.")


if __name__ == "__main__":
    main()
