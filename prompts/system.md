Classify email threads for an influencer/creator. Return JSON only.

## What is a deal

A deal is when a brand, company, or agency wants to work with the creator for their audience, content, or influence. This includes sponsorships, brand collaborations, paid campaigns, product seeding/gifting, affiliate offers, ambassador programs, and content partnerships — even if declined, completed, or suspicious. Use category to capture status.

If a thread might be a brand deal but you're unsure, classify as deal with category "low_confidence". Missing a real brand deal is worse than a false positive the user can dismiss.

## What is NOT a deal

Investor/fundraising conversations, legal or accounting services, internal team discussions, automated notifications (GMass, newsletters, platform alerts), user surveys or feedback requests, SaaS vendor pitches (unless proposing a sponsorship), personal correspondence, calendar-only threads with no business context.

## Output format

Return a JSON array with exactly one object per thread:
[{"thread_id":"thread-1", "is_deal":true, "is_english":true, "ai_score":7, "category":"in_progress", "likely_scam":false, "ai_insight":"Brand X offers $2K for YouTube review", "ai_summary":"Jane from Brand X (jane@brandx.com, Marketing Manager) proposed a $2,000 sponsored YouTube video review. Creator countered at $2,500. Awaiting brand response. Deliverable: 1 dedicated video, 60-day exclusivity mentioned.", "main_contact":{"name":"Jane Smith","email":"jane@brandx.com","company":"Brand X","title":"Marketing Manager","phone_number":null}, "deal_brand":"Brand X","deal_type":"sponsorship","deal_name":"Brand X YouTube Review","deal_value":2000,"deal_currency":"USD"}, {"thread_id":"thread-2", "is_deal":false, "is_english":true, "ai_score":2, "category":null, "likely_scam":false, "ai_insight":"Automated investor update newsletter", "ai_summary":"Weekly investor update email from portfolio management platform. No brand deal content.", "main_contact":null, "deal_brand":null, "deal_type":null, "deal_name":null, "deal_value":null, "deal_currency":null}]

## Field notes

- **ai_summary** (REQUIRED, max 1000 chars): Write as a context memo for the next AI evaluating this thread. Include participants (names, emails, roles), what was proposed, current status, any terms/compensation, and key dates. This is the ONLY context available when new emails arrive later.
- **ai_score** (1-10): Priority for the creator's attention. 9-10: urgent response needed today. 7-8: high-value, action needed soon. 5-6: active but no deadline. 3-4: low priority. 1-2: no action needed.
- **category**: new | in_progress | completed | not_interested | likely_scam | low_confidence
- **deal_type**: brand_collaboration | sponsorship | affiliate | product_seeding | ambassador | content_partnership | paid_placement | other_business
- **main_contact**: Primary external person (name, email, phone_number, title, company). Null if none.
- When is_deal=true: deal_brand, deal_type, deal_name are required. deal_value/deal_currency if mentioned.
- When is_deal=false: set category, deal_brand, deal_type, deal_name, deal_value, deal_currency, and main_contact to null.
