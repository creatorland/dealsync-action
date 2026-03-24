<classification_instructions>

Classify each email thread for an influencer/creator inbox. Return one JSON object per thread.

<recall_priority>
Your #1 goal is to NEVER miss a real deal. Apply this decision rule:
- If there is a 20% or greater chance this is a brand deal, classify as is_deal: true.
- If uncertain, classify as a deal with category "low_confidence". The creator can dismiss false positives instantly. They cannot recover deals they never saw.
</recall_priority>

<deal_definition>
A deal is when a brand, company, agency, platform, or fellow creator wants to work with this creator for their audience, content, reach, or influence. This includes but is not limited to:
- Sponsorships and paid brand collaborations
- Paid campaigns and content partnerships
- Product seeding, gifting, or PR packages with an implicit or explicit ask
- Affiliate offers and ambassador programs
- Event appearance, speaking, or hosting offers
- Paid placements, licensing, and usage rights agreements
- Creator-to-creator collaboration proposals
- Talent agency or management outreach

Classify as a deal regardless of status: new, active, declined, completed, or suspicious.
</deal_definition>

<deal_signals>
Before classifying each thread, evaluate these signals internally. Do NOT include your reasoning in the output.

Strong deal signals (any ONE = likely deal):
- Sender is from a brand, agency, PR firm, talent platform, or marketing company
- Email mentions: sponsorship, collaboration, partnership, campaign, ambassador, gifting, seeding
- Email references compensation: $, USD, payment, fee, budget, rate, gifting, complimentary, free product
- Email proposes a timeline, deadline, or deliverables for content
- Email references the creator's audience, followers, reach, engagement, or content
- Sender introduces themselves with a company title (e.g., "Partnerships Manager at...")
- Email contains a rate card request, media kit request, or asks about pricing
- Email mentions exclusivity, usage rights, or licensing terms
- Email comes from a platform that connects brands with creators (e.g., AspireIQ, Grin, CreatorIQ, etc.)

Weak signals (alone = low_confidence, combined = stronger):
- Generic "opportunity" or "proposal" language without specifics
- PR agency sending product news without an explicit ask
- Invitations to events without mention of compensation or content expectations
- Emails from unknown senders at corporate domains with vague subject lines
</deal_signals>

<not_a_deal>
These are NOT deals even if they come from companies:
- Investor, fundraising, or equity-related conversations
- Legal, accounting, or tax services
- Internal team discussions between the creator and their own staff
- Automated platform notifications (YouTube, Instagram, TikTok alerts, GMass, Mailchimp)
- User surveys, feedback requests, or NPS scores
- SaaS vendor pitches selling a tool TO the creator (unless they propose sponsoring the creator)
- Personal correspondence with friends or family
- Calendar-only threads with no business context
- Shipping, tracking, or order confirmations for personal purchases
- Social media follower/engagement notifications
- Password resets, security alerts, or account verification emails
- Subscription receipts or billing statements
</not_a_deal>

<scoring_guide>
ai_score (1-10) reflects urgency and value for the creator's attention:
- 9-10: Time-sensitive, high-value. Response needed today. Named brand, explicit budget, deadline approaching.
- 7-8: High-value, action needed within days. Active negotiation or strong offer from a recognized brand.
- 5-6: Active but no deadline. Ongoing conversation, no urgent action required.
- 3-4: Low priority. Early-stage inquiry, vague details, or low-value opportunity.
- 1-2: No action needed. Informational, completed, or declined.
</scoring_guide>

<categories>
- new: First contact or initial outreach, deal not yet discussed in depth
- in_progress: Active negotiation, terms being discussed, contracts in review
- completed: Deal closed, agreement signed, or deliverables fulfilled
- not_interested: Creator declined or explicitly not pursuing
- likely_scam: Suspicious patterns: no company website, too-good-to-be-true offers, requests for personal info, payment-before-work schemes
- low_confidence: Cannot determine with confidence whether this is a real deal
</categories>

<deal_types>
When is_deal is true, assign one of:
- brand_collaboration: General brand partnership or sponsored content
- sponsorship: Explicit paid sponsorship of content or channel
- affiliate: Commission-based or referral link arrangement
- product_seeding: Product gifting or PR package (with or without obligation)
- ambassador: Ongoing brand ambassador or rep program
- content_partnership: Creator-to-creator or media company content collaboration
- paid_placement: Paid product placement, licensing, or usage rights
- other_business: Business opportunity that doesn't fit the above
</deal_types>

<ai_summary_instructions>
The ai_summary field (max 1000 chars) is a context memo for the NEXT AI evaluation of this thread. It is the ONLY context available when new emails arrive later. Write it as a factual briefing:
- Who: Names, emails, roles/titles, company names of all participants
- What: Exact proposal or ask (be specific about deliverables and format)
- Status: Current state of negotiation or conversation
- Terms: Any compensation, rates, or budget mentioned (exact figures)
- Dates: Deadlines, campaign dates, or response-by dates
- Red flags: Anything suspicious or noteworthy for future evaluation
</ai_summary_instructions>

<previous_summary_handling>
When a thread includes a "Previous AI Summary", that summary reflects a prior evaluation. New emails in the thread may change the classification. Re-evaluate the entire thread from scratch, using the prior summary as background context only. The new emails take priority over the prior summary if they conflict.
</previous_summary_handling>

<language_detection>
If the primary language of the thread is not English, set is_english to false and language to the ISO 639-1 code. Non-English threads can absolutely be deals. Classify them using the same rules.
</language_detection>

</classification_instructions>

<examples>

<example index="1" scenario="Clear brand sponsorship offer">
<thread_summary>
From: sarah@beautybrandx.com (Sarah Kim, Partnerships Manager)
Subject: "Sponsored YouTube Video Opportunity - Beauty Brand X"
Body: Introduces herself, says they love the creator's skincare content, proposes a dedicated YouTube video reviewing their new serum line. Mentions $2,500 budget, 60-day exclusivity, and asks for the creator's rate card.
</thread_summary>
<correct_output>
{"thread_id": "example_1", "is_deal": true, "is_english": true, "ai_score": 8, "category": "new", "likely_scam": false, "ai_insight": "Beauty Brand X offers $2.5K for sponsored YouTube review", "ai_summary": "Sarah Kim (sarah@beautybrandx.com, Partnerships Manager, Beauty Brand X) proposes a sponsored dedicated YouTube video reviewing their new serum line. Budget: $2,500. Terms: 60-day exclusivity. Requested creator's rate card. Status: initial outreach, awaiting creator response.", "main_contact": {"name": "Sarah Kim", "email": "sarah@beautybrandx.com", "company_name": "Beauty Brand X", "title": "Partnerships Manager", "phone_number": null}, "deal_brand": "Beauty Brand X", "deal_type": "sponsorship", "deal_name": "Beauty Brand X YouTube Review", "deal_value": 2500, "deal_currency": "USD"}
</correct_output>
</example>

<example index="2" scenario="Automated notification, not a deal">
<thread_summary>
From: noreply@youtube.com
Subject: "Your channel just hit 100K subscribers!"
Body: Congratulates the creator on reaching 100K subscribers. Contains a link to order a Silver Play Button. No brand outreach, no business proposal.
</thread_summary>
<correct_output>
{"thread_id": "example_2", "is_deal": false, "is_english": true, "ai_score": 1, "category": null, "likely_scam": false, "ai_insight": "YouTube milestone notification, not a business opportunity", "ai_summary": "Automated YouTube notification congratulating creator on 100K subscribers. No deal content.", "main_contact": null, "deal_brand": null, "deal_type": null, "deal_name": null, "deal_value": null, "deal_currency": null}
</correct_output>
</example>

<example index="3" scenario="Ambiguous SaaS pitch with sponsorship angle">
<thread_summary>
From: mike@editortoolpro.com (Mike Chen, Head of Growth)
Subject: "Collab opportunity? Love your editing tutorials"
Body: Mike says he's a fan of the creator's video editing tutorials. He runs EditorToolPro, a video editing SaaS. He says "We'd love to explore a partnership - whether that's a sponsored tutorial, an affiliate deal, or just getting your honest feedback on our tool. Happy to discuss rates." Also mentions they have a free Pro license for the creator.
</thread_summary>
<correct_output>
{"thread_id": "example_3", "is_deal": true, "is_english": true, "ai_score": 6, "category": "new", "likely_scam": false, "ai_insight": "EditorToolPro proposes sponsorship or affiliate deal for editing tutorials", "ai_summary": "Mike Chen (mike@editortoolpro.com, Head of Growth, EditorToolPro) proposes partnership: sponsored tutorial, affiliate deal, or product review. Offers free Pro license. Open to discussing rates. Status: initial outreach, no specific budget mentioned yet.", "main_contact": {"name": "Mike Chen", "email": "mike@editortoolpro.com", "company_name": "EditorToolPro", "title": "Head of Growth", "phone_number": null}, "deal_brand": "EditorToolPro", "deal_type": "brand_collaboration", "deal_name": "EditorToolPro Partnership", "deal_value": null, "deal_currency": null}
</correct_output>
</example>

<example index="4" scenario="Product gifting with no explicit ask">
<thread_summary>
From: pr@luxfashionhouse.com (Ava Reyes, PR Coordinator)
Subject: "A gift from Lux Fashion House"
Body: Says they're sending the creator their new handbag from the spring collection, no strings attached. Provides a tracking number. Mentions "We'd love to see it on your feed but totally no pressure." No rate discussion, no contract.
</thread_summary>
<correct_output>
{"thread_id": "example_4", "is_deal": true, "is_english": true, "ai_score": 4, "category": "new", "likely_scam": false, "ai_insight": "Lux Fashion House sending gifted handbag, implicit content expectation", "ai_summary": "Ava Reyes (pr@luxfashionhouse.com, PR Coordinator, Lux Fashion House) sending gifted handbag from spring collection. No formal ask or contract, but mentions 'love to see it on your feed.' Tracking number provided. Status: product sent, no formal terms.", "main_contact": {"name": "Ava Reyes", "email": "pr@luxfashionhouse.com", "company_name": "Lux Fashion House", "title": "PR Coordinator", "phone_number": null}, "deal_brand": "Lux Fashion House", "deal_type": "product_seeding", "deal_name": "Lux Fashion House Gifted Handbag", "deal_value": null, "deal_currency": null}
</correct_output>
</example>

<example index="5" scenario="Likely scam email">
<thread_summary>
From: partnership@brand-deals-agency.xyz
Subject: "URGENT: $10,000 Brand Deal - Response Needed Today"
Body: Claims to represent "multiple Fortune 500 brands" but doesn't name any. Offers $10,000 for a single Instagram story. Asks the creator to click a link and "verify your PayPal" to receive payment. No company website, no LinkedIn, sender domain registered 2 weeks ago.
</thread_summary>
<correct_output>
{"thread_id": "example_5", "is_deal": true, "is_english": true, "ai_score": 2, "category": "likely_scam", "likely_scam": true, "ai_insight": "Suspicious: unnamed brands, unrealistic payout, asks for PayPal verification", "ai_summary": "Unknown sender (partnership@brand-deals-agency.xyz) claims to rep Fortune 500 brands without naming them. Offers $10K for single IG story. Asks creator to verify PayPal via link. Red flags: no specific brand named, recently registered domain (.xyz), unrealistic compensation, urgency pressure, payment verification request.", "main_contact": {"name": null, "email": "partnership@brand-deals-agency.xyz", "company_name": null, "title": null, "phone_number": null}, "deal_brand": null, "deal_type": "sponsorship", "deal_name": "Unknown - Likely Scam", "deal_value": 10000, "deal_currency": "USD"}
</correct_output>
</example>

</examples>

<output_schema>
Return a JSON array with exactly one object per thread in the input.

Fields:
- thread_id (string, required): The thread_id from the input
- is_deal (boolean, required): true if this is or could be a brand deal
- is_english (boolean, required): true if the primary language is English
- language (string, optional): ISO 639-1 code if is_english is false
- ai_score (integer 1-10, required): Priority score for creator attention
- category (string, required if is_deal is true, null if false): new | in_progress | completed | not_interested | likely_scam | low_confidence
- likely_scam (boolean, required): true if suspicious patterns detected
- ai_insight (string, required): One-line summary. If deal: what the deal is. If not: why it's not.
- ai_summary (string, required, max 1000 chars): Context memo for next AI evaluation. Include participants, proposal, status, terms, dates.
- main_contact (object or null): Primary external person. Fields: name, email, company_name, title, phone_number. Null if none identified or if is_deal is false.
- deal_brand (string or null): Brand or company name. Null if is_deal is false.
- deal_type (string or null): One of the deal types listed above. Null if is_deal is false.
- deal_name (string or null): Short descriptive name for this deal. Null if is_deal is false.
- deal_value (number or null): Monetary value if mentioned. Null otherwise.
- deal_currency (string or null): ISO 4217 currency code if deal_value is present. Null otherwise.
</output_schema>

<rules>
- Respond ONLY with the JSON array. No markdown, no explanation, no code fences.
- One entry per thread_id in the input.
- If is_deal is false: set category, deal_brand, deal_type, deal_name, deal_value, deal_currency, and main_contact to null.
- If is_deal is true: deal_brand, deal_type, and deal_name are required. deal_value and deal_currency only if mentioned.
- ai_summary is ALWAYS required regardless of is_deal.
- When in doubt about is_deal, default to true with category "low_confidence".
</rules>
