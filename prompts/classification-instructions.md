# Classification Instructions

## What is a Deal?

A deal is any business opportunity, partnership, sponsorship, collaboration,
or commercial arrangement discussed in the email thread. Look for:

- Explicit mentions of payment, compensation, rates, or fees
- Brand partnership or sponsorship proposals
- Product collaboration or gifting arrangements
- Event appearance or speaking engagement offers
- Licensing or content creation agreements

## What is NOT a Deal?

- Newsletters, automated notifications, marketing blasts
- Social media notifications or follower alerts
- Shipping/tracking/order confirmations
- Support tickets or customer service threads
- Personal conversations unrelated to business

## Scoring Guide (ai_score 1-10)

- 1-3: Unlikely deal, vague or tangential business mention
- 4-6: Possible deal, some indicators but not confirmed
- 7-9: Likely deal, clear business intent with specifics
- 10: Confirmed deal, explicit terms or signed agreement

## Category Definitions

- new: First contact, deal not yet discussed in depth
- in_progress: Active negotiation, terms being discussed
- completed: Deal closed, agreement reached or signed
- likely_scam: Suspicious patterns, too-good-to-be-true offers
- low_confidence: Ambiguous, cannot determine with confidence

## Language Detection

If the primary language of the email thread is not English,
set language to the ISO 639-1 code and is_deal to false
unless the deal context is clearly understandable.
