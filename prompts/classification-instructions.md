# Classification Instructions

Classify email threads for an influencer/creator. If a thread might be a brand deal but you're unsure, classify as a deal with category "low_confidence". Missing a real brand deal is worse than a false positive the user can dismiss.

## What is a Deal?

A deal is when a brand, company, or agency wants to work with the creator for their audience, content, or influence. This includes:

- Sponsorships and brand collaborations
- Paid campaigns and content partnerships
- Product seeding/gifting arrangements
- Affiliate offers and ambassador programs
- Event appearance or speaking engagement offers
- Paid placements and licensing agreements

Even if declined, completed, or suspicious — classify as a deal. Use category to capture status.

## What is NOT a Deal?

- Investor/fundraising conversations
- Legal or accounting services
- Internal team discussions
- Automated notifications (GMass, newsletters, platform alerts)
- User surveys or feedback requests
- SaaS vendor pitches (unless proposing a sponsorship)
- Personal correspondence
- Calendar-only threads with no business context
- Shipping/tracking/order confirmations
- Social media notifications or follower alerts

## Scoring Guide (ai_score 1-10)

Priority for the creator's attention:
- 9-10: Urgent response needed today
- 7-8: High-value, action needed soon
- 5-6: Active but no deadline
- 3-4: Low priority
- 1-2: No action needed

## Category Definitions

- **new**: First contact, deal not yet discussed in depth
- **in_progress**: Active negotiation, terms being discussed
- **completed**: Deal closed, agreement reached or signed
- **not_interested**: Creator declined or not pursuing
- **likely_scam**: Suspicious patterns, too-good-to-be-true offers
- **low_confidence**: Ambiguous, cannot determine with confidence

## Deal Type Values

When `is_deal` is true, use one of:
- `brand_collaboration`
- `sponsorship`
- `affiliate`
- `product_seeding`
- `ambassador`
- `content_partnership`
- `paid_placement`
- `other_business`

## AI Summary Guidelines

The `ai_summary` field (max 1000 chars) is a context memo for the next AI evaluating this thread. Include:
- Participants (names, emails, roles)
- What was proposed
- Current status
- Any terms/compensation mentioned
- Key dates or deadlines

This is the ONLY context available when new emails arrive later — make it count.

## Previous AI Summary

When a "Previous AI Summary" is provided for a thread, it means this thread was evaluated before and new emails have arrived. The summary is prior context — but new emails may change the classification. Re-evaluate the thread fully considering the new information.

## Language Detection

If the primary language of the email thread is not English, set language to the ISO 639-1 code. Non-English threads can still be deals if the context is clearly understandable.
