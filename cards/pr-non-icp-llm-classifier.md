# PR review card — Non-ICP V2, the model layer

**Branch:** `feat/non-icp-llm-classifier` · **14 Sept 2026** · **Not merged.**
**Ships behind three flags, all default OFF.** Merging changes nothing at all
until one of them is set on Railway.

Written for someone who was not in the session. Everything below was either run
or is flagged as not run.

---

## 0. Read this first — the recommendation, and why

**Do not switch the block on. Ship flagging + Meta suppression and read a week
of Slack posts**, which is the second of the two options in the brief.

The decision rule was: *zero real customers flagged, and nothing obviously good
in group 2, and we go live blocking.* Measured over **4,701 domains from 5,251
leads**, with all three models scoring identically on the number that matters:

| | |
|---|---|
| Paying customers the **model** flagged | **6** |
| …of those, caught by the known-customer bypass before any block | **5** |
| …**surviving the bypass** | **1** — `allstate.com`, which V1 already blocks today |
| Booked-and-showed demos flagged (unanimous across all three models) | **124 of 1,407 — 8.8%** |

So it is not zero. It is *nearly* zero at the system level, and the one survivor
adds no new risk. But the second condition is the one that fails: **124 people
who booked a demo and turned up would have been turned away**, and I do not
think anyone should flip a block on that number without reading the list. It is
in section 5, in full.

**This is a judgement call, not a blocker.** If you read the 124 and they are
the population Swapnil meant, the block is one environment variable away and
nothing in this PR has to change.

---

## 1. What it does

Reads the company's own website, classifies it into **one enumerated
`business_type`**, and blocks exactly two of them: `real_estate` and
`insurance`.

**Both mechanisms run, permanently, and the brand-domain list goes FIRST.**

```
verdict = domain_list_hit          (fast, exact, works on unreadable sites)
       OR llm_cached_verdict       (covers the long tail V1 cannot reach)
```

| | brand-domain list | model layer |
|---|---|---|
| National brand on its own domain | **exact** | **often cannot read the page** |
| Independent agency on its own domain | blind | **covers it** |
| Realtor on a personal-brand domain | blind | **covers it** |
| Site returns 403 to a scraper | **still works** | no verdict |
| A human can re-derive the decision | yes, read the list | no |

The ticket's claim that V1 should be *retired* was ours, not Swapnil's, and it
is corrected in `docs/tickets/non-icp-v1-block.md` with a dated section saying
so. The measurement is in section 4.

---

## 2. The three flags, and why there are three

| Flag | Default | What it does |
|---|---|---|
| `NON_ICP_LLM_ENABLED` | off | classify and store. Stamps `non_icp_llm_flagged`. Changes nothing a visitor or an SDR can see. |
| `NON_ICP_LLM_META` | off | a flagged lead stops firing `StartTrial` / `Lead` / `Schedule`. |
| `NON_ICP_LLM_BLOCK` | off | a flagged lead is refused at `/submit` and at all three booking routes. |

**META is separate from ENABLED on purpose.** `CLAUDE.md`: suppressing a Meta
event is a real cost and a decision to surface, never a side effect. Switching
on observation must not quietly reshape an ad audience — that rule, applied to
the switch itself.

**BLOCK implies META**, because a lead we turn away that still feeds the
algorithm a conversion is incoherent.

**Read `suppress_meta` off the verdict, never `blocked`.** A flagged lead is not
blocked and still has to stop firing Meta when META is on. Three call sites read
it, and a test drives all three.

---

## 3. What I did NOT change

- **Neither form file.** `gushwork-form.js` and `gushwork-form-popup.js` are
  untouched. The form already calls `/non-icp-check` three times per lead —
  email blur at step 1, the step-1 Next click, website blur at step 2 — all
  wired for V1. The model warm hangs off those existing calls. **So there is no
  Webflow re-pin and no Ads-fork port**, which is the drift that cost twelve
  days of Google Ads leads in August.
- **`leads.disqualified`.** Untouched, as in V1.
- **The AWS mirror.** `non_icp_llm_flagged` is **not** synced to
  `gw_form_leads`. Deliberate: a flagged-not-blocked lead *should* still be
  dialled, so shipping the column before there is a consumer would be
  premature. An LLM **block** sets `non_icp_blocked`, which already mirrors, so
  nothing breaks. Flag it if you want it.
- **The dashboard**, beyond one corrected line. No new tab, no new card. An LLM
  block lands on the existing Blocked tab because it sets `non_icp_blocked`.
  The one change: the Blocked tab's "Wrong? turn off `NON_ICP_BLOCK`" hint was
  actively misleading for a model block, which needs a different variable.

---

## 4. The evidence

### 4a. Paying customers — the gold standard

Six flagged. Five are bypassed by the known-customer check, which runs **before**
any block and reads the same three warehouse tables production reads.

| domain | judged | conf | company | bypassed? |
|---|---|---|---|---|
| `allstate.com` | insurance | 0.99 | JACOBS FAMILY INSURANCE @ALLSTATE, Visin Insurance, Milton-Scalise | **no** |
| `sspins.com` | insurance | 0.95 | Sterling Seacrest Pritchard | yes |
| `vellumlifegroup.com` | insurance | 0.95 | Vellum Life Group | yes |
| `garylifeindex.com` | insurance | 0.95 | OOC Unlimited / Gary Cosby | yes |
| `nomadgroup.io` | real_estate | 0.95 | Nomad | yes |
| `yourhealthyourmoneyaz.com` | insurance | 0.95 | Your Health Your Money Az | yes |

Two things worth saying out loud:

1. **`allstate.com` is not a new risk.** It is on `NON_ICP_DOMAINS` and V1
   blocks it today. The customer behind it, `nedjacobs@allstate.com`, gives
   website `jacobsfamilyinsurance.net`, which **is** in the bypass set — so
   that lead is saved by the website, not by the email.
2. **Five of our paying customers are insurance agencies or a real-estate
   brokerage.** The classifier is not wrong about them; it is right, and they
   bought anyway. That is a fact about ICP, not a bug, and it is the strongest
   single argument for reading the 124 before blocking.

**The bypass fails open on a warehouse timeout.** So during an outage the
exposure is the first column (6), not the second (1). Both numbers are in the
report for that reason.

### 4b. Recall against the mechanism we already trust — and why V1 stays

The brand-domain list matches **65** domains in our history. Re-measured with an
uncontended scrape (one domain at a time, so the result is the ceiling rather
than my laptop's throughput):

| | |
|---|---|
| Readable at all | **37 of 65** |
| Caught when readable | **36 of 37 — 97%**, identical on all three models |
| Overall recall | **36 of 65 — 55%** |

**The entire gap is the scraper, not the judgement.** The 28 unreadable domains
fail mostly on **HTTP 403** — sites actively refusing a scraper — and they
include `kw.com`, `exprealty.com`, `foxroach.com`, `bhhsrmr.com`,
`allstateagencies.com`, `farmersagency.com`, `lptrealty.com`, `farmersagent.com`
and `remax.net`.

**Retiring V1 would have un-blocked 28 of the 65 domains it covers**, including
three of the four real blocks from the first three days live. That is the
measurement behind the correction to the ticket.

The single readable miss is `stablewealthcorp.com` → `financial_advisory` at
0.85, which is arguably **correct**: the Non-ICP doc keeps financial advisory in
ICP by name.

### 4c. Booked and showed up — the number to read

| model | flagged, of 1,407 |
|---|---|
| claude-haiku-4-5 | 134 |
| claude-sonnet-5 | 130 |
| claude-opus-5 | 128 |
| **unanimous across all three** | **124** |

67 insurance, 57 real estate. Reading them, the great majority are exactly the
population the feature is for — State Farm agents, Coldwell Banker, Keller
Williams, Compass, eXp, RE/MAX, Century 21, Sotheby's, Berkshire Hathaway,
independent agencies. **They are correctly classified.** The question is not
whether the model is right; it is whether we want to turn away 124 people who
booked and turned up.

A handful I would look at specifically, because they are the boundary cases:

| domain | judged | why it is worth a look |
|---|---|---|
| `upcover.com` | insurance | reads like insurtech — a software company selling **to** insurers |
| `gregoryappel.com` | insurance | large commercial insurance brokerage, not a captive agent |
| `munerisbenefits.com`, `franklinbenefitsgroup.com`, `bizwellbenefits.com` | insurance | employee-benefits consulting; the prompt says advice-led benefits firms are `b2b_services` |
| `lee-associates.com`, `naisavannah.com`, `oxfordcres.com`, `fortisnetlease.com` | real_estate | commercial real estate / net lease, a different business from a residential agent |

### 4d. Everything else

**132 unanimous flags of 1,408 classified.** 62 insurance, 70 real estate. No
label, so it proves nothing — it is the count you asked for.

**262 unanimous flags across all 4,701 domains — 8.9% of the 2,934 that could
be read.**

### 4e. Model choice

The three agree far more than I expected. On identical page bytes:

| | |
|---|---|
| G1 flags | 6, 6, 6 — identical |
| G2 flags | 134 / 130 / 128, with **124 unanimous** |
| Brand-list recall | identical |
| Total domains where blocking disagrees | **14 of 4,701** |

The disagreements cluster on one boundary: **insurance vs financial_advisory** —
Medicare advisors, retirement planners, `equitable.com` ("Equitable Advisors").
That is the doc's own in-ICP row, so it is the right thing to be uncertain about.

**The separating result is calibration, not accuracy:**

| model | real-estate/insurance classifications | held back by the 0.75 confidence floor |
|---|---|---|
| claude-haiku-4-5 | 281 | **1** |
| claude-sonnet-5 | 300 | 25 |
| claude-opus-5 | 295 | **33** |

**Haiku's confidence is not usable as a safety valve** — it says ≥0.75 almost
always, so the floor does nothing. Opus is the most calibrated, Sonnet close
behind.

**Recommendation: `claude-sonnet-5`**, which is the shipped default. It matches
Opus on every accuracy number, its confidence still does real work, and it is
faster in a warm path a visitor may be waiting behind. Opus is the conservative
choice if you would rather the floor hold back more.

### 4f. Cost

Measured on real `usage` from a 40-page sample of the same pages, multiplied by
the actual number of model calls — **an estimate built from measurement, not an
invoice.**

| model | $/call | mean in / out tokens | **steady state, 30 new domains/day** |
|---|---|---|---|
| claude-haiku-4-5 | $0.0031 | 2,401 / 138 | $0.09/day — **$2.82/month** |
| claude-sonnet-5 | $0.0083 | 3,552 / 117 | $0.25/day — **$7.55/month** |
| claude-opus-5 | $0.0204 | 3,552 / 105 | $0.61/day — **$18.59/month** |

30/day is measured, not assumed: **~28 genuinely new email domains a day** over
the last 21 days. Per-domain caching means lead volume is not the driver — the
new-domain rate is.

**What this validation run cost: ~$96.** 2,934 readable domains × 3 models —
$9.07 Haiku, $24.28 Sonnet, $59.82 Opus — plus about $3 for the recall re-runs,
the live smoke tests and the cost sample itself. The 1,643 Haiku calls that
400'd on `effort` cost nothing, which is part of why nobody would have noticed.

A **full one-time rescan** on a future prompt version is the same shape: about
**$24 on Sonnet**, and it is the scrape, not the model, that takes the time.

---

## 5. The full list of 124

Every domain all three models agreed to flag **that booked a demo and showed up**.
Sorted by judged type. The quote is verbatim from the page and is what makes a
wrong flag falsifiable in one glance.

| # | domain | judged | conf | company | what the page said |
|---|---|---|---|---|---|
| 1 | `alliedhealth-agency.com` | insurance | 0.95 | Allied Health Agency | Pivotal Concepts, Inc. is a licensed health insurance agency that does business as Allied Healt |
| 2 | `ariglobal.com` | insurance | 0.95 | ARI Global, Inc. | ARI Global is a specialty Broker of Trade Credit Insurance. |
| 3 | `arisehealthinsurance.com` | insurance | 0.95 | Arise Health Insurance | Independent Insurance Advisor VP of Sales, US · Licensed in 21 States Your Trusted Guide to Hea |
| 4 | `atxinsure.com` | insurance | 0.98 | Venkat viswanathan agency | Insurance Products Offered Auto, Homeowners, Condo, Renters, Personal Articles, Business, Life, |
| 5 | `bankerslife.com` | insurance | 0.95 | Bankers Life | Bankers Life and Casualty Company, Washington National Insurance Company, Colonial Penn Life In |
| 6 | `beaconlightinsurance.com` | insurance | 0.98 | Beacon Light Insurance | Beacon Light Insurance is a 5-Star rated independent insurance agency |
| 7 | `bginsuranceagency.com` | insurance | 0.95 | Bg insurance | BG Insurance Agency LLC is an independent insurance agency located in Tucson, Arizona. We are c |
| 8 | `billrawlings.net` | insurance | 0.98 | Bill Rawlings | State Farm Insurance Agent Bill Rawlings Serving - NJ, PA, DE & NY |
| 9 | `bizwellbenefits.com` | insurance | 0.95 | BizWell Benefits, LLC | Welcome to BizWell Benefits, LLC Your Health Our Priority With our innovative approach and dedi |
| 10 | `black-swan-insurance-group.com` | insurance | 0.98 | Black Swan Insurance Group | Insurance Agents & Brokers / Black Swan Insurance Group |
| 11 | `claxtoninsurance.com` | insurance | 0.98 | Claxton Insurance Company | Welcome to Claxton Insurance Agency, LLC |
| 12 | `connor-mcclure.com` | insurance | 0.99 | Cmis / Connor mccclure | Connor-McClure Insurance Services offers a variety of personal insurance, business insurance, s |
| 13 | `corpfi.com` | insurance | 0.92 | CorpFi | Income Protection Specialists for Physicians & Executives / CorpFi |
| 14 | `daliaharris.com` | insurance | 0.95 | Dalia Harris | licensed independent health insurance advisor helping Arizonans navigate Medicare with confiden |
| 15 | `dossofinancial.com` | insurance | 0.95 | Madoussou dosso | Licensed Independent Life Insurance Producer serving Maryland clients. |
| 16 | `eastcoastinspro.com` | insurance | 0.95 | East Coast Insurance Providers | We were founded to be YOUR Independent Agent, in YOUR community. We take pride in our community |
| 17 | `edcookinsurance.com` | insurance | 0.98 | Ed Cook, State Farm Insurance Agent | I'm Ed Cook, your State Farm Agent in Lilburn, GA... Car, Home, Condo, Renters, Personal Articl |
| 18 | `esmfinancials.com` | insurance | 0.95 | ESM Financials | Your Trusted Life Insurance Advisor 11 years of experience delivering personalized advice and t |
| 19 | `faisinc.com` | insurance | 0.95 | Falcon & Associates Insurance Services, Inc. | Falcon Associates Insurance Services specializes in trucking, contractors general liability ins |
| 20 | `farmers.com` | insurance | 0.99 | Mandi Bedbury Agency - Farmers Insurance / Farmers Insu | Insurance Quotes for Home, Auto, & Life : Farmers Insurance |
| 21 | `fflmiddleamerica.com` | insurance | 0.95 | Middle America Financial | At Middle America Financial we are a broker agency made up of independent agents across the US. |
| 22 | `franklinbenefitsgroup.com` | insurance | 0.92 | Franklin Benefits Group, LLC | Franklin Benefits Group is The Premier Health Insurance Broker in Jamison and Doylestown Bucks  |
| 23 | `geico.com` | insurance | 0.99 | GEICO | An Insurance Company For Your Car And More / GEICO |
| 24 | `gregoryappel.com` | insurance | 0.85 | Gregory & Appel | Risk Management Insurance Advisors / Gregory Appel Capabilities For My Business Captives Risk R |
| 25 | `healthmarkets.com` | insurance | 0.98 | Erica Rae Inc / HealthMarkets, Inc. | HealthMarkets Insurance Agency, Inc. is licensed as an insurance agency nationwide |
| 26 | `healthmarketsjax.com` | insurance | 0.95 | HealthMarkets, Inc. | Licensed Insurance Agent – HealthMarkets Insurance Agency |
| 27 | `hollowtree.us` | insurance | 0.95 | Hollowtree | The specialist enrollment firm for DI and LTC...Group disability insurance and guaranteed-issue |
| 28 | `impactogloballatino.com` | insurance | 0.95 | IMPACTO GLOBAL LATINO | Ofrecemos seguros diseñados para tu vida y tu familia. Nuestro compromiso es cuidarte en cada p |
| 29 | `kertleblanc.com` | insurance | 0.98 | State Farm | State Farm® Insurance Agent Kert LeBlanc Kert LeBlanc Ins Agcy Inc Insurance Products Offered A |
| 30 | `kinshipsolutions.com` | insurance | 0.98 | Kinship Solutions Group | Nationwide Independent Insurance Broker · A+ Rated... Mortgage Protection Insurance / Kinship I |
| 31 | `lifecarelocal.com` | insurance | 0.95 | Life Care benefit services | We help families, individuals, and business owners protect their future with simple, affordable |
| 32 | `loriphenry.com` | insurance | 0.95 | State Farm | As a second-generation State Farm agent, my connection to this company runs deep...became a Sta |
| 33 | `lorrainesmithinsurance.life` | insurance | 0.92 | lorraine Smith Insurance | Whole Life Insurance Term Life Insurance Indexed Universal Life (IUL) Insurance Living Benefits |
| 34 | `lowppo.com` | insurance | 0.92 | PPO Exchange | We will find insurance plans for the entire family to help cover serious medical emergencies. |
| 35 | `lphenry.com` | insurance | 0.98 | State Farm | As a second-generation State Farm agent, my connection to this company runs deep. I grew up in  |
| 36 | `medigap4seniors.com` | insurance | 0.95 | Medigap4Seniors.com | Licensed, local Medicare Supplement agency serving Estero, Bonita Springs, Naples Fort Myers. |
| 37 | `middleamericafinancial.com` | insurance | 0.92 | Middle America Financial | At Middle America Financial we are a broker agency made up of independent agents across the US. |
| 38 | `minnesotabusinessinsurance.com` | insurance | 0.95 | Minnesota Business Insurance | MinnesotaBusinessInsurance.com is powered by a local independent insurance agency, not a call c |
| 39 | `munerisbenefits.com` | insurance | 0.95 | Muneris Benefits | As an insurance brokerage, we provide benefits solutions for businesses, individuals transition |
| 40 | `mustardseedhealthinsurance.com` | insurance | 0.95 | Mustard Seed Health and Life Insurance Group, LLC | we specialize in providing families and businesses across the DFW Metro Area, Texas, Colorado,  |
| 41 | `navsav.com` | insurance | 0.98 | NavSav Insurance | Welcome to NavSav Insurance An insurance policy is often the only thing keeping you from financ |
| 42 | `newmantucker.com` | insurance | 0.99 | Newman & Tucker Insurance | Newman Tucker Insurance is a reputable, trusted, and highly reviewed insurance agency that offe |
| 43 | `newyorklife.com` | insurance | 0.99 | New York Life | As the largest mutual insurer in the U.S., we operate for the benefit of policy owners |
| 44 | `nsurehub.com` | insurance | 0.85 | Nsurehub | Homeowners Insurance Policy Online Fast / Nsurehub |
| 45 | `ppoexchange.com` | insurance | 0.85 | PPO Exchange | PPO Exchange is an independent marketplace that helps consumers explore PPO health insurance co |
| 46 | `preferredia.com` | insurance | 0.98 | Preferred Insurance Agency | We have years of insurance experience helping clients prepare for the unknown. Ask us about: Au |
| 47 | `protectedbysarah.life` | insurance | 0.95 | Globe Life | Sarah Flor — Globe Life Insurance Agent / protectedbysarah.life Sarah Flor About Coverage Our T |
| 48 | `serenityhealthadvisors.com` | insurance | 0.95 | Serenity Health Advisors | Medicare Advantage Plans, Medicare Supplement Insurance Plans, Medicare Part D Prescription Dru |
| 49 | `shatainsurance.com` | insurance | 0.95 | Shata inaurance | A family company, providing access to quality, personalized healthcare throughout the nation |
| 50 | `soundibg.com` | insurance | 0.95 | Sound Insurance Brokerage Group | Sound Insurance Brokerage Group Insurance Solutions for Businesses and Individuals |
| 51 | `southsuburbaninsurance.com` | insurance | 0.95 | Scott Neil State Farm Insurance Agency | Insurance Products Offered Auto, Homeowners, Condo, Renters, Personal Articles, Business, Life, |
| 52 | `statefarm.com` | insurance | 0.99 | Ray Kelly Insurance agency inc / State Farm Agent / Sco | State Farm® / An Insurance Company Valued For Over 100 Years |
| 53 | `sweetbeeinsurancegroup.com` | insurance | 0.95 | Sweetbee Insurance Group | At Sweetbee Insurance Group, we work for you - not the insurance companies - to find the best c |
| 54 | `terrapininsurance.com` | insurance | 0.98 | Terrapin Insurance Group | Trusted Independent Insurance Broker · Est. 2011 We shop the nation's top carriers so you don't |
| 55 | `thekrenningagency.com` | insurance | 0.98 | The Krenning Agency | Mississippi insurance, made personal Coverage guidance shaped around the way you live, work, dr |
| 56 | `themagnoliaagencyinc.com` | insurance | 0.95 | The Magnolia Agency Inc. | The Magnolia Agency, Inc / Commercial or Residential Insurance / Petal, MS |
| 57 | `timjamesinsurance.net` | insurance | 0.95 | Tim James Agency LLC | As an independent insurance agency, we work with multiple insurance companies to help Texas ind |
| 58 | `tobbenagency.com` | insurance | 0.98 | State Farm | My agency handles home insurance, auto insurance, life insurance, business insurance, and finan |
| 59 | `trentadvisors.com` | insurance | 0.95 | Trent Advisors | An independent insurance advisory firm helping individuals, families, and retirees navigate hea |
| 60 | `truslowyost.com` | insurance | 0.99 | Truslow Yost Insurance Professionals | At Truslow Yost Insurance Professionals, we assist you in finding suitable coverage for your ho |
| 61 | `twfg.com` | insurance | 0.98 | TWFG Insurance (The Woodlands Financial Group) | at TWFG Insurance we treat our customers like family, not policies. Whether you are looking for |
| 62 | `upcover.com` | insurance | 0.95 | upcover | Insurance Built for Growing Businesses / upcover... We are digitising commercial insurance and  |
| 63 | `valorlifebenefits.com` | insurance | 0.85 | Valor assurance | Valor Assurance Life Benefits provides tailored life insurance solutions and financial protecti |
| 64 | `velorarisk.com` | insurance | 0.95 | Velora Risk Partners | Velora Risk Partners / Commercial Insurance Brokerage |
| 65 | `williamblountinc.com` | insurance | 0.95 | William Blount Insurance Advisors | William Blount Insurance Advisors is your Risk Management, Captive, Insurance, Bonds and Person |
| 66 | `wizewall.com` | insurance | 0.92 | Coalition nexus corp | Wizewall final expense review for families planning ahead. Explore coverage options that may he |
| 67 | `zoloins.com` | insurance | 0.95 | Zolo Insurance Services, Inc. | Welcome to Zolo! Driven by Dedication Your trusted ally in protecting your business. We are her |
| 68 | `activerealty.com` | real_estate | 0.98 | Active Realty, Inc | Southern California Real Estate: Homes for Sale with Photos, Tours & Local Insights |
| 69 | `baroncabot.com` | real_estate | 0.95 | Baron & Cabot | Baron Cabot offers simple and profitable UK property investment opportunities catering to both  |
| 70 | `bigskye.properties` | real_estate | 0.95 | Big skye properties | Welcome to Big Skye Properties, your resource for professional property management and real est |
| 71 | `bluebellluxuryhomes.com` | real_estate | 0.98 | Berkshire Hathaway HomeServices Fox & Roach, Realtors | Top Pennsylvania Real Estate Agent / Patricia Pezick Realtor |
| 72 | `bobbynorris.com` | real_estate | 0.99 | BOBBY NORRIS FARM & RANCH REALTY | Bobby Norris is a renowned Texas real estate agent with over five decades of experience deeply  |
| 73 | `carolkleinwnyhomes.com` | real_estate | 0.99 | Century 21 North East - New York | Carol Klein - Century 21 Northeast stands out as one of your premier realtors in WNY, dedicated |
| 74 | `cbburnet.com` | real_estate | 0.98 | HP Real Estate Group with Coldwell Banker Realty | Explore the Twin Cities & MN/WI Real Estate Market / Coldwell Banker Realty |
| 75 | `cbrealty.com` | real_estate | 0.98 | Coldwell Banker Realty / Christopher Barhoum -Keller Wi | Welcome to Coldwell Banker Realty, your trusted advisor for an informed, seamless home journey. |
| 76 | `chrisperezgroup.com` | real_estate | 0.99 | Chris Perez Group | As a Broker Associate at eXp Realty, Chris specializes in residential sales, probate transactio |
| 77 | `christypak.com` | real_estate | 0.99 | Christy Pak Real Estate | CHRISTY PAK - Silicon Valley Realtor® - Elevating Standards in Your Community |
| 78 | `coldwellbanker.com` | real_estate | 0.99 | Coldwell Banker | Real Estate and Homes for Sale - Coldwell Banker |
| 79 | `compass.com` | real_estate | 0.99 | COMPASS / Compass | Compass is a licensed real estate broker. |
| 80 | `dganigroup.com` | real_estate | 0.98 | Dgani | Dgani Group / South Florida Real Estate, Buying, Selling, Leasing and Renovation |
| 81 | `easysell411.com` | real_estate | 0.98 | Easysell | Easy Sell Property Solutions is a family-run cash home buying company based in Long Island, NY. |
| 82 | `eddieandallyteam.com` | real_estate | 0.99 | Eddie and Ally Team | From luxury homes to short-term rentals, investment properties to dream builds, we help clients |
| 83 | `elamre.com` | real_estate | 0.99 | Jacquelyn@elamre.com | At Elam Real Estate, we've spent over 30 years helping people across Middle Tennessee Nashville |
| 84 | `florida360mgmt.com` | real_estate | 0.95 | Florida 360 mgmt | Professional residential, commercial, and association management services backed by over 15 yea |
| 85 | `forkashomes.com` | real_estate | 0.98 | forkas homes | Michael founded Forkas Homes, Inc., where his firm focuses on helping clients find that dream h |
| 86 | `fortiscsg.com` | real_estate | 0.95 | Fortis Capital Solutions | Fortis Capital Solutions is a next-generation commercial real estate brokerage platform being b |
| 87 | `fortisnetlease.com` | real_estate | 0.95 | Fortis Capital Solutions | Net Lease Investment PROPERTY sales are all about maximizing exposure For today's Dispositions, |
| 88 | `gayhardtpartners.com` | real_estate | 0.99 | Gayhardt Real Estate | With over $138 million in closed real estate volume — 386 resale transactions and 441 new home  |
| 89 | `gayhardtrealestate.com` | real_estate | 0.99 | Gayhardt Real Estate | With over $138 million in closed real estate volume, Daryl Gayhardt has established himself as  |
| 90 | `goldtosold.com` | real_estate | 0.99 | eXp Realty | Chad Hedrick, a native Texan and five-time Olympic medalist, brings his relentless drive and lo |
| 91 | `gorillahomebuyers.com` | real_estate | 0.95 | Gorilla Home Buyers | We buy Houses AS-IS! No Showings No Cleaning Up We pay all fees We are local - close fast Goril |
| 92 | `greenfieldfl.com` | real_estate | 0.95 | Greenfield Group | We sell development property. Site plan. Architect's concept. Engineer's analysis. Free with yo |
| 93 | `greenoakpropertymanagement.com` | real_estate | 0.95 | Green Oak Property Management | Green Oak Property Management provides full-service Los Angeles property management for residen |
| 94 | `homesale.com` | real_estate | 0.95 | Berkshire Hathaway HomeServices Homesale Realty | We serve the Maryland and Pennsylvania real estate markets including Lancaster, York, Harrisbur |
| 95 | `homeslakecumberland.com` | real_estate | 0.99 | Cumberland realty group and auction | We offer a wide range of real estate services, including buying, selling, and auctioning proper |
| 96 | `imansir.com` | real_estate | 0.99 | Savoya international realty | Iman Baydoun Turminini / CA DRE# 02089906 Savoya International Realty / CA DRE# 02089906 |
| 97 | `internationalrg.com` | real_estate | 0.95 | International Realty Group | International Realty Group : Real Estate Web Site with active MLS Listings |
| 98 | `joeselzfla.com` | real_estate | 0.98 | Joseph V. Panetta, LLC Realtor | Meet Joe Panetta... Joe joined the Real Estate profession after retiring from almost 33 years w |
| 99 | `kaylaleeteam.com` | real_estate | 0.99 | Kayla Lee Team | Kayla Lee is an award-winning real estate agent that prides herself in her professionalism, fin |
| 100 | `kimmelcollective.com` | real_estate | 0.99 | Kimmel Collective | Kimmel Collective Real Estate Team / Amber Kimmel / Keller Williams Realty |
| 101 | `kimmelteam.com` | real_estate | 0.99 | Kimmel Collective | Kimmel Collective Real Estate Team / Amber Kimmel / Keller Williams Realty |
| 102 | `kpmcoreservices.com` | real_estate | 0.95 | KPM core services | KPM Core Services, LLC is a full-service firm specializing in residential and commercial proper |
| 103 | `lee-associates.com` | real_estate | 0.99 | Lee & Associates - San Francisco | Lee Associates is the largest commercial real estate firm owned by real estate professionals in |
| 104 | `luxurioushomes.com` | real_estate | 0.99 | Mel Bernstein Team - Premier Sotheby's Int'l Realry | Award winning and top producing team representing exceptional properties across Central Florida |
| 105 | `mcgrawrealtors.com` | real_estate | 0.98 | McGraw Realtors | Founded in 1938, McGraw Realtors remains the region's largest independent firm. |
| 106 | `miamiluxuryproperties.com` | real_estate | 0.99 | Miami Luxury Properties | #1 Real Estate Brokerage in Miami |
| 107 | `mmhwps.com` | real_estate | 0.95 | MASTERS MAKiT HOME REALTY | PARSIPPANY NJ Real Estate & Homes for Sale / Masters MAKiT Home Realty |
| 108 | `mykyhomeguide.com` | real_estate | 0.95 | Deuce Kirk Real Estate Team | The Deuce Kirk Team - eXp Realty LLC Buy a Home Sell a Home |
| 109 | `naisavannah.com` | real_estate | 0.98 | NAI Mopper/Benton - Savannah Commercial Real Estate | For over 40 years, the brokers of NAI Mopper/Benton have been intricately involved in Savannah' |
| 110 | `nationalpropertyinvestor.com` | real_estate | 0.95 | National Property Investor, LLC | We Buy Houses Nationwide, As-Is. No agents. No Commissions. |
| 111 | `nwahomesmarket.com` | real_estate | 0.98 | Remax | Mahmoud Chitsazan is a dedicated REALTOR® specializing in Northwest Arkansas (NWA) real estate. |
| 112 | `onguardpm.com` | real_estate | 0.95 | OnGuard Property Management | Property Marketing Stop struggling to find the right tenant for your property. Turn your home o |
| 113 | `oxfordcres.com` | real_estate | 0.98 | Oxford Partners | Houston commercial real estate agents dedicated to serving Office, Industrial, and Healthcare T |
| 114 | `pabstpremierproperties.com` | real_estate | 0.98 | Pabst Premier Properties | John Pabst is the real estate agent in San Diego you need by your side. He has the experience,  |
| 115 | `randrealty.com` | real_estate | 0.99 | Howard Hanna / Rand Realty | Howard Hanna / Rand Realty - A Family Real Estate Company Proudly Serving New York and New Jers |
| 116 | `redefinedrealty.com` | real_estate | 0.99 | Redefined Realty and Auction / Great American Comfort S | Redefined Realty and Auction - Wisconsin's Premier Real Estate Auction Experts |
| 117 | `revelunderwood.com` | real_estate | 0.95 | Revel & Underwood | RU² Commercial Real Estate × ... Development / Property Management / Brokerage / Self Storage / |
| 118 | `scottskarerealtor.com` | real_estate | 0.99 | Scott Skare LLC | Scott Skare, our Broker Associate... specializes in single-family residential homes, waterfront |
| 119 | `thefloridarealestatespecialist.com` | real_estate | 0.98 | Realty one group | Helping buyers, sellers, and investors navigate the South Florida real estate market with confi |
| 120 | `tradetryonrealty.com` | real_estate | 0.95 | Trade & Tryon Realty | Trade Tryon Realty - Your Trusted Charlotte and South Carolina Real Estate Experts |
| 121 | `waypoint-pa.com` | real_estate | 0.99 | Waypoint property advisors | As a premier real estate brokerage in Austin, TX, Waypoint Property Advisors is dedicated to pr |
| 122 | `westvirginiapropertymanagement.com` | real_estate | 0.95 | First Property Solutions Inc | We offer full service property management solutions for rental homes of all shapes and sizes. |
| 123 | `wvpmpro.com` | real_estate | 0.95 | First Property Solutions Inc | We offer full service property management solutions for rental homes of all shapes and sizes. |
| 124 | `yellowdogrealestate.com` | real_estate | 0.98 | Yellow Dog Real Estate | Carey Mitchell, Broker DRE#01356145 Most Recommended Realtor 3 Years in a Row |

---

## 6. A real defect this run found

**Haiku 4.5 rejects the `effort` parameter**, and it failed **silently open** on
all 1,643 calls:

```
HTTP 400 invalid_request_error
"This model does not support the effort parameter."
```

Every lead would have gone through perfectly happily and nothing anywhere would
have said the layer had stopped working. **That is what fail-open costs**, and
it is exactly why the health row reports RED on "errors, and nothing succeeded"
rather than waiting for a threshold.

Fixed with an **allowlist, not a denylist** — an unknown model omits `effort`
and works; a denylist would send it to the next model that refuses it and
silently classify nobody. Verified live against all three models, not just
asserted.

---

## 7. What was actually run

| | |
|---|---|
| `node tests/measure.js --check`, bare | **12 suites, 2,939 assertions, 0 failures** |
| Mutation testing | **8 mutations, 8 caught.** Model-decides-blocking, floor removed, page text into the system prompt, missing key blocks, schedule guard neutered with `if (false)`, warm awaited on the lead path, flag no longer sticky, verdict classifies inline |
| SQL **executed**, not read | All six touched statements `PREPARE`d against a temp shadow of the real schema plus the real migrations; both upserts then **run twice** to prove stickiness and that flagged ≠ blocked. Rolled back. |
| Live API | All three models verified end to end against the real Anthropic API, and the `effort` fix watched working |
| Historical validation | 4,701 domains, 3 models, 14,103 classifications |

**Section 13 of `tests/test-non-icp.js` EXECUTES the classifier** against a
stubbed fetch rather than reading its source: nine fail-open paths, the enum,
the confidence floor, injection handling, the auditable row, and the effort
allowlist. Section 8 now **executes** `nonIcpScheduleSuppressed` too, which
closes the reachability hole an `if (false)` inside it would have left.

### Three pre-existing failures on `main`, also fixed here

`main` has been red since 12 Sept and the bar was not readable until these were
cleared. All three are stale assertions, not defects — **no production code
changed for any of them**:

1. `test-partnerstack.js` — form banner pinned to `v5.10.0`; the files went to
   `v5.11.0` on 11 Sept and the test was not bumped with them.
2. `test-partnerstack.js` — the qualification-poll slice ran to
   `sendQualificationForDomain`, and `qualificationTargetCheck` was inserted
   between them, so the assertion started failing for a function it was never
   about. Now slices to the function's own closing brace.
3. `test-batch-a.js` — `!/IS NOT TRUE/` over the whole recovery health function;
   PR #62 correctly added `non_icp_blocked IS NOT TRUE`. Narrowed to what it
   actually meant: don't widen the *disqualified* predicate.

Also strengthened: the health-id assertion compared client ids to an
eight-name literal. It now derives both sides, so it cannot go stale again.

---

## 8. What has NOT been run

- **No browser pass.** Nothing in the browser changed, so there is nothing to
  walk through — but that is an argument, not an observation.
- **The block has never fired in production**, because all three flags are off.
- **No Slack post has been fired for real.** `slackNonIcpLlmFlagged` and the
  model branch of `slackNonIcpBlocked` are asserted, not watched.
  `tools/fire-non-icp-slack.js` should grow a case for them before either flag
  goes on — the repo's own rule is that every alert path gets fired once on
  purpose, and 21 dead PartnerStack call sites are why.
- **`non_icp_checked_at` has never been read by anything.** It is written and
  indexed; no query uses it yet.
- **The 180-day verdict TTL has never expired**, by construction.

---

## 9. Open items

1. **Fire the two new Slack paths for real** before either flag goes on.
2. **`sdr-calling` does not know about `non_icp_llm_flagged`**, and should not
   until someone decides a flagged lead should not be dialled.
3. **The warehouse classifier (`gist.icp_domains`) is not read, written or
   depended on**, per the brief. Worth a comparison pass during validation —
   it already holds verdicts for many of these domains and disagreements would
   be informative. Not done here.
4. **The bulk scrape read 62% of domains; a careful one-at-a-time pass reads
   substantially more.** The 8-second fetch timeout is **not** the cause — I
   measured 8s and 20s on the same 65 domains and got the same answer, so this
   is concurrency contention in the tool, not a production setting to change.
   Production scrapes one domain per blur, so it behaves like the careful pass.
5. **`business_type` has no dashboard surface.** A flagged lead's type,
   confidence and evidence quote live in `non_icp_domain_verdicts` and in the
   Slack post, and nowhere on the monitor.

---

# LIVE — 15 Sept 2026, 01:15 IST. Runbook.

**All four variables are set on `gushwork-api`.** `NON_ICP_LLM_ENABLED=true`,
`NON_ICP_LLM_META=true`, `NON_ICP_LLM_BLOCK=true`, `NON_ICP_LLM_MODEL=claude-opus-5`.
Shipped in PR #68. `NON_ICP_BLOCK=true` (V1) unchanged — both mechanisms run,
list first.

## Verified live, not asserted

```
POST /non-icp-check  garyrockwellinsurance.com
  call 1 (cold)  → {"blocked":false}                     ← fails open
  call 2 (warm)  → {"blocked":true,"label":"Insurance"}  ← an independent agency
                                                            no domain list can match
```

Production verdict rows, read back:

| domain | type | blocking | conf | evidence |
|---|---|---|---|---|
| `garyrockwellinsurance.com` | insurance | **true** | 0.98 | "Because I'm an independent insurance agent and I don't work for…" |
| `rocketairhvac.com` | home_services | false | 0.97 | "We specialize in AC repair, maintenance, and replacement" |
| `stripe.com` | software_technology | false | 0.95 | — |
| `joesdiner.com` | other | false | 0.60 | "There is no diner. There never was." |

The third and fourth are the controls: a real B2B company untouched, and a
parked joke domain correctly given low confidence and no action.

Health row: **`nonicpllm  green  4 classified in 24h`**.

## 1. Every table, column and row the classifier writes

### Railway — `non_icp_domain_verdicts` (new table, one row per DOMAIN)

Written by the warm path only, on a cache miss. Never at the moment of decision.

| column | what it holds |
|---|---|
| `domain` | PK, registrable domain via `partnerStackCustomerKey` |
| `business_type` | one of 20 enum values, or NULL if no verdict |
| `blocking` | true only for real_estate / insurance at ≥0.75 confidence |
| `confidence` | 0–1, the model's own |
| `evidence_quote` | verbatim from the page, ≤500 chars |
| `reason` | the model's one-line justification |
| `source` | `llm` \| `llm_unreachable` \| `llm_error` |
| `model_id`, `prompt_version` | `claude-opus-5`, `v1-2026-09-14` |
| `page_text_sha256`, `page_url_used`, `page_text_chars` | exactly which bytes were judged |
| `scrape_status` | `ok` \| `unreachable` \| `thin` \| `blocked_by_site` \| `private_host` |
| `error`, `checked_at` | |

Volume: **~28–35 new rows/day.** TTL 180 days for a real verdict, 6 hours for a
failure row.

### Railway — `leads` (5 columns, per lead)

`non_icp_blocked` (sticky), `non_icp_reason` (the domain judged), `non_icp_source`
(`llm` or `domain_list` — first write wins), `non_icp_checked_at`,
`non_icp_llm_flagged` (sticky).

### AWS mirror — `gw_form_leads`

**Only `non_icp_blocked` and `non_icp_reason`.** A model block reaches the dialer
because it sets `non_icp_blocked`, which already syncs. `non_icp_llm_flagged`,
`non_icp_source` and `non_icp_checked_at` are **deliberately not mirrored** —
a flagged-not-blocked lead should still be dialled, so shipping the column before
`sdr-calling` has a consumer would be premature.

**Nothing is written to `gist.icp_domains`.** Read-only, never touched.

## 2. What the Slack post looks like

Fired for real tonight, both paths, Slack 200 on each. Verbatim:

```
🚫 Lead Blocked — Non-ICP
─────────────────────────
Read their website and judged: Insurance  (confidence 95%)
Domain judged: `deliberate-non-icp-test.invalid`
Their website: deliberate-non-icp-test.invalid
What it said: "We are an independent insurance agency serving families
               across Ohio with auto, home, life and commercial insurance."
Stopped at: submit (email + website)

  👤 Name · 📧 Email · 🏢 Company · 📞 Phone · 🎯 Sells to · 💬 Heard about us
  📝 About their business

They filled the whole form, were sent to /thank-you and never reached the
calendar. No Meta event fired. Not pushed to Salesforce.

Wrong? NON_ICP_LLM_BLOCK=false on Railway stops the model layer blocking
without touching the brand-domain list or needing a deploy.
Decided by claude-opus-5, prompt v1-2026-09-14.
```

The Meta-only industries get a **different** post — `📉 Meta events withheld —
non-ICP industry (model)` — because calling those "would have been blocked"
would be false in the one channel that exists to catch mistakes.

## 3. If a verdict is wrong

**How you find out.** Every submit-time block posts to the leads channel with
the business type, the confidence and the verbatim sentence it decided on. That
is the whole review surface, and it is why gate 1 existed.

**How fast.** At submit, in real time. A **step-1-only** block posts nothing —
that lead appears on the Blocked tab and nowhere else, unchanged from V1.

**Reversing it — narrowest first.**

```bash
# 1. ONE wrong verdict. Deletes the cached row; re-warms on the next blur.
#    If it re-classifies the same way, use 2 or 3.
railway run -s Postgres bash -c \
  "psql \"\$DATABASE_PUBLIC_URL\" -c \"DELETE FROM non_icp_domain_verdicts WHERE domain='example.com'\""

# 2. Stop the model layer BLOCKING. Meta suppression and classification continue.
railway variable set --service gushwork-api NON_ICP_LLM_BLOCK=false

# 3. Stop Meta suppression too. Classification continues, nothing acts on it.
railway variable set --service gushwork-api NON_ICP_LLM_META=false

# 4. Stop the whole layer. V1's brand-domain list keeps working.
railway variable set --service gushwork-api NON_ICP_LLM_ENABLED=false

# 5. Un-block leads already stamped tonight (does NOT un-send Meta).
railway run -s Postgres bash -c \
  "psql \"\$DATABASE_PUBLIC_URL\" -c \"UPDATE leads SET non_icp_blocked=false \
   WHERE non_icp_source='llm' AND created_at > NOW() - INTERVAL '24 hours'\""
```

Each takes effect on the **next request** — no deploy. Anything at level 2 or
above leaves V1 untouched and still blocking.

**What reversal cannot undo:** a suppressed Meta event. There is no replay.

## 4. What I am uneasy about, shipping this unwatched

1. **A blocked lead can still take a calendar slot.** The unwarmed-verdict gap,
   unchanged from V1: if the warm has not finished when `/submit` runs, the
   verdict fails open, the calendar renders, and the booking routes then refuse
   it with a **critical alert**. Overnight nobody cancels that slot. Measured
   end-to-end warm is median 3.8s but **max 6.1s — 1 in 12 over six seconds**, and
   the scrape can take up to 8s. The email blur at step 1 normally buys 30–60s,
   so this should be rare; V1 has fired the alert zero times in four days. But
   V1 never had to scrape and call a model first.

2. **One lead in ten stops firing Meta.** 6.2% blocked plus 4.2% Meta-only.
   That is a far bigger change to the ad signal than V1 made, it starts tonight,
   and it cannot be un-sent. Authorised — but it is the single largest-blast-radius
   thing here.

3. **The cache is cold.** Four rows. Every domain tomorrow is a first-time
   classification, so tonight's behaviour depends on warm timing rather than on
   the 271 domains already validated. I have **4,701 Opus verdicts on disk** from
   the validation run and did **not** load them, because that is a 4,701-row
   production write nobody authorised. Say the word and it is one command — it
   would make tomorrow deterministic and match exactly the analysis you have read.

4. **`home_services` is the biggest Meta-only group by far** — 156 of the 220
   domains in the other four industries. If one industry is going to be wrong at
   scale, it is that one, and plumbers and HVAC firms are plausible ICP. Worth
   reading those posts specifically.

5. **`Opus 5` at effort low is untested at volume in this path.** 25 calls
   measured, zero failures. It has never run a full day.

6. **Nothing tells you the cost.** There is no spend alarm. At ~30 domains/day
   this is ~$18/month, but a scraper loop or a retry storm has no ceiling.
