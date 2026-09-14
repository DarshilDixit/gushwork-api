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

# ADDENDUM, 14 Sept 2026 — the revenue question, and it changes the answer

Asked after the PR merged: *of the 124 booked-and-showed leads, how many became
paying customers?* The literal answer is **zero**. The useful answer is not,
and it argues harder against the block than anything in section 4.

## 1. The literal question: 0 of 124

Checked four ways against `gist.customer_contract_terms` and the wider customer
tables:

| check | result |
|---|---|
| customer row whose **domain** is one of the 124 | 0 |
| customer row whose **customer_email** is one of the 146 lead emails behind them | 0 |
| customer row whose **email domain** is one of the 124 | 0 |
| any of the 124 in the **681-domain bypass set** (all three customer tables) | 0 |
| **company-name** match against `customer_enrichment` + `gist_accountsmaster` | 0 |

**Two of those five are partly circular and I am not counting them as evidence.**
G2 was constructed to exclude G1, and G1 was built from the customer domain and
the customer-email domain — so the first and third rows could not have come back
non-zero. The name match and the bypass-set check are independent, and both are
zero.

**The limit, stated rather than hidden:** a company among the 124 that became a
customer under *both* a different domain *and* a different company name would
not be found by any of this. There is no way to rule that out short of manual
review.

## 2. The question that actually decides it

Turn it around. **Of our 675 enriched customers, how many are in the two
business types this feature blocks?**

**28** — 15 whose `major_industry` is `Insurance`, 13 `Real Estate`. That is
**4.2% of the customer base**, and it is not an artefact of anything: it comes
from a table this analysis had not otherwise touched.

Run the real classifier over their websites:

| model | readable | **would flag** |
|---|---|---|
| claude-sonnet-5 | 20 of 26 | **14** |
| claude-opus-5 | 20 of 26 | **14** |

Including `gabrielleborowski.com` at **0.99 / 0.98** — sub-industry
*"Residential Real Estate Agent"*. An individual residential realtor who is a
paying customer. That is the exact population the block exists to turn away.

## 3. The bypass would not have saved them, and here is why

The obvious objection is that the known-customer bypass protects all of this.
**It does not, because the bypass only knows about people who are already
customers.**

Eight of the 26 came through the form. In **every single case the lead arrived
BEFORE the onboarding date**:

| domain | first lead | onboarded | gap | would the model flag it? | MRR |
|---|---|---|---|---|---|
| `nomadgroup.io` | 2026-04-21 | 2026-04-26 | 5d | **FLAG** real_estate 0.90 | **$2,200** |
| `sspins.com` | 2026-05-07 | 2026-06-30 | 54d | **FLAG** insurance 0.95 | **$920** |
| `yourhealthyourmoneyaz.com` | 2026-05-30 | 2026-06-04 | 5d | **FLAG** insurance 0.90 | **$740** |
| `garylifeindex.com` | 2026-06-25 | 2026-06-29 | 4d | **FLAG** insurance 0.97 | **$640** |
| `vellumlifegroup.com` | 2026-05-22 | 2026-05-29 | 7d | **FLAG** insurance 0.98 | **$500** |
| `jacobsfamilyinsurance.net` | 2026-06-21 | 2026-06-24 | 3d | no verdict — page too thin | $900 |
| `homesandrental.com` | 2026-05-10 | 2026-05-18 | 8d | no verdict — unreachable | — |
| `americanhomeinvestmentsatlanta.com` | 2026-07-11 | 2026-07-15 | 4d | no verdict — page too thin | — |

**Five paying customers, $5,000/month of MRR, would have been turned away at the
form** — days or weeks before they were customers, so nothing in the bypass
could have known.

The three that survive are saved by an unreadable page, not by design. And
`jacobsfamilyinsurance.net`'s lead email is `nedjacobs@allstate.com`, which **V1
blocks today on the email** — so that $900 is already exposed to the mechanism
that is live right now.

## 4. What this means for the decision

**The customer bypass protects renewals. It cannot protect first-time buyers,
and first-time buyers are the entire population the form exists to capture.**
That is a structural hole, not a tuning problem, and it is why section 4a's
comforting "1 of 6 survives the bypass" understates the risk. Scored at the
moment a lead actually arrives, the number is **5 of 8**.

Nothing here says the model is wrong. All five are correctly classified —
they *are* insurance brokerages and a real-estate firm. It says the premise is
wrong: **we sell to these people.** 4.2% of the customer base and $6,035/month
of known MRR across 7 contracted RE/insurance customers.

**Recommendation is unchanged and now much more strongly held: do not switch
`NON_ICP_LLM_BLOCK` on.** Flag-and-watch gives the AE-time saving you actually
asked for without the revenue risk, because a flagged lead still books.

If the block is switched on later, the bypass needs a second leg that the
current design does not have — something that recognises a *prospective* buyer,
not just an existing one.

## 5. The breakdown by type and brand affiliation

| | insurance | real_estate | total |
|---|---|---|---|
| **independent** | 53 | 44 | **97 (78%)** |
| brand, by company name | 6 | 7 | 13 |
| already on `NON_ICP_DOMAINS` | 8 | 6 | 14 |
| **total** | **67** | **57** | **124** |

**78% are independents** — precisely the gap V1 cannot reach, so the layer is
doing what it was built for. Only 27 are brand-affiliated, and **14 of those are
already blocked today**, so the model block's incremental effect on this
population is **110 new refusals, not 124**.

The brand-affiliated 27 are the recognisable ones: State Farm agents (×6),
Coldwell Banker (×3), Berkshire Hathaway (×2), Compass, Century 21, eXp, RE/MAX,
Sotheby's, Howard Hanna, McGraw, HealthMarkets (×2), Bankers Life, GEICO, Globe
Life, New York Life, Farmers.

---

# ADDENDUM 2, 15 Sept 2026 — the retention check. It cannot be run.

Swapnil's 11 Sept reasoning, supplied after Addendum 1: *"they will churn within
a few months because they won't see value."* So closing was never his objection —
he knows they buy. The claim is retention, and the Non-ICP doc says the same
thing: rules 5 and 6 are built on retention and refund rate, and the doc states
outright that the flagged group may close perfectly well.

That makes Addendum 1's **$5,000/month figure a claim about revenue that is
still there, and I did not check that it is. Corrected below.**

## 1. Three of the four things asked for do not exist

| asked for | available? | why |
|---|---|---|
| Current status — active / churned / refunded / paused | **partially** | see §2 |
| Churn date | **no** | no populated `End_Date`, no churn-date column anywhere, and Salesforce keeps **no field history** on `Customer_Status__c` |
| Months retained before churning | **no** | follows from the above |
| Total revenue actually collected | **no** | see below |
| Refunds | **no** | same source, same problem |

**On collected revenue.** The warehouse does hold invoice-level Stripe data —
`gist.gist_invoicesstripedata`, 3,915 invoices, $56.5M collected. **It is not
ours.** Its two `source_account` values are *Regents* and *Delfin*, it ends
**2025-09-22**, and it matches **25 of 469** rows in `customer_contract_terms`
by `stripe_customer_id` and **3** by email. The $56.5M against contracts
averaging $835/month is the giveaway. Same for refunds: 29 rows on Regents, 1 on
Delfin, wrong entity and wrong period.

`public.subscriptions` has **zero rows with a future billing date** (max
2026-06-19), so it is stale too. `gist_accountsmaster` is stale to Dec 2025 and
contains **none** of the 28. `biz_health_Customer_Master_` is 49 rows ending
Dec 2025.

CLAUDE.md's line that *"nothing in `gw_prod` can date a churn"* is still
correct. It named three tables; the reason is broader — **there is no ingestion
of our own billing into this warehouse at all.**

## 2. The one real source, and where it stops

**Salesforce `Account.Customer_Status__c`** is the only genuine retention state:
Active 240, Post-Onboarding Drop Off 16, Pre-Onboarding Drop Off 17, Churned 1,
To Be Onboarded 15.

**It is maintained through April 2026, partially in May (29 of 110 accounts),
and not at all from June onward:**

| onboarding month | accounts | with a status |
|---|---|---|
| 2026-03 | 79 | 79 |
| 2026-04 | 75 | 75 |
| 2026-05 | 110 | **29** |
| 2026-06 | 155 | **0** |
| 2026-07 | 115 | **0** |
| 2026-08 | 50 | **0** |

**Every customer Addendum 1's argument rests on sits in the unmaintained
window.** Of the eight RE/insurance customers who came through the form, exactly
**one** (`nomadgroup.io`, onboarded 27 Apr) has a status. The other seven — the
ones carrying the $5,000 — have none, and never will unless somebody backfills
the field.

This is a CS process gap, not an engineering one. Nothing in this repo writes
that field.

## 3. What the measurable slice says — and why it decides nothing

Every account with a decided status, age-matched by construction:

| | n | active | dropped | drop-off rate |
|---|---|---|---|---|
| **RE/insurance** | **7** | 7 | **0** | **0.0%** |
| everyone else | 255 | 233 | 22 | **8.6%** |

The seven: `truecostgroup.us`, `aloriinternationalholdings.com`,
`somainsure.com`, `dealerre.com`, `warranty-re.com`, `nomadgroup.io`,
`acsbonding.com`. All Active, onboarded Jan–May 2026, so 4–8 months in.

**This is not evidence that they retain better. It is not evidence of
anything.** At the base rate, the chance of seeing zero drop-offs in seven
accounts is **53%** — zero is the single most likely outcome even if retention
is identical. The 95% interval on 0/7 runs from **0% to 34.8%**, against a base
of 8.6%. It cannot distinguish "much better" from "four times worse."

To have 84% power to see even one drop-off at the base rate you would need
**n=20**. We have 7.

## 4. Contract structure — available, and it points the other way

This one is fully measurable across all 469 contracts:

| | n | avg monthly | avg lock-in | avg upfront |
|---|---|---|---|---|
| **RE/insurance** | 14 | **$869** | **3.21 months** | **$651** |
| everyone else | 462 | $835 | 2.73 months | $276 |

They sign slightly larger contracts, slightly longer lock-ins, and **more than
double the upfront payment**. In the Jan–Apr cohort the gap is wider: $1,040/mo
against $719. That is not the shape of a group expected to leave in a few
months — but it is a proxy for commitment at signing, not for retention, and
n=14.

## 5. Correcting Addendum 1

**"$5,000/month of MRR would have been turned away" overstated what I checked.**
What is actually supportable:

> Five companies with **$5,000/month of contracted MRR at signature**, all
> reading `Active` in a `customer_contract_terms` snapshot **loaded 13 July
> 2026** — between two weeks and two and a half months after they onboarded.
> There is no data after 13 July. Whether that revenue survived is unknown.

The direction of the argument stands: the bypass cannot protect a first-time
buyer, and these five were flagged at a moment when they were not yet customers.
The **size** of it does not.

## 6. The part that matters most: the test cannot be run yet

Even if `Customer_Status__c` were backfilled tomorrow, **the answer would still
not exist.** The eight form-arriving RE/insurance customers onboarded between
**19 May and 16 July 2026** — they are two to four months old. Swapnil's claim is
that this group churns *within a few months*. The cohort has not aged past the
window his claim is about.

So the honest position on both sides:

- **Swapnil's claim is not falsified.** Nothing here contradicts it.
- **Swapnil's claim is not supported either**, and neither is the Non-ICP doc's
  retention-and-refund premise. There is no refund data and no churn dating in
  this warehouse at all, so the doc's stated basis has never been measured here.
- **My Addendum 1 argument is weaker than I presented it**, for the reason he
  identified.

**No recommendation, per the instruction, because the numbers do not exist.**

### What would make it answerable, in order of cost

1. **Backfill `Customer_Status__c` for June 2026 onward.** No engineering. It is
   the only field anywhere that carries this, and 320+ accounts are missing it.
2. **Add a churn DATE and turn on field history** for that field. Without a
   date, "months retained" is permanently uncomputable even once status exists.
3. **Ingest our own Stripe** into the warehouse. Collected revenue and refunds —
   the doc's actual stated basis for rules 5 and 6 — cannot be measured until
   this exists.
4. **Then wait.** The earliest honest read on the May–July 2026 cohort is around
   **December 2026 to March 2027**.

Until at least (1), this decision is being made on Swapnil's judgement rather
than on a measurement, which is a legitimate way to make it — it just should not
be described as evidence-backed in either direction.
