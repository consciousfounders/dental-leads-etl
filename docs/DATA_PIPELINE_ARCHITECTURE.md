# Data Pipeline Architecture

## System Overview

```mermaid
graph TB
    %% Data Sources Layer
    subgraph "Data Sources"
        DS1[CMS NPPES<br/>National Provider Registry]
        DS2[State Dental Boards<br/>License Data]
        DS3[Enrichment APIs<br/>Wiza, Apollo, Clay]
        DS4[Validation APIs<br/>Addy, Twilio]
        DS5[Marketing Platforms<br/>Google Ads, Meta, TTD]
        DS6[Email Platforms<br/>Instantly, Smartlead]
        DS7[CRM Systems<br/>Go High Level]
        DS8[Website Analytics<br/>GA4, De-anon Services]
    end

    %% VM Layer - First Transformations
    subgraph "GCP VM - Orchestration Hub"
        VM1[File Type Conversion<br/>CSV → Parquet, JSON → Structured]
        VM2[Data Validation<br/>Schema Checks, Format Normalization]
        VM3[Enrichment Orchestration<br/>API Calls, Rate Limiting]
        VM4[Webhook Receivers<br/>Event Ingestion]
    end

    %% Storage Layer
    subgraph "GCS Bucket - Data Lake"
        GCS1[Raw Data<br/>Immutable Archive]
        GCS2[Staged Data<br/>Pre-warehouse Processing]
    end

    %% Data Warehouse Layer
    subgraph "Data Warehouse - Medallion Architecture"
        direction TB
        RAW[RAW Layer<br/>Unprocessed Source Data]
        CLEAN[CLEAN Layer<br/>Validated & Normalized]
        ENRICHED[ENRICHED Layer<br/>Augmented with External Data]
        SEGMENTED[SEGMENTED Layer<br/>Targeted Audiences]
        
        RAW --> CLEAN
        CLEAN --> ENRICHED
        ENRICHED --> SEGMENTED
    end

    %% Visualization Layer
    subgraph "Visualization & Exploration"
        VIZ1[Streamlit<br/>Interactive Dashboards]
        VIZ2[Looker<br/>BI & Reporting]
        VIZ3[Hex<br/>Data Analysis]
    end

    %% Activation Layer
    subgraph "CRM & Advertising"
        CRM[CRM System<br/>Go High Level]
        AD1[Email Marketing<br/>Instantly, Smartlead]
        AD2[Display Advertising<br/>Google Ads, Meta, TTD]
        AD3[Website<br/>Landing Pages]
    end

    %% Feedback Loop
    subgraph "Event Tracking & Attribution"
        EVT1[Email Events<br/>Open, Click, Reply]
        EVT2[Ad Events<br/>Impression, Click, Conversion]
        EVT3[Website Events<br/>Page View, Form Submit]
        EVT4[CRM Events<br/>Contact Created, Deal Won]
    end

    %% ML/AI Layer
    subgraph "ML / AI Analysis"
        ML1[Lead Scoring<br/>0-100 Score]
        ML2[Conversion Prediction<br/>Probability Models]
        ML3[Channel Optimization<br/>Best Channel Predictor]
        ML4[Creative Personalization<br/>Content Recommender]
        ML5[Timing Optimization<br/>Optimal Send Time]
    end

    %% Data Flow - Sources to VM
    DS1 --> VM1
    DS2 --> VM1
    DS3 --> VM3
    DS4 --> VM2
    DS5 --> VM4
    DS6 --> VM4
    DS7 --> VM4
    DS8 --> VM4

    %% VM to Storage
    VM1 --> GCS1
    VM2 --> GCS1
    VM3 --> GCS2
    VM4 --> GCS2

    %% Storage to Warehouse
    GCS1 --> RAW
    GCS2 --> RAW

    %% Warehouse to Visualization
    RAW --> VIZ1
    CLEAN --> VIZ1
    ENRICHED --> VIZ2
    SEGMENTED --> VIZ3

    %% Warehouse to Activation
    SEGMENTED --> CRM
    SEGMENTED --> AD1
    SEGMENTED --> AD2
    SEGMENTED --> AD3

    %% Activation to Events
    AD1 --> EVT1
    AD2 --> EVT2
    AD3 --> EVT3
    CRM --> EVT4

    %% Events back to Warehouse
    EVT1 --> RAW
    EVT2 --> RAW
    EVT3 --> RAW
    EVT4 --> RAW

    %% Warehouse to ML
    ENRICHED --> ML1
    SEGMENTED --> ML2
    RAW --> ML3
    RAW --> ML4
    RAW --> ML5

    %% ML back to Activation
    ML1 --> CRM
    ML2 --> AD1
    ML3 --> AD2
    ML4 --> AD2
    ML5 --> AD1

    %% ML feeds back to channels
    ML4 -.Personalized Creative.-> AD1
    ML4 -.Personalized Creative.-> AD2
    ML5 -.Optimal Timing.-> AD1
    ML3 -.Channel Selection.-> CRM

    %% Styling
    classDef sourceStyle fill:#e1f5ff,stroke:#01579b,stroke-width:2px
    classDef vmStyle fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef storageStyle fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef warehouseStyle fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    classDef vizStyle fill:#fff9c4,stroke:#f57f17,stroke-width:2px
    classDef activationStyle fill:#fce4ec,stroke:#880e4f,stroke-width:2px
    classDef eventStyle fill:#e0f2f1,stroke:#004d40,stroke-width:2px
    classDef mlStyle fill:#fff8e1,stroke:#ff6f00,stroke-width:2px

    class DS1,DS2,DS3,DS4,DS5,DS6,DS7,DS8 sourceStyle
    class VM1,VM2,VM3,VM4 vmStyle
    class GCS1,GCS2 storageStyle
    class RAW,CLEAN,ENRICHED,SEGMENTED warehouseStyle
    class VIZ1,VIZ2,VIZ3 vizStyle
    class CRM,AD1,AD2,AD3 activationStyle
    class EVT1,EVT2,EVT3,EVT4 eventStyle
    class ML1,ML2,ML3,ML4,ML5 mlStyle
```

## Conversion Events Tracked

```mermaid
graph LR
    A[Lead/Contact/Prospect] --> B{Engagement Event}
    
    B --> C1[Ad Engagement<br/>View, Click]
    B --> C2[Email Open<br/>Email Click]
    B --> C3[Website Visit<br/>Page View]
    B --> C4[Form Submission<br/>Contact Info]
    B --> C5[Meeting Booked<br/>Calendar Event]
    B --> C6[One-Time Purchase<br/>Product Sale]
    B --> C7[Subscription Start<br/>Recurring Revenue]
    B --> C8[Renewal<br/>Subscription Renewal]
    
    C1 --> D[Data Warehouse]
    C2 --> D
    C3 --> D
    C4 --> D
    C5 --> D
    C6 --> D
    C7 --> D
    C8 --> D
    
    D --> E[ML Analysis]
    E --> F[Personalized Creative]
    F --> A
```

## Data Flow Stages

### Stage 1: Ingestion
- **Input**: Raw data from sources (CSV, JSON, APIs)
- **Process**: File type conversion, initial validation
- **Output**: Standardized files in GCS

### Stage 2: Transformation (RAW → CLEAN)
- **Input**: Raw data from GCS
- **Process**: 
  - Remove inactive records
  - Filter by criteria (e.g., dental only)
  - Deduplicate
  - Address/phone validation
- **Output**: Clean, validated records

### Stage 3: Enrichment (CLEAN → ENRICHED)
- **Input**: Clean records
- **Process**:
  - Email enrichment (Wiza/Apollo/Clay)
  - LinkedIn profile matching
  - Firmographic data augmentation
- **Output**: Enriched records with contact info

### Stage 4: Segmentation (ENRICHED → SEGMENTED)
- **Input**: Enriched records
- **Process**:
  - ICP scoring
  - Geographic segmentation
  - Practice size estimation
  - Behavioral clustering
- **Output**: Targeted audience segments

### Stage 5: Activation
- **Input**: Segmented audiences
- **Process**: Export to CRM/advertising platforms
- **Output**: Active campaigns

### Stage 6: Feedback Loop
- **Input**: Event data from campaigns
- **Process**: Attribution, event tracking
- **Output**: Event records in RAW layer

### Stage 7: ML Analysis
- **Input**: Historical data (enriched + events)
- **Process**: 
  - Feature engineering
  - Model training
  - Prediction generation
- **Output**: Scores, probabilities, recommendations

### Stage 8: Personalization
- **Input**: ML predictions
- **Process**: Dynamic creative generation
- **Output**: Personalized content per lead/contact

---

## Key Components

### Data Sources
- **CMS NPPES**: National Provider Identifier registry (weekly updates)
- **State Boards**: Dental license data (scraped/API)
- **Enrichment APIs**: Wiza (email), Apollo (person/company), Clay (multi-source)
- **Validation APIs**: Addy (address), Twilio (phone)
- **Marketing Platforms**: Google Ads, Meta, The Trade Desk
- **Email Platforms**: Instantly, Smartlead
- **CRM**: Go High Level
- **Analytics**: GA4, de-anonymization services (RB2B, Leadfeeder)

### VM Functions
- File type conversion (CSV → Parquet, JSON → structured)
- Data validation (schema checks, format normalization)
- Enrichment orchestration (API calls, rate limiting, retries)
- Webhook receivers (event ingestion from all platforms)

### Storage (GCS)
- **Raw Data**: Immutable archive of all source data
- **Staged Data**: Pre-processed data ready for warehouse ingestion

### Data Warehouse (Medallion Architecture)
- **RAW**: Unprocessed source data (immutable)
- **CLEAN**: Validated, normalized, deduplicated
- **ENRICHED**: Augmented with external data (emails, LinkedIn, etc.)
- **SEGMENTED**: Targeted audiences ready for activation

### Visualization Tools
- **Streamlit**: Interactive dashboards for segmentation
- **Looker**: BI reporting and analysis
- **Hex**: Data exploration and ad-hoc analysis

### Activation Channels
- **CRM**: Contact/account management (Go High Level)
- **Email**: Cold email campaigns (Instantly, Smartlead)
- **Display Ads**: Programmatic advertising (Google, Meta, TTD)
- **Website**: Landing pages and forms

### Event Tracking
- Email events: open, click, reply, bounce
- Ad events: impression, click, conversion
- Website events: page view, form submit, demo booked
- CRM events: contact created, deal won, subscription started

### ML/AI Models
- **Lead Scoring**: 0-100 score for prioritization
- **Conversion Prediction**: Probability of conversion event
- **Channel Optimization**: Best channel for each lead
- **Creative Personalization**: Content recommendations per lead
- **Timing Optimization**: Optimal send time prediction

---

## Feedback Loops

### Loop 1: Event → Warehouse → ML → Activation
1. Events flow back to RAW layer
2. ML models analyze patterns
3. Predictions feed into activation channels
4. Personalized creatives generated
5. New events generated
6. Cycle repeats

### Loop 2: Performance → Segmentation → Targeting
1. Campaign performance analyzed
2. Segments refined based on conversion data
3. New segments created
4. Targeted campaigns launched
5. Performance measured
6. Cycle repeats

---

*Last Updated: December 2024*

