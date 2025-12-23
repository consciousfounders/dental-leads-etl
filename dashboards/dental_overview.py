"""
Dental Leads Dashboard - Streamlit
Run: streamlit run dashboards/dental_overview.py
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils.snowflake_client import SnowflakeClient

# Page config
st.set_page_config(
    page_title="Dental Leads Dashboard",
    page_icon="🦷",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    /* Force dark background - using !important to override Streamlit defaults */
    .stApp {
        background-color: #0e1117 !important;
    }
    .main .block-container {
        background-color: #0e1117 !important;
    }
    .main {
        background-color: #0e1117 !important;
    }
    body {
        background-color: #0e1117 !important;
    }
    section[data-testid="stSidebar"] {
        background-color: #1e3a5f !important;
    }
    [data-testid="stSidebar"] {
        background-color: #1e3a5f !important;
    }
    
    /* Force text colors */
    h1, h2, h3, h4, h5, h6, p, div, span, label {
        color: #ffffff !important;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: bold;
    }
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.8;
    }
    .stMetric {
        background: #1e3a5f !important;
        padding: 15px;
        border-radius: 8px;
        color: white !important;
    }
    .stMetric label {
        color: white !important;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: white !important;
    }
    .stMetric [data-testid="stMetricLabel"] {
        color: white !important;
    }
</style>
""", unsafe_allow_html=True)

# State abbreviation to full name mapping (for Plotly choropleth)
STATE_NAMES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
    'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
    'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
    'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
    'DC': 'District of Columbia', 'PR': 'Puerto Rico'
}


@st.cache_data(ttl=3600)
def load_data():
    """Load data from Snowflake with caching"""
    os.environ['SKIP_SECRET_MANAGER'] = 'true'
    
    with SnowflakeClient() as client:
        # Individual dentists by state
        dentists_by_state = pd.DataFrame(
            client.execute("""
                SELECT STATE, COUNT(*) as COUNT, 
                       COUNT(CASE WHEN GENDER = 'M' THEN 1 END) as MALE,
                       COUNT(CASE WHEN GENDER = 'F' THEN 1 END) as FEMALE
                FROM CLEAN.V_INDIVIDUAL_DENTISTS 
                WHERE STATE IS NOT NULL
                GROUP BY STATE
                ORDER BY COUNT DESC
            """),
            columns=['STATE', 'COUNT', 'MALE', 'FEMALE']
        )
        
        # Practice age cohorts
        age_cohorts = pd.DataFrame(
            client.execute("""
                SELECT PRACTICE_AGE_COHORT, COUNT(*) as COUNT
                FROM CLEAN.V_INDIVIDUAL_DENTISTS
                WHERE PRACTICE_AGE_COHORT IS NOT NULL
                GROUP BY PRACTICE_AGE_COHORT
                ORDER BY PRACTICE_AGE_COHORT
            """),
            columns=['COHORT', 'COUNT']
        )
        
        # Gender breakdown
        gender = pd.DataFrame(
            client.execute("""
                SELECT GENDER, COUNT(*) as COUNT
                FROM CLEAN.V_INDIVIDUAL_DENTISTS
                WHERE GENDER IS NOT NULL
                GROUP BY GENDER
            """),
            columns=['GENDER', 'COUNT']
        )
        
        # Summary stats
        stats = client.execute("""
            SELECT 
                (SELECT COUNT(*) FROM CLEAN.V_INDIVIDUAL_DENTISTS) as dentists,
                (SELECT COUNT(*) FROM CLEAN.V_ORGANIZATIONS) as orgs,
                (SELECT COUNT(*) FROM CLEAN.V_DECISION_MAKERS) as decision_makers,
                (SELECT COUNT(*) FROM CLEAN.V_AUTH_OFFICIALS) as auth_officials,
                (SELECT COUNT(*) FROM CLEAN.V_AUTH_OFFICIALS WHERE ENRICHED_EMAIL IS NOT NULL) as enriched
        """)[0]
        
        # Top specialties (from taxonomy)
        specialties = pd.DataFrame(
            client.execute("""
                SELECT 
                    CASE 
                        WHEN TAXONOMY_CODE = '1223G0001X' THEN 'General Dentist'
                        WHEN TAXONOMY_CODE = '1223P0221X' THEN 'Pediatric'
                        WHEN TAXONOMY_CODE = '1223S0112X' THEN 'Oral Surgery'
                        WHEN TAXONOMY_CODE = '1223E0200X' THEN 'Endodontics'
                        WHEN TAXONOMY_CODE = '1223P0300X' THEN 'Periodontics'
                        WHEN TAXONOMY_CODE = '1223D0001X' THEN 'Orthodontics'
                        WHEN TAXONOMY_CODE = '1223P0700X' THEN 'Prosthodontics'
                        ELSE 'Other Specialty'
                    END as SPECIALTY,
                    COUNT(*) as COUNT
                FROM CLEAN.V_INDIVIDUAL_DENTISTS
                GROUP BY 1
                ORDER BY COUNT DESC
            """),
            columns=['SPECIALTY', 'COUNT']
        )
        
        # Decision makers by state
        dm_by_state = pd.DataFrame(
            client.execute("""
                SELECT ORG_STATE as STATE, COUNT(*) as COUNT
                FROM CLEAN.V_DECISION_MAKERS
                WHERE ORG_STATE IS NOT NULL
                GROUP BY ORG_STATE
                ORDER BY COUNT DESC
            """),
            columns=['STATE', 'COUNT']
        )
        
    return {
        'dentists_by_state': dentists_by_state,
        'age_cohorts': age_cohorts,
        'gender': gender,
        'stats': {
            'dentists': stats[0],
            'orgs': stats[1],
            'decision_makers': stats[2],
            'auth_officials': stats[3],
            'enriched': stats[4]
        },
        'specialties': specialties,
        'dm_by_state': dm_by_state
    }


def main():
    # Header
    st.title("🦷 Dental Leads Intelligence")
    st.markdown("*Real-time insights from NPI data*")
    
    # Load data
    with st.spinner("Loading data from Snowflake..."):
        data = load_data()
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    all_states = ['All States'] + data['dentists_by_state']['STATE'].tolist()
    selected_state = st.sidebar.selectbox("State", all_states)
    
    # KPI Cards
    st.markdown("---")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("👨‍⚕️ Individual Dentists", f"{data['stats']['dentists']:,}")
    with col2:
        st.metric("🏢 Organizations", f"{data['stats']['orgs']:,}")
    with col3:
        st.metric("🎯 Decision Makers", f"{data['stats']['decision_makers']:,}")
    with col4:
        st.metric("📋 Auth Officials", f"{data['stats']['auth_officials']:,}")
    with col5:
        enrichment_rate = (data['stats']['enriched'] / data['stats']['auth_officials'] * 100) if data['stats']['auth_officials'] > 0 else 0
        st.metric("✅ Enriched", f"{data['stats']['enriched']:,}", f"{enrichment_rate:.1f}%")
    
    st.markdown("---")
    
    # Row 1: Map and Top States
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📍 Dentists by State")
        
        # Prepare data for choropleth
        map_df = data['dentists_by_state'].copy()
        map_df['STATE_NAME'] = map_df['STATE'].map(STATE_NAMES)
        
        fig_map = px.choropleth(
            map_df,
            locations='STATE',
            locationmode='USA-states',
            color='COUNT',
            scope='usa',
            color_continuous_scale='Blues',
            hover_name='STATE_NAME',
            hover_data={'COUNT': ':,', 'STATE': False},
            labels={'COUNT': 'Dentists'}
        )
        fig_map.update_layout(
            geo=dict(bgcolor='rgba(0,0,0,0)'),
            margin=dict(l=0, r=0, t=0, b=0),
            coloraxis_colorbar=dict(title="Count")
        )
        st.plotly_chart(fig_map, use_container_width=True)
    
    with col2:
        st.subheader("🏆 Top 10 States")
        top_states = data['dentists_by_state'].head(10)
        
        fig_bar = px.bar(
            top_states,
            x='COUNT',
            y='STATE',
            orientation='h',
            color='COUNT',
            color_continuous_scale='Blues',
            text='COUNT'
        )
        fig_bar.update_traces(texttemplate='%{text:,}', textposition='outside')
        fig_bar.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            showlegend=False,
            coloraxis_showscale=False,
            margin=dict(l=0, r=50, t=0, b=0),
            height=400
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # Row 2: Gender, Specialties, Practice Age
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("👥 Gender Distribution")
        fig_gender = px.pie(
            data['gender'],
            values='COUNT',
            names='GENDER',
            color='GENDER',
            color_discrete_map={'M': '#3498db', 'F': '#e74c3c'},
            hole=0.4
        )
        fig_gender.update_traces(textinfo='percent+label')
        fig_gender.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig_gender, use_container_width=True)
    
    with col2:
        st.subheader("🎓 Specialties")
        fig_spec = px.pie(
            data['specialties'],
            values='COUNT',
            names='SPECIALTY',
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        fig_spec.update_traces(textinfo='percent+label', textposition='inside')
        fig_spec.update_layout(margin=dict(l=0, r=0, t=0, b=0), showlegend=False)
        st.plotly_chart(fig_spec, use_container_width=True)
    
    with col3:
        st.subheader("📅 Practice Age (NPI)")
        fig_age = px.bar(
            data['age_cohorts'],
            x='COHORT',
            y='COUNT',
            color='COUNT',
            color_continuous_scale='Greens',
            text='COUNT'
        )
        fig_age.update_traces(texttemplate='%{text:,}', textposition='outside')
        fig_age.update_layout(
            showlegend=False,
            coloraxis_showscale=False,
            xaxis_title="Years Since NPI Registration",
            yaxis_title="Count",
            margin=dict(l=0, r=0, t=0, b=0)
        )
        st.plotly_chart(fig_age, use_container_width=True)
    
    # Row 3: Decision Makers Map
    st.markdown("---")
    st.subheader("🎯 Decision Makers (Dentist Practice Owners)")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        dm_df = data['dm_by_state'].copy()
        dm_df['STATE_NAME'] = dm_df['STATE'].map(STATE_NAMES)
        
        fig_dm_map = px.choropleth(
            dm_df,
            locations='STATE',
            locationmode='USA-states',
            color='COUNT',
            scope='usa',
            color_continuous_scale='Oranges',
            hover_name='STATE_NAME',
            hover_data={'COUNT': ':,', 'STATE': False},
            labels={'COUNT': 'Decision Makers'}
        )
        fig_dm_map.update_layout(
            geo=dict(bgcolor='rgba(0,0,0,0)'),
            margin=dict(l=0, r=0, t=0, b=0),
            coloraxis_colorbar=dict(title="Count")
        )
        st.plotly_chart(fig_dm_map, use_container_width=True)
    
    with col2:
        st.markdown("""
        ### 🎯 High-Value Targets
        
        **Decision Makers** are dentists we've confirmed as practice owners:
        
        - ✅ Licensed dentist (DDS/DMD)
        - ✅ Listed as Authorized Official
        - ✅ Matched to individual NPI
        - ✅ Has contact info
        
        **Total:** {:,} confirmed owners
        
        ---
        
        **Next: Enrich** the {:,} unmatched auth officials with Wiza to expand this list.
        """.format(data['stats']['decision_makers'], data['stats']['auth_officials']))
    
    # Footer
    st.markdown("---")
    st.caption("Data source: NPI Registry | Last updated: Real-time from Snowflake")


if __name__ == "__main__":
    main()

