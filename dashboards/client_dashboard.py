"""
Dental Market Intelligence Dashboard
Run: streamlit run dashboards/client_dashboard.py
"""

import os
import sys
import streamlit as st
import pandas as pd
import plotly.express as px

# Only US states (50) + DC
US_STATES_ONLY = ('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',
                  'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
                  'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',
                  'VA','WA','WV','WI','WY','DC')

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
    'DC': 'District of Columbia'
}

# Page config - no sidebar
st.set_page_config(
    page_title="Dental Market Intelligence",
    page_icon="🦷",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# OpenAI-inspired Modern Dark Theme
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Base - OpenAI dark theme */
    html, body, [class*="css"] { 
        font-family: 'Inter', -apple-system, sans-serif;
        background-color: #0d0d0d !important;
    }
    .main > div { background-color: #0d0d0d; }
    
    /* Header - subtle */
    .main-header {
        background: linear-gradient(135deg, #1a1a1a 0%, #2d2d2d 100%);
        padding: 2rem; border-radius: 16px; margin-bottom: 1.5rem; color: white;
        border: 1px solid rgba(255,255,255,0.1);
    }
    .main-header h1 { font-size: 2rem; font-weight: 600; margin-bottom: 0.3rem; }
    .main-header p { color: rgba(255,255,255,0.6); font-size: 0.95rem; }
    
    /* Section headers */
    .section-header {
        font-size: 1.1rem; font-weight: 600; color: #ffffff;
        margin-top: 1.5rem; margin-bottom: 1rem; padding-bottom: 0.5rem;
        border-bottom: 1px solid rgba(255,255,255,0.1);
    }
    
    /* Insight boxes - OpenAI green accent */
    .insight-box {
        background: rgba(16, 163, 127, 0.1);
        border-radius: 8px; padding: 1rem; margin: 0.75rem 0;
        border-left: 3px solid #10a37f;
        color: rgba(255,255,255,0.8);
        font-size: 0.9rem;
    }
    
    /* TABS - Clean dark with hover lift */
    .stTabs [data-baseweb="tab-list"] { 
        gap: 8px; 
        background: rgba(255,255,255,0.03);
        padding: 8px;
        border-radius: 12px;
    }
    
    .stTabs [data-baseweb="tab"] { 
        height: 48px !important; 
        padding: 0 20px !important; 
        font-size: 0.9rem !important; 
        font-weight: 600 !important;
        border-radius: 8px !important;
        border: none !important;
        background: rgba(255,255,255,0.05) !important;
        color: rgba(255,255,255,0.7) !important;
        transition: all 0.2s ease !important;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        transform: translateY(-2px) !important;
        background: rgba(255,255,255,0.1) !important;
        color: white !important;
    }
    
    .stTabs [aria-selected="true"] { 
        background: #10a37f !important;
        color: white !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 4px 12px rgba(16, 163, 127, 0.3) !important;
    }
    
    .stTabs [data-baseweb="tab-highlight"] { display: none !important; }
    .stTabs [data-baseweb="tab-border"] { display: none !important; }
    
    /* Sidebar */
    [data-testid="stSidebar"] { background: #0d0d0d !important; border-right: 1px solid rgba(255,255,255,0.1); }
    [data-testid="stSidebar"] * { color: rgba(255,255,255,0.8) !important; }
    
    /* Charts transparent */
    .stPlotlyChart { background: transparent !important; }
    [data-testid="stMetric"] { background: transparent !important; }
    
    /* DataFrame */
    .stDataFrame { background: rgba(255,255,255,0.02) !important; border-radius: 8px !important; }
    
    /* Buttons - OpenAI green */
    .stDownloadButton button {
        background: #10a37f !important;
        color: white !important;
        border: none !important;
        border-radius: 6px !important;
        font-weight: 500 !important;
    }
    .stDownloadButton button:hover { background: #0d8a6a !important; }
    
    /* Selectbox */
    [data-testid="stSelectbox"] > div { background: rgba(255,255,255,0.05) !important; border-radius: 8px !important; }
    
    hr { border-color: rgba(255,255,255,0.08) !important; }
    
    /* KPI cards - subtle */
    .kpi-row { display: flex; gap: 12px; margin-bottom: 1rem; }
    .kpi-card {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 8px; padding: 12px 16px; text-align: center; flex: 1;
    }
    .kpi-value { font-size: 1.4rem; font-weight: 600; color: white; }
    .kpi-label { font-size: 0.75rem; color: rgba(255,255,255,0.5); text-transform: uppercase; letter-spacing: 0.5px; }
</style>
""", unsafe_allow_html=True)


def check_password():
    """Simple password protection"""
    if os.getenv('SKIP_AUTH', 'false').lower() == 'true':
        return True
    if st.session_state.get('authenticated', False):
        return True
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("### 🦷 Dental Market Intelligence")
        st.markdown("*Enter credentials to access the dashboard*")
        password = st.text_input("Password", type="password", key="password_input")
        if st.button("Login", type="primary", use_container_width=True):
            try:
                correct_password = st.secrets.get("auth", {}).get("password", "buffered")
            except:
                correct_password = os.getenv('DASHBOARD_PASSWORD', 'buffered')
            if password == correct_password:
                st.session_state['authenticated'] = True
                st.rerun()
            else:
                st.error("Incorrect password")
        st.caption("Contact: zander@consciousfounders.com")
    return False


def get_snowflake_connection():
    """Get Snowflake connection - works with Streamlit Cloud and local"""
    import snowflake.connector
    
    # Try Streamlit secrets first (for Streamlit Cloud)
    try:
        if hasattr(st, 'secrets') and 'snowflake' in st.secrets:
            return snowflake.connector.connect(
                account=st.secrets.snowflake.account,
                user=st.secrets.snowflake.user,
                password=st.secrets.snowflake.password,
                warehouse=st.secrets.snowflake.warehouse,
                database=st.secrets.snowflake.database,
                schema=st.secrets.snowflake.get('schema', 'CLEAN')
            )
    except:
        pass  # Silently fall back to env vars
    
    # Fall back to environment variables (for local dev)
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT', 'JW33852'),
        user=os.getenv('SNOWFLAKE_USER', 'ZANDER'),
        password=os.getenv('SNOWFLAKE_PASSWORD', ''),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'DENTAL_LEADS'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'CLEAN')
    )


@st.cache_data(ttl=300)
def load_data():
    """Load data from Snowflake"""
    us_states_sql = "','".join(US_STATES_ONLY)
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        # State insights
        cursor.execute(f"SELECT * FROM CLEAN.V_STATE_INSIGHTS WHERE STATE IN ('{us_states_sql}')")
        state_insights = pd.DataFrame(cursor.fetchall(), 
            columns=['STATE', 'TOTAL_DENTISTS', 'FEMALE_DENTISTS', 'FEMALE_PCT', 
                    'NEW_PRACTICES', 'GROWTH_PRACTICES', 'ESTABLISHED_PRACTICES',
                    'INNOVATION_READY_PCT', 'ZIP_COUNT', 'CITY_COUNT'])
        for col in ['TOTAL_DENTISTS', 'FEMALE_DENTISTS', 'FEMALE_PCT', 'NEW_PRACTICES', 
                   'GROWTH_PRACTICES', 'ESTABLISHED_PRACTICES', 'INNOVATION_READY_PCT', 
                   'ZIP_COUNT', 'CITY_COUNT']:
            state_insights[col] = pd.to_numeric(state_insights[col], errors='coerce').fillna(0)
        
        # County density
        cursor.execute("SELECT * FROM CLEAN.V_COUNTY_DENSITY ORDER BY PROVIDER_COUNT DESC LIMIT 500")
        county_density = pd.DataFrame(cursor.fetchall(),
            columns=['STATE', 'COUNTY', 'STATE_COUNTY', 'PROVIDER_COUNT', 'NEW_PRACTICE_COUNT', 'INNOVATION_SCORE'])
        for col in ['PROVIDER_COUNT', 'NEW_PRACTICE_COUNT', 'INNOVATION_SCORE']:
            county_density[col] = pd.to_numeric(county_density[col], errors='coerce').fillna(0)
        
        # City density
        cursor.execute(f"SELECT * FROM CLEAN.V_CITY_DENSITY WHERE STATE IN ('{us_states_sql}') ORDER BY PROVIDER_COUNT DESC LIMIT 100")
        city_density = pd.DataFrame(cursor.fetchall(),
            columns=['CITY', 'STATE', 'CITY_STATE', 'PROVIDER_COUNT', 'FEMALE_COUNT', 
                    'FEMALE_PCT', 'NEW_PRACTICE_COUNT', 'MID_PRACTICE_COUNT', 'ESTABLISHED_COUNT'])
        for col in ['PROVIDER_COUNT', 'FEMALE_COUNT', 'FEMALE_PCT', 'NEW_PRACTICE_COUNT', 
                   'MID_PRACTICE_COUNT', 'ESTABLISHED_COUNT']:
            city_density[col] = pd.to_numeric(city_density[col], errors='coerce').fillna(0)
        
        # Organizations by state
        cursor.execute(f"""
            SELECT STATE, COUNT(*) as ORG_COUNT,
                   COUNT(CASE WHEN PRACTICE_AGE_COHORT IN ('Very New (0-2 yrs)', 'New (2-5 yrs)') THEN 1 END) as NEW_ORGS,
                   ROUND(COUNT(CASE WHEN PRACTICE_AGE_COHORT IN ('Very New (0-2 yrs)', 'New (2-5 yrs)') THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) as INNOVATION_SCORE
            FROM CLEAN.V_ORGANIZATIONS WHERE STATE IN ('{us_states_sql}')
            GROUP BY STATE ORDER BY ORG_COUNT DESC
        """)
        org_by_state = pd.DataFrame(cursor.fetchall(), columns=['STATE', 'ORG_COUNT', 'NEW_ORGS', 'INNOVATION_SCORE'])
        for col in ['ORG_COUNT', 'NEW_ORGS', 'INNOVATION_SCORE']:
            org_by_state[col] = pd.to_numeric(org_by_state[col], errors='coerce').fillna(0)
        
        # Organizations by city
        cursor.execute(f"""
            SELECT STATE, CITY, COUNT(*) as ORG_COUNT,
                   COUNT(CASE WHEN PRACTICE_AGE_COHORT IN ('Very New (0-2 yrs)', 'New (2-5 yrs)') THEN 1 END) as NEW_ORGS,
                   ROUND(COUNT(CASE WHEN PRACTICE_AGE_COHORT IN ('Very New (0-2 yrs)', 'New (2-5 yrs)') THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) as INNOVATION_SCORE
            FROM CLEAN.V_ORGANIZATIONS WHERE STATE IN ('{us_states_sql}')
            GROUP BY STATE, CITY HAVING COUNT(*) >= 3
            ORDER BY ORG_COUNT DESC LIMIT 200
        """)
        organizations = pd.DataFrame(cursor.fetchall(), columns=['STATE', 'CITY', 'ORG_COUNT', 'NEW_ORGS', 'INNOVATION_SCORE'])
        for col in ['ORG_COUNT', 'NEW_ORGS', 'INNOVATION_SCORE']:
            organizations[col] = pd.to_numeric(organizations[col], errors='coerce').fillna(0)
        
        # Market opportunity
        cursor.execute(f"SELECT * FROM CLEAN.V_MARKET_OPPORTUNITY WHERE STATE IN ('{us_states_sql}') ORDER BY NEW_PRACTICE_PCT DESC LIMIT 200")
        market_opp = pd.DataFrame(cursor.fetchall(),
            columns=['CITY', 'STATE', 'MARKET', 'TOTAL_PROVIDERS', 'NEW_PRACTICES', 'NEW_PRACTICE_PCT', 'FEMALE_PCT', 'MARKET_TYPE'])
        for col in ['TOTAL_PROVIDERS', 'NEW_PRACTICES', 'NEW_PRACTICE_PCT', 'FEMALE_PCT']:
            market_opp[col] = pd.to_numeric(market_opp[col], errors='coerce').fillna(0)
        
        # Specialty
        cursor.execute(f"""
            SELECT * FROM CLEAN.V_SPECIALTY_BY_STATE 
            WHERE STATE IN (SELECT STATE FROM CLEAN.V_STATE_INSIGHTS WHERE STATE IN ('{us_states_sql}') ORDER BY TOTAL_DENTISTS DESC LIMIT 10)
        """)
        specialty = pd.DataFrame(cursor.fetchall(), columns=['STATE', 'SPECIALTY', 'PROVIDER_COUNT'])
        specialty['PROVIDER_COUNT'] = pd.to_numeric(specialty['PROVIDER_COUNT'], errors='coerce').fillna(0)
        
        # Stats
        cursor.execute(f"""
            SELECT 
                (SELECT COUNT(*) FROM CLEAN.V_INDIVIDUAL_DENTISTS WHERE STATE IN ('{us_states_sql}')),
                (SELECT COUNT(*) FROM CLEAN.V_ORGANIZATIONS WHERE STATE IN ('{us_states_sql}')),
                (SELECT COUNT(*) FROM CLEAN.V_DECISION_MAKERS WHERE ORG_STATE IN ('{us_states_sql}')),
                51,
                (SELECT COUNT(DISTINCT CITY) FROM CLEAN.V_INDIVIDUAL_DENTISTS WHERE STATE IN ('{us_states_sql}')),
                (SELECT COUNT(DISTINCT COUNTY) FROM CLEAN.V_COUNTY_DENSITY)
        """)
        stats = cursor.fetchone()
        
    finally:
        cursor.close()
        conn.close()
    
    return {
        'state_insights': state_insights,
        'county_density': county_density,
        'city_density': city_density,
        'organizations': organizations,
        'org_by_state': org_by_state,
        'market_opp': market_opp,
        'specialty': specialty,
        'stats': {
            'dentists': int(stats[0]) if stats[0] else 0,
            'orgs': int(stats[1]) if stats[1] else 0,
            'decision_makers': int(stats[2]) if stats[2] else 0,
            'states': int(stats[3]) if stats[3] else 0,
            'cities': int(stats[4]) if stats[4] else 0,
            'counties': int(stats[5]) if stats[5] else 0
        }
    }


def main():
    if not check_password():
        return
    
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>🦷 Dental Market Intelligence</h1>
        <p>Comprehensive provider analytics and market segmentation insights</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading market data..."):
        data = load_data()
    
    # No sidebar - hide it completely
    st.markdown("""<style>[data-testid="stSidebar"] { display: none; }</style>""", unsafe_allow_html=True)
    
    # Subtle KPI Row
    st.markdown(f"""
    <div style="display: flex; gap: 12px; margin-bottom: 1.5rem;">
        <div class="kpi-card"><div class="kpi-value">{data['stats']['dentists']:,}</div><div class="kpi-label">Dentists</div></div>
        <div class="kpi-card"><div class="kpi-value">{data['stats']['orgs']:,}</div><div class="kpi-label">Practices</div></div>
        <div class="kpi-card"><div class="kpi-value">{data['stats']['decision_makers']:,}</div><div class="kpi-label">Owners</div></div>
        <div class="kpi-card"><div class="kpi-value">{data['stats']['states']}</div><div class="kpi-label">States</div></div>
        <div class="kpi-card"><div class="kpi-value">{data['stats']['counties']:,}</div><div class="kpi-label">Counties</div></div>
        <div class="kpi-card"><div class="kpi-value">{data['stats']['cities']:,}</div><div class="kpi-label">Cities</div></div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Clean tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Dentists", "Practices", "Segments", "Growth Markets", "Data"
    ])
    
    # Teal color scale (OpenAI-ish)
    teal_scale = [[0, '#0d3d38'], [0.5, '#10a37f'], [1, '#6ee7b7']]
    
    # Apply dark theme to a figure
    def apply_dark_theme(fig, category_order=None):
        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='rgba(255,255,255,0.8)', size=11),
        )
        fig.update_xaxes(gridcolor='rgba(255,255,255,0.06)', zerolinecolor='rgba(255,255,255,0.06)')
        fig.update_yaxes(gridcolor='rgba(255,255,255,0.06)', zerolinecolor='rgba(255,255,255,0.06)')
        if category_order:
            fig.update_yaxes(categoryorder=category_order)
        return fig
    
    # TAB 1: Dentists
    with tab1:
        col1, col2 = st.columns([3, 2])
        with col1:
            st.markdown('<div class="section-header">Dentist Density by State</div>', unsafe_allow_html=True)
            map_df = data['state_insights'].copy()
            map_df['STATE_NAME'] = map_df['STATE'].map(STATE_NAMES)
            fig_map = px.choropleth(map_df, locations='STATE', locationmode='USA-states',
                color='TOTAL_DENTISTS', scope='usa', color_continuous_scale=teal_scale,
                hover_name='STATE_NAME', hover_data={'TOTAL_DENTISTS': ':,', 'INNOVATION_READY_PCT': ':.1f', 'STATE': False})
            fig_map.update_layout(geo=dict(bgcolor='rgba(0,0,0,0)', lakecolor='rgba(0,0,0,0)', landcolor='#1a1a1a', showlakes=False), 
                margin=dict(l=0, r=0, t=0, b=0))
            apply_dark_theme(fig_map)
            st.plotly_chart(fig_map, use_container_width=True)
        
        with col2:
            st.markdown('<div class="section-header">Top 15 States</div>', unsafe_allow_html=True)
            top_states = data['state_insights'].nlargest(15, 'TOTAL_DENTISTS')
            fig_bar = px.bar(top_states, x='TOTAL_DENTISTS', y='STATE', orientation='h',
                color='TOTAL_DENTISTS', color_continuous_scale=teal_scale, text='TOTAL_DENTISTS')
            fig_bar.update_traces(texttemplate='%{text:,}', textposition='outside', textfont=dict(color='rgba(255,255,255,0.7)'))
            fig_bar.update_layout(showlegend=False, height=450, coloraxis_showscale=False, margin=dict(l=0, r=70, t=0, b=0))
            apply_dark_theme(fig_bar, 'total ascending')
            st.plotly_chart(fig_bar, use_container_width=True)
        
        st.markdown('<div class="section-header">Top 25 Counties</div>', unsafe_allow_html=True)
        st.markdown('<div class="insight-box"><strong>Innovation Score</strong> = % of dentists registered in last 5 years. Higher scores indicate markets more receptive to modern solutions.</div>', unsafe_allow_html=True)
        county_df = data['county_density'].head(25)
        fig_county = px.bar(county_df, x='STATE_COUNTY', y='PROVIDER_COUNT', color='INNOVATION_SCORE',
            color_continuous_scale=teal_scale, text='PROVIDER_COUNT')
        fig_county.update_traces(texttemplate='%{text:,}', textposition='outside', textfont=dict(color='rgba(255,255,255,0.7)'))
        fig_county.update_layout(xaxis_tickangle=-45, height=400, margin=dict(l=0, r=0, t=20, b=120))
        apply_dark_theme(fig_county)
        st.plotly_chart(fig_county, use_container_width=True)
    
    # TAB 2: Practices
    with tab2:
        col1, col2 = st.columns([3, 2])
        with col1:
            st.markdown('<div class="section-header">Dental Practices by State</div>', unsafe_allow_html=True)
            org_map_df = data['org_by_state'].copy()
            org_map_df['STATE_NAME'] = org_map_df['STATE'].map(STATE_NAMES)
            fig_org_map = px.choropleth(org_map_df, locations='STATE', locationmode='USA-states',
                color='ORG_COUNT', scope='usa', color_continuous_scale=teal_scale, hover_name='STATE_NAME',
                hover_data={'ORG_COUNT': ':,', 'INNOVATION_SCORE': ':.1f', 'STATE': False})
            fig_org_map.update_layout(geo=dict(bgcolor='rgba(0,0,0,0)', lakecolor='rgba(0,0,0,0)', landcolor='#1a1a1a', showlakes=False), 
                margin=dict(l=0, r=0, t=0, b=0))
            apply_dark_theme(fig_org_map)
            st.plotly_chart(fig_org_map, use_container_width=True)
        
        with col2:
            st.markdown('<div class="section-header">Top 15 States</div>', unsafe_allow_html=True)
            top_org = data['org_by_state'].nlargest(15, 'ORG_COUNT')
            fig_org_bar = px.bar(top_org, x='ORG_COUNT', y='STATE', orientation='h',
                color='ORG_COUNT', color_continuous_scale=teal_scale, text='ORG_COUNT')
            fig_org_bar.update_traces(texttemplate='%{text:,}', textposition='outside', textfont=dict(color='rgba(255,255,255,0.7)'))
            fig_org_bar.update_layout(showlegend=False, height=450, coloraxis_showscale=False, margin=dict(l=0, r=70, t=0, b=0))
            apply_dark_theme(fig_org_bar, 'total ascending')
            st.plotly_chart(fig_org_bar, use_container_width=True)
        
        st.markdown('<div class="section-header">Top 20 Cities</div>', unsafe_allow_html=True)
        org_city = data['organizations'].head(20)
        org_city['CITY_STATE'] = org_city['CITY'] + ', ' + org_city['STATE']
        fig_org_city = px.bar(org_city, x='CITY_STATE', y='ORG_COUNT', color='ORG_COUNT',
            color_continuous_scale=teal_scale, text='ORG_COUNT')
        fig_org_city.update_traces(texttemplate='%{text:,}', textposition='outside', textfont=dict(color='rgba(255,255,255,0.7)'))
        fig_org_city.update_layout(xaxis_tickangle=-45, height=400, coloraxis_showscale=False, margin=dict(l=0, r=0, t=20, b=100))
        apply_dark_theme(fig_org_city)
        st.plotly_chart(fig_org_city, use_container_width=True)
    
    # TAB 3: Segments
    with tab3:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-header">Innovation Score by State</div>', unsafe_allow_html=True)
            st.markdown('<div class="insight-box"><strong>Innovation Score</strong> = % registered in last 10 years. Higher = more receptive to new solutions.</div>', unsafe_allow_html=True)
            innov_df = data['state_insights'].nlargest(15, 'INNOVATION_READY_PCT')
            fig_innov = px.bar(innov_df, x='STATE', y='INNOVATION_READY_PCT', color='INNOVATION_READY_PCT',
                color_continuous_scale=teal_scale, text='INNOVATION_READY_PCT')
            fig_innov.update_traces(texttemplate='%{text:.1f}%', textposition='outside', textfont=dict(color='rgba(255,255,255,0.7)'))
            fig_innov.update_layout(showlegend=False, coloraxis_showscale=False, height=350)
            apply_dark_theme(fig_innov)
            st.plotly_chart(fig_innov, use_container_width=True)
        
        with col2:
            st.markdown('<div class="section-header">Practice Age Distribution</div>', unsafe_allow_html=True)
            new_count = data['state_insights']['NEW_PRACTICES'].sum()
            growth_count = data['state_insights']['GROWTH_PRACTICES'].sum()
            established_count = data['state_insights']['ESTABLISHED_PRACTICES'].sum()
            age_data = pd.DataFrame({
                'Cohort': ['New (0-5 yrs)', 'Growth (5-10 yrs)', 'Established (10+ yrs)'],
                'Count': [new_count, growth_count, established_count]
            })
            fig_age = px.pie(age_data, values='Count', names='Cohort', hole=0.5,
                color_discrete_map={'New (0-5 yrs)': '#10a37f', 'Growth (5-10 yrs)': '#3b82f6', 'Established (10+ yrs)': '#374151'})
            fig_age.update_traces(textinfo='percent+label', textposition='outside', textfont=dict(color='rgba(255,255,255,0.7)'))
            fig_age.update_layout(height=350, showlegend=False)
            apply_dark_theme(fig_age)
            st.plotly_chart(fig_age, use_container_width=True)
            total = new_count + growth_count + established_count
            new_pct = new_count / total * 100 if total > 0 else 0
            st.markdown(f'<div class="insight-box"><strong>{new_pct:.1f}%</strong> registered in last 5 years — early adopter targets.</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="section-header">Specialty Distribution</div>', unsafe_allow_html=True)
        spec_national = data['specialty'].groupby('SPECIALTY')['PROVIDER_COUNT'].sum().reset_index().sort_values('PROVIDER_COUNT', ascending=False)
        fig_spec = px.bar(spec_national, y='SPECIALTY', x='PROVIDER_COUNT', orientation='h', 
            color='PROVIDER_COUNT', color_continuous_scale=teal_scale, text='PROVIDER_COUNT')
        fig_spec.update_traces(texttemplate='%{text:,}', textposition='outside', textfont=dict(color='rgba(255,255,255,0.7)'))
        fig_spec.update_layout(showlegend=False, height=400, coloraxis_showscale=False, margin=dict(l=0, r=70, t=20, b=0))
        apply_dark_theme(fig_spec, 'total ascending')
        st.plotly_chart(fig_spec, use_container_width=True)
    
    # TAB 4: Growth Markets
    with tab4:
        st.markdown('<div class="section-header">High-Growth Market Opportunities</div>', unsafe_allow_html=True)
        st.markdown('<div class="insight-box"><strong>High Growth</strong> = 50+ providers with >15% new practices in last 5 years.</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 2])
        with col1:
            market_counts = data['market_opp']['MARKET_TYPE'].value_counts()
            fig_pie = px.pie(values=market_counts.values, names=market_counts.index, hole=0.5,
                color_discrete_map={'High Growth': '#10a37f', 'Growing': '#3b82f6', 'Established': '#6366f1', 'Mid-Size': '#8b5cf6', 'Emerging': '#374151'})
            fig_pie.update_traces(textinfo='percent+label', textfont=dict(color='rgba(255,255,255,0.7)'))
            fig_pie.update_layout(height=320, showlegend=False)
            apply_dark_theme(fig_pie)
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            fig_scatter = px.scatter(data['market_opp'].head(100), x='TOTAL_PROVIDERS', y='NEW_PRACTICE_PCT',
                color='MARKET_TYPE', size='TOTAL_PROVIDERS', hover_name='MARKET',
                color_discrete_map={'High Growth': '#10a37f', 'Growing': '#3b82f6', 'Established': '#6366f1', 'Mid-Size': '#8b5cf6', 'Emerging': '#374151'})
            fig_scatter.update_layout(height=320)
            apply_dark_theme(fig_scatter)
            st.plotly_chart(fig_scatter, use_container_width=True)
        
        st.markdown('<div class="section-header">Top Growth Markets</div>', unsafe_allow_html=True)
        growth_markets = data['market_opp'][data['market_opp']['MARKET_TYPE'].isin(['High Growth', 'Growing'])].head(25)
        st.dataframe(growth_markets[['MARKET', 'TOTAL_PROVIDERS', 'NEW_PRACTICES', 'NEW_PRACTICE_PCT', 'MARKET_TYPE']].rename(
            columns={'MARKET': 'Market', 'TOTAL_PROVIDERS': 'Total', 'NEW_PRACTICES': 'New', 'NEW_PRACTICE_PCT': 'Innovation %', 'MARKET_TYPE': 'Type'}),
            use_container_width=True, hide_index=True)
    
    # TAB 5: Data
    with tab5:
        st.markdown('<div class="section-header">State-Level Data</div>', unsafe_allow_html=True)
        st.dataframe(data['state_insights'], use_container_width=True, hide_index=True)
        st.download_button("Download State Data", data['state_insights'].to_csv(index=False), "dental_state_data.csv", "text/csv")
        
        st.markdown('<div class="section-header">County-Level Data</div>', unsafe_allow_html=True)
        st.dataframe(data['county_density'].head(100), use_container_width=True, hide_index=True)
        st.download_button("Download County Data", data['county_density'].to_csv(index=False), "dental_county_data.csv", "text/csv")
    
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0; color: rgba(255,255,255,0.3); font-size: 0.8rem;">
        Data Source: NPI Registry · consciousfounders.com
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
