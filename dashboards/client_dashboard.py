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

# Page config
st.set_page_config(
    page_title="Dental Market Intelligence",
    page_icon="🦷",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
    .main-header {
        background: linear-gradient(135deg, #0f2027 0%, #203a43 50%, #2c5364 100%);
        padding: 2rem; border-radius: 12px; margin-bottom: 2rem; color: white;
    }
    .section-header {
        font-size: 1.4rem; font-weight: 600; color: #203a43;
        margin-top: 2rem; margin-bottom: 1rem; padding-bottom: 0.5rem;
        border-bottom: 2px solid #e0e0e0;
    }
    .insight-box {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border-radius: 8px; padding: 1rem; margin: 0.5rem 0;
        border-left: 3px solid #2c5364;
    }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; background-color: #f0f2f6; padding: 0.5rem; border-radius: 10px; }
    .stTabs [data-baseweb="tab"] { height: 60px; padding: 0 24px; font-size: 1.1rem; font-weight: 600;
        background-color: white; border-radius: 8px; border: none; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .stTabs [aria-selected="true"] { background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%); color: white !important; }
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
    
    # Try Streamlit secrets first
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
    except Exception as e:
        st.warning(f"Streamlit secrets not available: {e}")
    
    # Fall back to environment variables
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
    
    # Sidebar
    st.sidebar.title("🦷 Filters")
    all_states = ['All States'] + sorted([s for s in data['state_insights']['STATE'].tolist() if s in US_STATES_ONLY])
    selected_state = st.sidebar.selectbox("Select State", all_states)
    
    # KPIs
    def kpi_card(label, value, emoji=""):
        return f"""<div style="background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%); 
            padding: 1rem; border-radius: 10px; text-align: center; color: white;">
            <div style="font-size: 0.8rem; opacity: 0.85; margin-bottom: 0.3rem;">{emoji} {label}</div>
            <div style="font-size: 1.5rem; font-weight: 700;">{value}</div></div>"""
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1: st.markdown(kpi_card("Dentists", f"{data['stats']['dentists']:,}", "👨‍⚕️"), unsafe_allow_html=True)
    with col2: st.markdown(kpi_card("Practices", f"{data['stats']['orgs']:,}", "🏢"), unsafe_allow_html=True)
    with col3: st.markdown(kpi_card("Practice Owners", f"{data['stats']['decision_makers']:,}", "🎯"), unsafe_allow_html=True)
    with col4: st.markdown(kpi_card("States", f"{data['stats']['states']}", "🗺️"), unsafe_allow_html=True)
    with col5: st.markdown(kpi_card("Counties", f"{data['stats']['counties']:,}", "📍"), unsafe_allow_html=True)
    with col6: st.markdown(kpi_card("Cities", f"{data['stats']['cities']:,}", "🏙️"), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📍 DENTISTS", "🏢 PRACTICES", "📊 SEGMENTS", "🎯 GROWTH MARKETS", "📋 DATA"
    ])
    
    # TAB 1: Dentists
    with tab1:
        col1, col2 = st.columns([3, 2])
        with col1:
            st.markdown('<div class="section-header">Dentist Density by State</div>', unsafe_allow_html=True)
            map_df = data['state_insights'].copy()
            map_df['STATE_NAME'] = map_df['STATE'].map(STATE_NAMES)
            fig_map = px.choropleth(map_df, locations='STATE', locationmode='USA-states',
                color='TOTAL_DENTISTS', scope='usa', color_continuous_scale='Blues',
                hover_name='STATE_NAME', hover_data={'TOTAL_DENTISTS': ':,', 'INNOVATION_READY_PCT': ':.1f', 'STATE': False})
            fig_map.update_layout(geo=dict(bgcolor='rgba(0,0,0,0)'), margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig_map, use_container_width=True)
        
        with col2:
            st.markdown('<div class="section-header">Top 15 States</div>', unsafe_allow_html=True)
            top_states = data['state_insights'].nlargest(15, 'TOTAL_DENTISTS')
            fig_bar = px.bar(top_states, x='TOTAL_DENTISTS', y='STATE', orientation='h',
                color='INNOVATION_READY_PCT', color_continuous_scale='RdYlGn', text='TOTAL_DENTISTS')
            fig_bar.update_traces(texttemplate='%{text:,}', textposition='outside')
            fig_bar.update_layout(yaxis={'categoryorder': 'total ascending'}, showlegend=False, height=450, margin=dict(l=0, r=80, t=0, b=0))
            st.plotly_chart(fig_bar, use_container_width=True)
        
        st.markdown('<div class="section-header">Top 25 Counties</div>', unsafe_allow_html=True)
        st.markdown('<div class="insight-box"><strong>Innovation Score</strong> = % of dentists registered with NPI in last 5 years.</div>', unsafe_allow_html=True)
        county_df = data['county_density'].head(25)
        fig_county = px.bar(county_df, x='STATE_COUNTY', y='PROVIDER_COUNT', color='INNOVATION_SCORE',
            color_continuous_scale='RdYlGn', text='PROVIDER_COUNT')
        fig_county.update_traces(texttemplate='%{text:,}', textposition='outside')
        fig_county.update_layout(xaxis_tickangle=-45, height=400, margin=dict(l=0, r=0, t=20, b=120))
        st.plotly_chart(fig_county, use_container_width=True)
    
    # TAB 2: Practices
    with tab2:
        col1, col2 = st.columns([3, 2])
        with col1:
            st.markdown('<div class="section-header">Dental Practices by State</div>', unsafe_allow_html=True)
            org_map_df = data['org_by_state'].copy()
            org_map_df['STATE_NAME'] = org_map_df['STATE'].map(STATE_NAMES)
            fig_org_map = px.choropleth(org_map_df, locations='STATE', locationmode='USA-states',
                color='ORG_COUNT', scope='usa', color_continuous_scale='Oranges', hover_name='STATE_NAME',
                hover_data={'ORG_COUNT': ':,', 'INNOVATION_SCORE': ':.1f', 'STATE': False})
            fig_org_map.update_layout(geo=dict(bgcolor='rgba(0,0,0,0)'), margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig_org_map, use_container_width=True)
        
        with col2:
            st.markdown('<div class="section-header">Top 15 States</div>', unsafe_allow_html=True)
            top_org = data['org_by_state'].nlargest(15, 'ORG_COUNT')
            fig_org_bar = px.bar(top_org, x='ORG_COUNT', y='STATE', orientation='h',
                color='INNOVATION_SCORE', color_continuous_scale='RdYlGn', text='ORG_COUNT')
            fig_org_bar.update_traces(texttemplate='%{text:,}', textposition='outside')
            fig_org_bar.update_layout(yaxis={'categoryorder': 'total ascending'}, showlegend=False, height=450, margin=dict(l=0, r=80, t=0, b=0))
            st.plotly_chart(fig_org_bar, use_container_width=True)
        
        st.markdown('<div class="section-header">Top 20 Cities by Practice Count</div>', unsafe_allow_html=True)
        org_city = data['organizations'].head(20)
        org_city['CITY_STATE'] = org_city['CITY'] + ', ' + org_city['STATE']
        fig_org_city = px.bar(org_city, x='CITY_STATE', y='ORG_COUNT', color='INNOVATION_SCORE',
            color_continuous_scale='RdYlGn', text='ORG_COUNT')
        fig_org_city.update_traces(texttemplate='%{text:,}', textposition='outside')
        fig_org_city.update_layout(xaxis_tickangle=-45, height=400, margin=dict(l=0, r=0, t=20, b=100))
        st.plotly_chart(fig_org_city, use_container_width=True)
    
    # TAB 3: Segments
    with tab3:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-header">Innovation Score by State</div>', unsafe_allow_html=True)
            st.markdown('<div class="insight-box"><strong>Innovation Score</strong> = % registered in last 10 years. Higher = more receptive to new solutions.</div>', unsafe_allow_html=True)
            innov_df = data['state_insights'].nlargest(15, 'INNOVATION_READY_PCT')
            fig_innov = px.bar(innov_df, x='STATE', y='INNOVATION_READY_PCT', color='INNOVATION_READY_PCT',
                color_continuous_scale='Greens', text='INNOVATION_READY_PCT')
            fig_innov.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
            fig_innov.update_layout(showlegend=False, coloraxis_showscale=False, height=350)
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
            fig_age = px.pie(age_data, values='Count', names='Cohort', hole=0.45,
                color_discrete_map={'New (0-5 yrs)': '#27ae60', 'Growth (5-10 yrs)': '#3498db', 'Established (10+ yrs)': '#7f8c8d'})
            fig_age.update_traces(textinfo='percent+label', textposition='outside')
            fig_age.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig_age, use_container_width=True)
            total = new_count + growth_count + established_count
            new_pct = new_count / total * 100 if total > 0 else 0
            st.markdown(f'<div class="insight-box"><strong>{new_pct:.1f}%</strong> registered in last 5 years — early adopter targets.</div>', unsafe_allow_html=True)
        
        st.markdown('<div class="section-header">Specialty Distribution</div>', unsafe_allow_html=True)
        spec_national = data['specialty'].groupby('SPECIALTY')['PROVIDER_COUNT'].sum().reset_index().sort_values('PROVIDER_COUNT', ascending=False)
        colors = ['#1abc9c', '#3498db', '#9b59b6', '#e74c3c', '#f39c12', '#2ecc71', '#e67e22', '#1abc9c', '#34495e', '#16a085']
        fig_spec = px.bar(spec_national, y='SPECIALTY', x='PROVIDER_COUNT', orientation='h', color='SPECIALTY',
            color_discrete_sequence=colors, text='PROVIDER_COUNT')
        fig_spec.update_traces(texttemplate='%{text:,}', textposition='outside')
        fig_spec.update_layout(yaxis={'categoryorder': 'total ascending'}, showlegend=False, height=400, margin=dict(l=0, r=80, t=20, b=0))
        st.plotly_chart(fig_spec, use_container_width=True)
    
    # TAB 4: Growth Markets
    with tab4:
        st.markdown('<div class="section-header">High-Growth Market Opportunities</div>', unsafe_allow_html=True)
        st.markdown('<div class="insight-box"><strong>High Growth</strong> = 50+ providers with >15% new practices in last 5 years.</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 2])
        with col1:
            market_counts = data['market_opp']['MARKET_TYPE'].value_counts()
            fig_pie = px.pie(values=market_counts.values, names=market_counts.index, hole=0.4,
                color_discrete_map={'High Growth': '#27ae60', 'Growing': '#3498db', 'Established': '#9b59b6', 'Mid-Size': '#e67e22', 'Emerging': '#f1c40f'})
            fig_pie.update_traces(textinfo='percent+label')
            fig_pie.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            fig_scatter = px.scatter(data['market_opp'].head(100), x='TOTAL_PROVIDERS', y='NEW_PRACTICE_PCT',
                color='MARKET_TYPE', size='TOTAL_PROVIDERS', hover_name='MARKET',
                color_discrete_map={'High Growth': '#27ae60', 'Growing': '#3498db', 'Established': '#9b59b6', 'Mid-Size': '#e67e22', 'Emerging': '#f1c40f'})
            fig_scatter.update_layout(height=300)
            st.plotly_chart(fig_scatter, use_container_width=True)
        
        st.markdown('<div class="section-header">Top Growth Markets</div>', unsafe_allow_html=True)
        growth_markets = data['market_opp'][data['market_opp']['MARKET_TYPE'].isin(['High Growth', 'Growing'])].head(25)
        st.dataframe(growth_markets[['MARKET', 'TOTAL_PROVIDERS', 'NEW_PRACTICES', 'NEW_PRACTICE_PCT', 'MARKET_TYPE']].rename(
            columns={'MARKET': 'Market', 'TOTAL_PROVIDERS': 'Total', 'NEW_PRACTICES': 'New', 'NEW_PRACTICE_PCT': 'Innovation %', 'MARKET_TYPE': 'Type'}),
            use_container_width=True, hide_index=True)
    
    # TAB 5: Data
    with tab5:
        st.markdown('<div class="section-header">State-Level Data</div>', unsafe_allow_html=True)
        if selected_state != 'All States':
            filtered = data['state_insights'][data['state_insights']['STATE'] == selected_state]
        else:
            filtered = data['state_insights']
        st.dataframe(filtered, use_container_width=True, hide_index=True)
        st.download_button("📥 Download CSV", filtered.to_csv(index=False), "dental_state_data.csv", "text/csv")
        
        st.markdown('<div class="section-header">County-Level Data</div>', unsafe_allow_html=True)
        st.dataframe(data['county_density'].head(100), use_container_width=True, hide_index=True)
        st.download_button("📥 Download Counties CSV", data['county_density'].to_csv(index=False), "dental_county_data.csv", "text/csv")
    
    st.markdown("---")
    st.caption("Data Source: National Provider Identifier (NPI) Registry")


if __name__ == "__main__":
    main()
