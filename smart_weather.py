import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="Multi-City Weather Dashboard",
    page_icon="🌤️",
    layout="wide"
)

# Database paths (matching your Airflow code structure)
BASE_PATH = Path(".")  # Current folder
DB_METEOSTAT = BASE_PATH / "meteostat_data.db"
DB_FORECAST = BASE_PATH / "forecast_data.db"
DB_CURRENT = BASE_PATH / "current_data.db"

# Database loading functions
@st.cache_data(ttl=3600)
def get_available_cities():
    """Dynamically fetch list of cities from databases"""
    cities = set()
    
    # Try to get cities from current weather database
    try:
        conn = sqlite3.connect(DB_CURRENT)
        query = "SELECT DISTINCT city FROM current"
        df = pd.read_sql_query(query, conn)
        conn.close()
        cities.update(df['city'].tolist())
    except Exception as e:
        st.warning(f"Could not load cities from current database: {e}")
    
    # Try to get cities from forecast database
    try:
        conn = sqlite3.connect(DB_FORECAST)
        query = "SELECT DISTINCT city FROM forecast"
        df = pd.read_sql_query(query, conn)
        conn.close()
        cities.update(df['city'].tolist())
    except Exception as e:
        st.warning(f"Could not load cities from forecast database: {e}")
    
    # Try to get cities from meteostat database
    try:
        conn = sqlite3.connect(DB_METEOSTAT)
        query = "SELECT DISTINCT city FROM meteostat"
        df = pd.read_sql_query(query, conn)
        conn.close()
        cities.update(df['city'].tolist())
    except Exception as e:
        st.warning(f"Could not load cities from meteostat database: {e}")
    
    return sorted(list(cities)) if cities else ["Nairobi", "Sydney", "New York", "London"]
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_current_weather():
    """Load current weather data from database"""
    try:
        conn = sqlite3.connect(DB_CURRENT)
        query = """
        SELECT city, datetime, temp_c, humidity, wind_kph, wind_dir, 
               precip_mm, aqi, condition, created_at
        FROM current
        ORDER BY created_at DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Get most recent record for each city
        df['created_at'] = pd.to_datetime(df['created_at'])
        df = df.sort_values('created_at').groupby('city').tail(1)
        return df
    except Exception as e:
        st.error(f"Error loading current weather: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_forecast_data():
    """Load forecast data from database"""
    try:
        conn = sqlite3.connect(DB_FORECAST)
        query = """
        SELECT city, datetime, temp_c, humidity, wind_kph, wind_dir,
               precip_mm, aqi, condition
        FROM forecast
        ORDER BY datetime
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df
    except Exception as e:
        st.error(f"Error loading forecast data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_meteostat_data():
    """Load historical meteostat data from database"""
    try:
        conn = sqlite3.connect(DB_METEOSTAT)
        query = """
        SELECT date, city, temperature, precipitation, snow, 
               wind_dir, wind_speed, humidity, cloud_cover, sunshine_duration
        FROM meteostat
        ORDER BY date
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        st.error(f"Error loading historical data: {e}")
        return pd.DataFrame()

# Main app
def main():
    st.title("🌤️ Multi-City Weather Comparison Dashboard")
    st.markdown("---")
    
    # Dynamically get available cities from databases
    CITIES = get_available_cities()
    
    if not CITIES:
        st.error("❌ No cities found in databases. Please ensure data has been collected.")
        return
    
    st.sidebar.success(f"✅ Found {len(CITIES)} cities in database")
    
    # Load all data
    current_df = load_current_weather()
    forecast_df = load_forecast_data()
    historical_df = load_meteostat_data()
    
    # Sidebar for city selection
    st.sidebar.header("🌍 Select Cities to Compare")
    
    # First city selection
    city1 = st.sidebar.selectbox("City 1", CITIES, index=0, key="city1")
    
    # Second city selection - exclude the first selected city
    available_cities_for_city2 = [city for city in CITIES if city != city1]
    
    if not available_cities_for_city2:
        st.sidebar.error("⚠️ Need at least 2 cities in database for comparison")
        return
    
    city2 = st.sidebar.selectbox("City 2", available_cities_for_city2, index=0, key="city2")
    
    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs([
        "📍 Current Weather", 
        "📈 Forecast Comparison", 
        "📊 Historical Analysis",
        "🔍 Detailed Metrics"
    ])
    
    # TAB 1: Current Weather Comparison
    with tab1:
        st.header(f"Current Weather: {city1} vs {city2}")
        
        if not current_df.empty:
            col1, col2 = st.columns(2)
            
            # City 1 Current Weather
            with col1:
                city1_data = current_df[current_df['city'] == city1]
                if not city1_data.empty:
                    display_current_weather_card(city1, city1_data.iloc[0])
                else:
                    st.warning(f"No current data for {city1}")
            
            # City 2 Current Weather
            with col2:
                city2_data = current_df[current_df['city'] == city2]
                if not city2_data.empty:
                    display_current_weather_card(city2, city2_data.iloc[0])
                else:
                    st.warning(f"No current data for {city2}")
            
            # Comparison metrics
            st.markdown("---")
            st.subheader("📊 Quick Comparison")
            create_comparison_chart(city1, city2, current_df)
        else:
            st.warning("No current weather data available")
    
    # TAB 2: Forecast Comparison
    with tab2:
        st.header(f"Weather Forecast: {city1} vs {city2}")
        
        if not forecast_df.empty:
            # Filter forecast data for selected cities
            forecast_city1 = forecast_df[forecast_df['city'] == city1].copy()
            forecast_city2 = forecast_df[forecast_df['city'] == city2].copy()
            
            if not forecast_city1.empty and not forecast_city2.empty:
                # Temperature forecast
                st.subheader("🌡️ Temperature Forecast (Next 24 Hours)")
                fig_temp = go.Figure()
                fig_temp.add_trace(go.Scatter(
                    x=forecast_city1['datetime'],
                    y=forecast_city1['temp_c'],
                    name=city1,
                    mode='lines+markers',
                    line=dict(color='#FF6B6B', width=3)
                ))
                fig_temp.add_trace(go.Scatter(
                    x=forecast_city2['datetime'],
                    y=forecast_city2['temp_c'],
                    name=city2,
                    mode='lines+markers',
                    line=dict(color='#4ECDC4', width=3)
                ))
                fig_temp.update_layout(
                    xaxis_title="Date/Time",
                    yaxis_title="Temperature (°C)",
                    hovermode='x unified',
                    height=400
                )
                st.plotly_chart(fig_temp, use_container_width=True)
                
                # Precipitation and Humidity
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("🌧️ Precipitation Forecast")
                    fig_precip = go.Figure()
                    fig_precip.add_trace(go.Bar(
                        x=forecast_city1['datetime'],
                        y=forecast_city1['precip_mm'],
                        name=city1,
                        marker_color='#FF6B6B'
                    ))
                    fig_precip.add_trace(go.Bar(
                        x=forecast_city2['datetime'],
                        y=forecast_city2['precip_mm'],
                        name=city2,
                        marker_color='#4ECDC4'
                    ))
                    fig_precip.update_layout(
                        xaxis_title="Date/Time",
                        yaxis_title="Precipitation (mm)",
                        height=350,
                        barmode='group'
                    )
                    st.plotly_chart(fig_precip, use_container_width=True)
                
                with col2:
                    st.subheader("💧 Humidity Forecast")
                    fig_humidity = go.Figure()
                    fig_humidity.add_trace(go.Scatter(
                        x=forecast_city1['datetime'],
                        y=forecast_city1['humidity'],
                        name=city1,
                        fill='tozeroy',
                        line=dict(color='#FF6B6B')
                    ))
                    fig_humidity.add_trace(go.Scatter(
                        x=forecast_city2['datetime'],
                        y=forecast_city2['humidity'],
                        name=city2,
                        fill='tozeroy',
                        line=dict(color='#4ECDC4')
                    ))
                    fig_humidity.update_layout(
                        xaxis_title="Date/Time",
                        yaxis_title="Humidity (%)",
                        height=350
                    )
                    st.plotly_chart(fig_humidity, use_container_width=True)
            else:
                st.warning("Insufficient forecast data for comparison")
        else:
            st.warning("No forecast data available")
    
    # TAB 3: Historical Analysis
    with tab3:
        st.header(f"Historical Weather Analysis: {city1} vs {city2}")
        
        if not historical_df.empty:
            # Filter historical data
            hist_city1 = historical_df[historical_df['city'] == city1].copy()
            hist_city2 = historical_df[historical_df['city'] == city2].copy()
            
            if not hist_city1.empty and not hist_city2.empty:
                # Calculate yearly averages
                st.subheader("📅 2025 Year-to-Date Averages")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    avg_temp1 = hist_city1['temperature'].mean()
                    avg_temp2 = hist_city2['temperature'].mean()
                    st.metric(
                        f"{city1} Avg Temp",
                        f"{avg_temp1:.1f}°C"
                    )
                    st.metric(
                        f"{city2} Avg Temp",
                        f"{avg_temp2:.1f}°C",
                        delta=f"{avg_temp2 - avg_temp1:.1f}°C"
                    )
                
                with col2:
                    avg_precip1 = hist_city1['precipitation'].sum()
                    avg_precip2 = hist_city2['precipitation'].sum()
                    st.metric(
                        f"{city1} Total Precip",
                        f"{avg_precip1:.0f}mm"
                    )
                    st.metric(
                        f"{city2} Total Precip",
                        f"{avg_precip2:.0f}mm",
                        delta=f"{avg_precip2 - avg_precip1:.0f}mm"
                    )
                
                with col3:
                    avg_humidity1 = hist_city1['humidity'].mean()
                    avg_humidity2 = hist_city2['humidity'].mean()
                    st.metric(
                        f"{city1} Avg Humidity",
                        f"{avg_humidity1:.0f}%"
                    )
                    st.metric(
                        f"{city2} Avg Humidity",
                        f"{avg_humidity2:.0f}%",
                        delta=f"{avg_humidity2 - avg_humidity1:.0f}%"
                    )
                
                with col4:
                    avg_wind1 = hist_city1['wind_speed'].mean()
                    avg_wind2 = hist_city2['wind_speed'].mean()
                    st.metric(
                        f"{city1} Avg Wind",
                        f"{avg_wind1:.1f}km/h"
                    )
                    st.metric(
                        f"{city2} Avg Wind",
                        f"{avg_wind2:.1f}km/h",
                        delta=f"{avg_wind2 - avg_wind1:.1f}km/h"
                    )
                
                # Temperature trend
                st.subheader("🌡️ Temperature Trend (2025 YTD)")
                fig_hist_temp = go.Figure()
                fig_hist_temp.add_trace(go.Scatter(
                    x=hist_city1['date'],
                    y=hist_city1['temperature'],
                    name=city1,
                    mode='lines',
                    line=dict(color='#FF6B6B', width=2)
                ))
                fig_hist_temp.add_trace(go.Scatter(
                    x=hist_city2['date'],
                    y=hist_city2['temperature'],
                    name=city2,
                    mode='lines',
                    line=dict(color='#4ECDC4', width=2)
                ))
                fig_hist_temp.update_layout(
                    xaxis_title="Date",
                    yaxis_title="Temperature (°C)",
                    hovermode='x unified',
                    height=400
                )
                st.plotly_chart(fig_hist_temp, use_container_width=True)
                
                # Precipitation comparison
                st.subheader("🌧️ Precipitation Comparison")
                fig_precip_hist = go.Figure()
                fig_precip_hist.add_trace(go.Bar(
                    x=hist_city1['date'],
                    y=hist_city1['precipitation'],
                    name=city1,
                    marker_color='#FF6B6B'
                ))
                fig_precip_hist.add_trace(go.Bar(
                    x=hist_city2['date'],
                    y=hist_city2['precipitation'],
                    name=city2,
                    marker_color='#4ECDC4'
                ))
                fig_precip_hist.update_layout(
                    xaxis_title="Date",
                    yaxis_title="Precipitation (mm)",
                    barmode='group',
                    height=400
                )
                st.plotly_chart(fig_precip_hist, use_container_width=True)
            else:
                st.warning("Insufficient historical data for comparison")
        else:
            st.warning("No historical data available")
    
    # TAB 4: Detailed Metrics
    with tab4:
        st.header("🔍 Detailed Metrics & Data Tables")
        
        # Show raw data tables
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader(f"📋 {city1} Current Weather")
            if not current_df.empty:
                city1_current = current_df[current_df['city'] == city1]
                if not city1_current.empty:
                    st.dataframe(city1_current, use_container_width=True)
        
        with col2:
            st.subheader(f"📋 {city2} Current Weather")
            if not current_df.empty:
                city2_current = current_df[current_df['city'] == city2]
                if not city2_current.empty:
                    st.dataframe(city2_current, use_container_width=True)
        
        # Historical statistics
        st.markdown("---")
        st.subheader("📊 Historical Statistics Summary")
        
        if not historical_df.empty:
            summary_data = []
            for city in [city1, city2]:
                city_hist = historical_df[historical_df['city'] == city]
                if not city_hist.empty:
                    summary_data.append({
                        'City': city,
                        'Avg Temperature (°C)': f"{city_hist['temperature'].mean():.2f}",
                        'Max Temperature (°C)': f"{city_hist['temperature'].max():.2f}",
                        'Min Temperature (°C)': f"{city_hist['temperature'].min():.2f}",
                        'Total Precipitation (mm)': f"{city_hist['precipitation'].sum():.2f}",
                        'Avg Humidity (%)': f"{city_hist['humidity'].mean():.2f}",
                        'Avg Wind Speed (km/h)': f"{city_hist['wind_speed'].mean():.2f}"
                    })
            
            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                st.dataframe(summary_df, use_container_width=True)

def display_current_weather_card(city, data):
    """Display a weather card for a city"""
    st.markdown(f"### 🌍 {city}")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("🌡️ Temperature", f"{data['temp_c']:.1f}°C")
        st.metric("💧 Humidity", f"{data['humidity']:.0f}%")
    
    with col2:
        st.metric("💨 Wind Speed", f"{data['wind_kph']:.1f} km/h")
        st.metric("🌧️ Precipitation", f"{data['precip_mm']:.1f} mm")
    
    with col3:
        st.metric("👁️ AQI", f"{data['aqi']:.0f}")
        st.info(f"☁️ {data['condition']}")
    
    st.caption(f"Last updated: {data['datetime']}")

def create_comparison_chart(city1, city2, current_df):
    """Create a radar chart comparing current conditions"""
    city1_data = current_df[current_df['city'] == city1].iloc[0]
    city2_data = current_df[current_df['city'] == city2].iloc[0]
    
    categories = ['Temperature', 'Humidity', 'Wind Speed', 'Precipitation', 'AQI']
    
    # Normalize values for radar chart (0-100 scale)
    city1_values = [
        city1_data['temp_c'] * 2,  # Scale temp
        city1_data['humidity'],
        city1_data['wind_kph'] * 2,  # Scale wind
        city1_data['precip_mm'] * 10,  # Scale precip
        city1_data['aqi']
    ]
    
    city2_values = [
        city2_data['temp_c'] * 2,
        city2_data['humidity'],
        city2_data['wind_kph'] * 2,
        city2_data['precip_mm'] * 10,
        city2_data['aqi']
    ]
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatterpolar(
        r=city1_values,
        theta=categories,
        fill='toself',
        name=city1,
        line_color='#FF6B6B'
    ))
    
    fig.add_trace(go.Scatterpolar(
        r=city2_values,
        theta=categories,
        fill='toself',
        name=city2,
        line_color='#4ECDC4'
    ))
    
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=True,
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()