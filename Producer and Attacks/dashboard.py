# dashboard.py - Ready with Stale Data Handling

"""
Module: dashboard.py
Description:
    This is a Streamlit-based web application that serves as the frontend for
    the Real-Time Ad-Tech Analyzer pipeline.

    It connects to a MongoDB database to visualize traffic metrics processed
    by Spark Streaming. It distinguishes between organic user traffic and
    potential bot attacks using probabilistic heuristics (Hits-Per-User Ratio).

    Key Features:
    - Real-Time Visualization: Auto-refreshing charts using Plotly and Streamlit.
    - Bot Detection: Estimates bot probability using a sigmoid function on traffic ratios.
    - Stale Data Handling: "Smart" cutoff logic to handle pipeline restarts or delays.
    - Campaign Filtering: Drill-down analysis for specific ad campaigns.
    - Data Quality Monitoring: Tracks malformed events dropped by the pipeline.

Dependencies:
    - streamlit: Web framework.
    - plotly: Interactive charting.
    - pandas: Data manipulation.
    - pymongo: Database connectivity.
"""

# Import time for sleep functions (used in auto-refresh loops)
import time
# Import datetime classes for timestamp manipulation and timezone handling
from datetime import datetime, timedelta, timezone
# Import pandas for creating DataFrames to drive charts and tables
import pandas as pd
# Import Plotly Graph Objects for custom, interactive plotting
import plotly.graph_objects as go
# Import Streamlit for the web dashboard interface
import streamlit as st
# Import MongoClient to connect to the MongoDB database
from pymongo import MongoClient
# Import math for mathematical functions (exp) used in the bot probability algorithm
import math

# Define Gaza Timezone (Fixed +02:00)
# define a fixed timezone offset to ensure consistent reporting
# regardless of the server's local system time.
GAZA_TZ = timezone(timedelta(hours=2))

# Page configuration
# Sets the browser tab title, favicon, and layout mode.
st.set_page_config(
    page_title="Real-Time Ad-Tech Analyzer",
    page_icon="📊",
    layout="wide",  # Uses the full width of the screen
)


# MongoDB connection caching
# @st.cache_resource ensures the connection object is created once and reused
# across re-runs, preventing connection leaks.
@st.cache_resource
def get_mongo_client():
    """
    Establishes and caches the MongoDB connection.

    Returns:
        MongoClient: The connected MongoDB client instance.
    """
    return MongoClient("mongodb://localhost:27017/")


def estimate_bot_traffic_percentage(ratio):
    """
    Estimates the percentage of HITS coming from bots based on the hits/user ratio.

    The logic uses a Sigmoid function to normalize the ratio into a 0-100% scale.
    Organic traffic typically has a ratio close to 1.0-1.5. Bot traffic often
    exhibits high repetition, driving the ratio up.

    Math:
        f(x) = 1 / (1 + e^(-k * (x - x0)))
        Where 'x' is the normalized ratio.

    Args:
        ratio (float): The calculated Hits Per User ratio (Total Hits / Unique Users).

    Returns:
        float: Estimated percentage of bot traffic (0.0 to 100.0).
    """
    # Baseline for normal organic behavior (approx 1.5 hits per user)
    normal_ratio = 1.5
    # Threshold where we consider traffic to be heavily bot-dominated
    high_bot_ratio = 8.0

    # If ratio is within normal limits, assume 0% bot traffic
    if ratio <= normal_ratio:
        return 0.0

    # Normalize the ratio to a 0-1 range between normal and high thresholds
    normalized = (ratio - normal_ratio) / (high_bot_ratio - normal_ratio)
    # Clamp values to strictly 0-1
    normalized = max(0, min(1, normalized))

    # Apply Sigmoid function to create a smooth S-curve transition
    # The -6 factor controls the steepness of the curve
    sigmoid = 1 / (1 + math.exp(-6 * (normalized - 0.5)))

    # Convert to percentage
    bot_percentage = sigmoid * 100

    # Clamp final result between 0 and 100
    return min(100, max(0, bot_percentage))


def get_risk_category(bot_percentage):
    """
    Determines the risk level label and color code based on bot percentage.

    Args:
        bot_percentage (float): The estimated bot percentage (0-100).

    Returns:
        tuple: (Label String, Color String for Streamlit metrics)
    """
    if bot_percentage < 15:
        return "LOW RISK", "normal"  # Green/Standard color
    elif bot_percentage < 40:
        return "MODERATE", "off"  # Grey/Neutral color
    elif bot_percentage < 70:
        return "HIGH RISK", "inverse"  # Red/Warning color
    else:
        return "CRITICAL", "inverse"  # Red/Warning color


def get_smart_cutoff(collection, minutes=5, max_staleness_minutes=10):
    """
    Calculates the optimal query start time (cutoff), handling stale data.

    In a production streaming environment, the pipeline might stop or lag.
    If we always query 'now() - 5 minutes', we might see empty charts if the
    pipeline stopped 20 minutes ago.

    Strategy:
    1. Check the timestamp of the *latest* document in the DB.
    2. If that data is fresh (< max_staleness_minutes), base the window on that time.
    3. If that data is stale (> max_staleness_minutes), fallback to 'now' and warn the user.

    Args:
        collection (Collection): MongoDB collection to check for latest data.
        minutes (int): The size of the time window to display (e.g., last 5 mins).
        max_staleness_minutes (int): Threshold to consider data "current".

    Returns:
        datetime: The calculated cutoff time for MongoDB queries.
    """
    # Get current UTC time for comparison
    now = datetime.now(timezone.utc)

    try:
        # Fetch the single most recent document based on window_start
        latest_doc = collection.find_one(sort=[("window_start", -1)])

        if latest_doc and "window_start" in latest_doc:
            latest_time = latest_doc["window_start"]

            # Ensure the fetched time is timezone-aware (UTC) for accurate math
            if latest_time.tzinfo is None:
                latest_time = latest_time.replace(tzinfo=timezone.utc)

            # Calculate how old the data is
            staleness = now - latest_time
            staleness_minutes = staleness.total_seconds() / 60

            if staleness_minutes <= max_staleness_minutes:
                # Scenario A: Data is fresh.
                # Use the DATA's timestamp as the anchor, not the system time.
                # This ensures charts look full even if there's a 2-minute pipeline lag.
                return latest_time - timedelta(minutes=minutes)
            else:
                # Scenario B: Data is stale (e.g., pipeline crashed 2 hours ago).
                # Show a warning and default to system time so the user sees the gap.
                st.warning(
                    f"⚠️ Latest data is {staleness_minutes:.1f} minutes old. "
                    f"Showing data from current time instead. "
                    f"Pipeline may be catching up..."
                )
                return now - timedelta(minutes=minutes)

    except Exception as e:
        # Log error to UI if DB read fails
        st.error(f"Error checking DB time: {e}")

    # Fallback: If DB is empty or error occurs, return standard 'now - minutes'
    return now - timedelta(minutes=minutes)


def get_pipeline_health(traffic_collection):
    """
    Diagnoses pipeline health by checking the recency of the last written document.

    Args:
        traffic_collection (Collection): MongoDB collection to check.

    Returns:
        tuple: (is_healthy (bool), status_message (str), staleness_seconds (float))
    """
    now = datetime.now(timezone.utc)

    try:
        # Check the latest document
        latest_doc = traffic_collection.find_one(sort=[("window_start", -1)])

        if not latest_doc:
            return False, "No data in database", None

        latest_time = latest_doc["window_start"]
        # Ensure UTC
        if latest_time.tzinfo is None:
            latest_time = latest_time.replace(tzinfo=timezone.utc)

        # Calculate delay
        staleness = now - latest_time
        staleness_seconds = staleness.total_seconds()

        # Determine health status based on delay thresholds
        if staleness_seconds < 60:
            return True, "Pipeline healthy - data is current", staleness_seconds
        elif staleness_seconds < 300:
            return True, "Pipeline may be catching up", staleness_seconds
        else:
            return False, "Pipeline appears stopped or delayed", staleness_seconds

    except Exception as e:
        return False, f"Error checking health: {e}", None


def get_data(collection, cutoff_time):
    """
    Fetches traffic metrics from MongoDB starting from the cutoff time.

    Args:
        collection (Collection): The MongoDB collection to query.
        cutoff_time (datetime): The start time for the query.

    Returns:
        list: A list of dictionaries containing the traffic data.
    """
    try:
        # Query: Get all documents where window_start >= cutoff_time
        # Projection: Exclude _id field as it's not needed for plotting
        cursor = (
            collection.find(
                {"window_start": {"$gte": cutoff_time}},
                {"_id": 0},
            )
            .sort("window_start", 1)  # Sort ascending for time-series plotting
        )
        data = list(cursor)
        return data
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return []


def render_campaign_metrics(campaign_df, campaign_name):
    """
    Renders the metric cards (Big Number Display) for a specific campaign.

    This abstracts the UI logic for displaying Total Hits, Unique Users,
    Ratio, and Bot Estimates into a reusable function.

    Args:
        campaign_df (DataFrame): The data for the specific campaign.
        campaign_name (str): The name of the campaign.
    """
    if len(campaign_df) == 0:
        st.warning(f"No data found for {campaign_name}")
        return

    # Get the most recent data point (the last row in the sorted DataFrame)
    latest = campaign_df.iloc[-1]

    # Extract metrics
    total_hits = int(latest.get('total_hits', 0))
    unique_users = int(latest.get('unique_users', 0))
    ratio = latest.get("hits_per_user_ratio", 0)

    # Calculate bot probabilities
    bot_prob = estimate_bot_traffic_percentage(ratio)
    risk_level, risk_color = get_risk_category(bot_prob)

    # Layout: 4 columns for 4 metrics
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric("Total Hits", f"{total_hits:,}")

    with c2:
        st.metric("Unique Users (HLL++)", f"{unique_users:,}")

    with c3:
        st.metric("Hits/User Ratio", f"{ratio:.2f}")

    with c4:
        # Delta color allows us to color-code the risk (Green/Red)
        st.metric(
            "Bot Traffic Estimate",
            f"{bot_prob:.1f}%",
            delta=risk_level,
            delta_color=risk_color
        )

    # Add explanatory text
    st.caption(
        f"🔍 **{campaign_name} Analysis:** "
        f"With a ratio of {ratio:.2f}, approximately {bot_prob:.1f}% of hits "
        f"are estimated to be from bots. Risk Level: **{risk_level}**"
    )


def render_dashboard():
    """
    Main execution function for the Streamlit dashboard.

    Orchestrates:
    1. Database connection.
    2. Sidebar configuration (Inputs).
    3. Global vs. Campaign data fetching.
    4. Chart and Table rendering.
    5. Auto-refresh loop.
    """

    # 1. Connect to Database
    client = get_mongo_client()
    db = client["adtech"]

    # Define collection references
    traffic_collection = db["traffic_metrics"]  # Global aggregations
    campaign_collection = db["campaign_metrics"]  # Per-campaign aggregations
    country_collection = db["country_metrics"]  # Per-country aggregations
    alerts_collection = db["alerts"]  # Anomalies detected by Spark
    quality_collection = db["quality_metrics"]  # Data quality stats

    # --- HEADER ---
    st.title("🎯 Real-Time Ad-Tech Traffic Analyzer")
    st.markdown("**Powered by HyperLogLog++ Probabilistic Counting**")

    #
    # === PIPELINE HEALTH CHECK ===
    # Check if data is flowing before rendering the rest
    is_healthy, health_msg, staleness = get_pipeline_health(traffic_collection)

    health_col1, health_col2 = st.columns([3, 1])

    with health_col1:
        if is_healthy:
            st.success(f"✅ {health_msg}")
        else:
            st.error(f"❌ {health_msg}")

    with health_col2:
        if staleness is not None:
            st.metric("Data Age", f"{int(staleness)}s")

    # --- SIDEBAR ---
    st.sidebar.title("Dashboard Controls")

    # Time mode selector allows user to switch between "Live" and "Historical" analysis
    time_mode = st.sidebar.radio(
        "Time Mode",
        options=["Auto (Smart)", "Current Time", "Historical"],
        help="""
        - Auto: Uses latest data if fresh (<10 min old), otherwise current time
        - Current Time: Always shows last N minutes from now
        - Historical: Manually select time range
        """
    )

    # Slider for window size (how much data to show on X-axis)
    time_window_minutes = st.sidebar.slider(
        "Time Window (minutes)",
        min_value=1,
        max_value=30,
        value=5,
        help="How many minutes of history to display"
    )

    # Logic for Historical Mode inputs
    if time_mode == "Historical":
        st.sidebar.markdown("**Historical Mode**")
        hours_back = st.sidebar.slider("Hours back", 0, 24, 0)
        minutes_back = st.sidebar.slider("Minutes back", 0, 59, 30)

        # Calculate exact start/end times based on user input
        end_time = datetime.now(timezone.utc) - timedelta(hours=hours_back, minutes=minutes_back)
        cutoff_time = end_time - timedelta(minutes=time_window_minutes)

        # Convert to Gaza time just for the UI label display (User convenience)
        st_start = cutoff_time.astimezone(GAZA_TZ).strftime('%H:%M')
        st_end = end_time.astimezone(GAZA_TZ).strftime('%H:%M')
        st.sidebar.info(f"Showing: {st_start} to {st_end} (Gaza Time)")

    else:
        cutoff_time = None  # Will be calculated dynamically below

    # Control refresh rate
    refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 1, 10, 1)

    # Filter Campaigns: Populate dropdown from DB
    try:
        # Fetch distinct campaign IDs from DB to populate dropdown
        campaigns_cursor = campaign_collection.distinct("campaign_id")
        # Filter out None and sort
        campaigns = sorted([c for c in campaigns_cursor if c is not None])
    except:
        campaigns = []

    # Dropdown for selecting campaign
    selected_campaign = st.sidebar.selectbox(
        "Filter Campaign",
        options=["(All campaigns)"] + campaigns,
        index=0,
    )

    # Calibration Info Expander (Educational content for user)
    with st.expander("ℹ️ How Bot Probability is Calculated", expanded=False):
        st.markdown("""
        **Bot Probability** estimates the percentage of **HITS** (not users) from bots.

        **Calibration:**
        - **Ratio 1.0-1.5**: 0% bot traffic (organic users browse normally)
        - **Ratio ~3.0**: ~30% bot traffic (matches generator setting)
        - **Ratio ~5.0**: ~70% bot traffic (heavy attack)
        - **Ratio 10.0+**: ~95% bot traffic (critical attack)

        **Why not 30% = 30% probability?**
        - The generator creates 30% bot *users*, but bots generate MORE hits per user
        - If bots click 20x more than humans, 30% bot users → 87% bot hits
        - This metric measures hit volume, not user count
        """)

    # Debug Stats (For system administrators)
    with st.expander("🔌 Database & Debug Stats", expanded=False):
        c1, c2, c3 = st.columns(3)
        c1.write(f"**Traffic Docs:** {traffic_collection.count_documents({})}")
        c2.write(f"**Campaign Docs:** {campaign_collection.count_documents({})}")
        c3.write(f"**Country Docs:** {country_collection.count_documents({})}")

        # Show latest timestamp available in DB
        latest_doc = traffic_collection.find_one(sort=[("window_start", -1)])
        if latest_doc:
            utc_time = latest_doc.get('window_start')

            # 1. Ensure the time from Mongo is treated as UTC
            if utc_time.tzinfo is None:
                utc_time = utc_time.replace(tzinfo=timezone.utc)

            # 2. Convert to Gaza Time using the constant we defined earlier
            gaza_time = utc_time.astimezone(GAZA_TZ)

            st.write(f"**Latest Data:** {gaza_time.strftime('%Y-%m-%d %H:%M:%S')} (Gaza Time)")

    st.markdown("---")

    # --- DETERMINE TIME WINDOW ---
    # Determine the start time (cutoff) for queries based on mode
    if time_mode == "Auto (Smart)":
        cutoff_time = get_smart_cutoff(
            traffic_collection,
            minutes=time_window_minutes,
            max_staleness_minutes=10
        )
    elif time_mode == "Current Time":
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=time_window_minutes)
    # Historical mode already set cutoff_time above

    # === CONDITIONAL RENDERING: GLOBAL vs CAMPAIGN VIEW ===

    if selected_campaign == "(All campaigns)":
        # ==================== GLOBAL VIEW ====================

        global_data = get_data(traffic_collection, cutoff_time)

        if global_data:
            df = pd.DataFrame(global_data)
            # Convert UTC (DB time) to Gaza Time (Display time) for the Chart X-Axis
            df["window_start"] = pd.to_datetime(df["window_start"]).dt.tz_localize("UTC").dt.tz_convert(GAZA_TZ)

            st.subheader(f"📊 Global Overview (Last {len(df)} Windows × 10 seconds)")

            latest = df.iloc[-1]

            # Calculate metrics for the latest window
            ratio = latest.get("hits_per_user_ratio", 0)
            bot_prob = estimate_bot_traffic_percentage(ratio)
            risk_level, risk_color = get_risk_category(bot_prob)

            # Display Global Metrics
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Hits", f"{int(latest.get('total_hits', 0)):,}")
            c2.metric("Unique Users (HLL++)", f"{int(latest.get('unique_users', 0)):,}")
            c3.metric("Hits/User Ratio", f"{ratio:.2f}")
            c4.metric(
                "Bot Traffic Estimate",
                f"{bot_prob:.1f}%",
                delta=risk_level,
                delta_color=risk_color
            )

            # Calculate and show latency (Time difference between Event Generation and Dashboard Display)
            now = datetime.now(timezone.utc)
            latest_window = latest.get("window_start")
            if latest_window.tzinfo is None:
                latest_window = latest_window.replace(tzinfo=timezone.utc)

            time_since_latest = now - latest_window
            latency_seconds = int(time_since_latest.total_seconds())

            st.caption(
                f"🔍 **Interpretation:** With a ratio of {ratio:.2f}, "
                f"approximately {bot_prob:.1f}% of hits are estimated to be from bots. "
                f"Risk Level: **{risk_level}** | "
                f"⏱️ Latency: **{latency_seconds}s** from event to display"
            )

            # === MAIN CHART ===
            fig = go.Figure()
            # Trace 1: Total Hits (Red Line)
            fig.add_trace(go.Scatter(
                x=df["window_start"],
                y=df["total_hits"],
                name="Total Hits",
                line=dict(color="#FF6B6B")
            ))
            # Trace 2: Unique Users (Teal Line)
            fig.add_trace(go.Scatter(
                x=df["window_start"],
                y=df["unique_users"],
                name="Unique Users (HLL++)",
                line=dict(color="#4ECDC4")
            ))

            # Trace 3: Ratio (Yellow Dashed Line) on Secondary Y-Axis
            fig.add_trace(go.Scatter(
                x=df["window_start"],
                y=df["hits_per_user_ratio"],
                name="Hits/User Ratio",
                line=dict(color="#FFD93D", dash="dash"),
                yaxis="y2"  # Map to second Y axis
            ))

            fig.update_layout(
                height=350,
                template="plotly_white",
                margin=dict(t=10, b=10),
                yaxis=dict(title="Hits / Users"),
                yaxis2=dict(
                    title="Ratio",
                    overlaying="y",
                    side="right"
                )
            )
            st.plotly_chart(fig, use_container_width=True)

        else:
            # Handle empty state (no data returned)
            st.warning(f"⚠️ No global data found since: {cutoff_time.strftime('%H:%M:%S UTC')}")
            st.info("""
            **Possible reasons:**
            1. Pipeline is still catching up after restart
            2. No events being generated (check ad_traffic_generator.py)
            3. Spark Streaming not running
            4. Time window is looking at wrong period (try 'Current Time' mode)
            """)

    else:
        # ==================== CAMPAIGN-SPECIFIC VIEW ====================
        # User selected a specific campaign

        st.subheader(f"🎯 Campaign: {selected_campaign}")

        # Query only specific campaign data
        camp_query = {
            "window_start": {"$gte": cutoff_time},
            "campaign_id": selected_campaign
        }

        camp_data = list(campaign_collection.find(camp_query, {"_id": 0}).sort("window_start", 1))

        if camp_data:
            camp_df = pd.DataFrame(camp_data)
            # Timezone conversion for display
            camp_df["window_start"] = pd.to_datetime(camp_df["window_start"]).dt.tz_localize("UTC").dt.tz_convert(
                GAZA_TZ)

            st.markdown(f"**Performance Metrics (Last {len(camp_df)} Windows × 10 seconds)**")
            # Render the reusable metrics component
            render_campaign_metrics(camp_df, selected_campaign)

            # Show latency
            now = datetime.now(timezone.utc)
            latest_camp = camp_df.iloc[-1]
            latest_window = latest_camp.get("window_start")
            if latest_window.tzinfo is None:
                latest_window = latest_window.replace(tzinfo=timezone.utc)

            time_since_latest = now - latest_window
            latency_seconds = int(time_since_latest.total_seconds())
            st.caption(f"⏱️ Data Latency: **{latency_seconds} seconds**")

            st.markdown("---")

            # === CAMPAIGN CHART ===
            fig_camp = go.Figure()

            # Plot Total Hits
            fig_camp.add_trace(go.Scatter(
                x=camp_df["window_start"],
                y=camp_df["total_hits"],
                name="Total Hits",
                line=dict(color="#FF6B6B")
            ))

            # Plot Unique Users
            fig_camp.add_trace(go.Scatter(
                x=camp_df["window_start"],
                y=camp_df["unique_users"],
                name="Unique Users",
                line=dict(color="#4ECDC4")
            ))

            # Plot Ratio (Secondary Axis)
            fig_camp.add_trace(go.Scatter(
                x=camp_df["window_start"],
                y=camp_df["hits_per_user_ratio"],
                name="Hits/User Ratio",
                line=dict(color="#FFD93D", dash="dash"),
                yaxis="y2"
            ))

            fig_camp.update_layout(
                height=350,
                template="plotly_white",
                margin=dict(t=10, b=10),
                yaxis=dict(title="Hits / Users"),
                yaxis2=dict(
                    title="Ratio",
                    overlaying="y",
                    side="right"
                )
            )

            st.plotly_chart(fig_camp, use_container_width=True)

            # === CAMPAIGN STATISTICS TABLE ===
            st.markdown("**Window-by-Window Breakdown**")

            stats_df = camp_df.copy()
            # Calculate bot estimate per row for the table
            stats_df["bot_estimate"] = stats_df["hits_per_user_ratio"].apply(
                estimate_bot_traffic_percentage
            ).round(1)

            # Filter columns for display
            display_df = stats_df[[
                "window_start",
                "total_hits",
                "unique_users",
                "hits_per_user_ratio",
                "bot_estimate"
            ]].copy()

            # Format time string for cleaner table
            display_df["window_start"] = display_df["window_start"].dt.strftime("%H:%M:%S")

            # Render Table
            st.dataframe(
                display_df.rename(columns={
                    "window_start": "Time",
                    "total_hits": "Hits",
                    "unique_users": "Users",
                    "hits_per_user_ratio": "Ratio",
                    "bot_estimate": "Bot %"
                }),
                use_container_width=True,
                hide_index=True
            )

        else:
            st.warning(f"No data found for campaign '{selected_campaign}' in this time window.")

    # === COMMON SECTIONS (Rendered for both Global and Campaign views) ===

    st.markdown("---")

    # === TOP CAMPAIGNS (Only show in global view) ===
    if selected_campaign == "(All campaigns)":
        st.subheader(f"🔥 Top 5 Campaigns (Last {time_window_minutes} Min)")

        # Aggregate metrics for the whole window per campaign
        camp_summary = list(
            campaign_collection.find(
                {"window_start": {"$gte": cutoff_time}},
                {"_id": 0, "campaign_id": 1, "total_hits": 1, "unique_users": 1}
            )
        )

        if camp_summary:
            camp_summary_df = pd.DataFrame(camp_summary)

            # Group by Campaign ID and Sum values
            top_campaigns = (
                camp_summary_df
                .groupby("campaign_id", as_index=False)
                .agg({
                    "total_hits": "sum",
                    "unique_users": "sum"
                })
            )

            # Recalculate ratio on aggregated data
            top_campaigns["ratio"] = (
                    top_campaigns["total_hits"] / top_campaigns["unique_users"]
            ).round(2)

            # Calculate bot probability
            top_campaigns["bot_probability"] = top_campaigns["ratio"].apply(
                estimate_bot_traffic_percentage
            ).round(1)

            # Sort by hits and take top 5
            top_campaigns = top_campaigns.sort_values("total_hits", ascending=False).head(5)

            st.dataframe(
                top_campaigns.rename(columns={
                    "campaign_id": "Campaign",
                    "total_hits": "Total Hits",
                    "unique_users": "Unique Users",
                    "ratio": "Hits/User",
                    "bot_probability": "Bot % Est."
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.caption("No campaign data available.")

    # === GEO & ALERTS ===
    st.subheader("📍 Geographic Distribution & Alerts")

    col_geo, col_alerts = st.columns(2)

    # -- Geographic Distribution Column --
    with col_geo:
        st.markdown("**🌍 Top Countries**")

        geo_query = {"window_start": {"$gte": cutoff_time}}

        c_data = list(country_collection.find(geo_query, {"_id": 0}))

        if c_data:
            c_df = pd.DataFrame(c_data)

            # Aggregate total hits across all windows per country
            geo_agg = c_df.groupby("country", as_index=False)["total_hits"].sum()

            # Filter out countries with insignificant traffic to reduce noise
            geo_agg = geo_agg[geo_agg["total_hits"] >= 10]

            # Sort and show top 5
            geo_agg = geo_agg.sort_values("total_hits", ascending=False).head(5)

            if len(geo_agg) > 0:
                st.bar_chart(geo_agg.set_index("country"))
            else:
                st.caption("No significant country data yet.")
        else:
            st.caption("No geo data.")

    # -- Alerts Column --
    with col_alerts:
        st.markdown("**🚨 Recent Alerts**")

        alert_query = {"window_start": {"$gte": cutoff_time}}

        # If a specific campaign is selected, filter alerts for that campaign OR global alerts
        if selected_campaign != "(All campaigns)":
            alert_query["$or"] = [
                {"campaign_id": selected_campaign},
                {"campaign_id": "GLOBAL"}
            ]

        # Fetch recent alerts
        alerts = list(
            alerts_collection.find(
                alert_query,
                {"_id": 0}
            ).sort("window_start", -1).limit(5)
        )

        if alerts:
            alert_display = []
            for alert in alerts:
                # Handle potential missing fields
                campaign_id = alert.get("campaign_id", "N/A")
                if campaign_id is None:
                    campaign_id = "N/A"

                severity = alert.get("severity_level", alert.get("severity", "UNKNOWN"))
                ratio = alert.get("hits_per_user_ratio")
                ratio_str = f"{ratio:.2f}" if ratio is not None else "N/A"

                # Construct alert object for display
                alert_display.append({
                    "Time": alert.get("window_start", "").strftime("%H:%M:%S") if isinstance(alert.get("window_start"),
                                                                                             datetime) else str(
                        alert.get("window_start", "")),
                    "Campaign": campaign_id,
                    "Severity": severity,
                    "Ratio": ratio_str,
                    # Truncate long messages
                    "Message": alert.get("message", "")[:40] + "..." if len(
                        alert.get("message", "")) > 40 else alert.get("message", "")
                })

            st.dataframe(alert_display, use_container_width=True, hide_index=True)
        else:
            st.success("No alerts.")

    st.markdown("---")

    # === DATA QUALITY SECTION ===
    st.subheader(f"🧪 Data Quality (Last {time_window_minutes} Minutes)")

    # Fetch quality metrics
    q_data = list(
        quality_collection.find(
            {"window_start": {"$gte": cutoff_time}},
            {"_id": 0},
        ).sort("window_start", 1)
    )

    if q_data:
        q_df = pd.DataFrame(q_data)
        # Sum metrics over the window
        total_events = int(q_df["total_events"].sum()) if "total_events" in q_df else 0
        malformed_events = int(q_df["malformed_events"].sum()) if "malformed_events" in q_df else 0

        # Calculate Percentage
        malformed_pct = (malformed_events / total_events * 100) if total_events > 0 else 0.0

        # Display Metrics
        q_col1, q_col2, q_col3 = st.columns(3)
        with q_col1:
            st.metric("Events processed", f"{total_events:,}")
        with q_col2:
            st.metric("Malformed events", f"{malformed_events:,}")
        with q_col3:
            # Color code health status
            health_status = "HEALTHY" if malformed_pct < 2 else "DEGRADED" if malformed_pct < 5 else "UNHEALTHY"
            st.metric(
                "Malformed %",
                f"{malformed_pct:.2f}%",
                delta=health_status,
                delta_color="inverse" if malformed_pct > 2 else "normal"
            )

        st.caption(
            f"ℹ️ Data quality metrics are **global** across all campaigns. "
            f"Out of {total_events:,} events processed, {malformed_events:,} "
            f"({malformed_pct:.2f}%) had data quality issues."
        )
    else:
        st.caption("No data-quality metrics yet for this time range.")

    # Auto-refresh Logic
    # Sleep for X seconds (defined by slider) then rerun the script
    time.sleep(refresh_interval)
    st.rerun()


# Standard Python boilerplate to run the function if executed directly
if __name__ == "__main__":
    render_dashboard()