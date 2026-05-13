import os
import streamlit as st
import requests


load_dotenv()

API_BASE = "https://isp.beans.ai/enterprise/v1/lists/status_logs"
AUTH = os.getenv("AUTH")

st.title("Tracking Status Lookup")

tracking_id = st.text_input("Enter Tracking ID", placeholder="e.g. ABC123456")

if tracking_id:
    with st.spinner("Fetching status..."):
        try:
            resp = requests.get(
                API_BASE,
                params={"tracking_id": tracking_id, "readable": "true"},
                headers={"Authorization": AUTH},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            st.success(f"Status for `{tracking_id}`")
            st.json(data)
        except requests.exceptions.HTTPError as e:
            st.error(f"HTTP error: {e.response.status_code} — {e.response.text}")
        except requests.exceptions.RequestException as e:
            st.error(f"Request failed: {e}")
