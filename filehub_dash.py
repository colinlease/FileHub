# filehub.py
# Microservice module for secure file transfer between Streamlit apps via S3

import boto3
import os
from datetime import datetime
from datetime import timedelta
import streamlit as st
from streamlit_autorefresh import st_autorefresh


# Prefer Streamlit secrets for Streamlit Cloud compatibility, fallback to env vars
AWS_ACCESS_KEY_ID = st.secrets.get("AWS_ACCESS_KEY_ID", os.getenv("AWS_ACCESS_KEY_ID"))
AWS_SECRET_ACCESS_KEY = st.secrets.get("AWS_SECRET_ACCESS_KEY", os.getenv("AWS_SECRET_ACCESS_KEY"))
S3_BUCKET_NAME = st.secrets.get("S3_BUCKET_NAME", os.getenv("S3_BUCKET_NAME", "your-bucket-name"))
S3_REGION = st.secrets.get("S3_REGION", os.getenv("S3_REGION", "us-east-1"))

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not S3_BUCKET_NAME:
    st.error("AWS credentials and S3 bucket name must be set in secrets or environment variables.")
    st.stop()

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=S3_REGION
)


# --- S3 refresh and cleanup logic (runs on every reload/autorefresh) ---
now = datetime.utcnow()
if "run_id" not in st.session_state:
    st.session_state["run_id"] = str(now)
if "last_s3_refresh_time" not in st.session_state:
    st.session_state["last_s3_refresh_time"] = now

# Refresh S3 and delete expired files every 5 minutes
if (now - st.session_state["last_s3_refresh_time"]).total_seconds() > 300:
    st.session_state["run_id"] = str(now)
    delete_expired_files()
    st.session_state["last_s3_refresh_time"] = now

@st.cache_data(ttl=300)
def get_cached_s3_listing(run_id):
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
    return response.get("Contents", [])

def delete_expired_files():
    """Delete files older than 15 minutes from S3 bucket."""
    response = {"Contents": get_cached_s3_listing(st.session_state["run_id"])}
    if "Contents" not in response:
        return

    now = datetime.utcnow()
    for obj in response["Contents"]:
        last_modified = obj["LastModified"].replace(tzinfo=None)
        age_seconds = (now - last_modified).total_seconds()
        if age_seconds > 900:  # Older than 15 minutes
            try:
                if "deletion_log" not in st.session_state:
                    st.session_state["deletion_log"] = []

                st.session_state["deletion_log"].append(
                    f"Deleted {obj['Key']} at {now.strftime('%Y-%m-%d %H:%M:%S')} UTC"
                )
                s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=obj["Key"])
            except Exception as e:
                st.warning(f"Failed to delete {obj['Key']}: {e}")

def list_active_filehub_objects_ui():
    st.header("📂 FileHub (S3) Admin Console")
    st.markdown("<br><br>", unsafe_allow_html=True)

    response = {"Contents": get_cached_s3_listing(st.session_state["run_id"])}
    if "Contents" not in response:
        st.info("No files currently stored.")
        return

    now = datetime.utcnow()
    active_files = [
        obj for obj in response["Contents"]
        if (now - obj["LastModified"].replace(tzinfo=None)).total_seconds() < 900
    ]

    total_bytes = sum(obj["Size"] for obj in active_files)
    total_mb = total_bytes / (1024 * 1024)

    all_objects = response["Contents"]
    total_file_count = len(all_objects)
    total_file_size = sum(obj["Size"] for obj in all_objects) / (1024 * 1024)

    col1, col2 = st.columns(2)
    col1.markdown(f"**Total Active File Size:** `{total_mb:.2f} MB`")
    col2.markdown(f"**Active File Count:** `{len(active_files)}`")
    col3, col4 = st.columns(2)
    col3.markdown(f"**Total File Size:** `{total_file_size:.2f} MB`")
    col4.markdown(f"**Total File Count:** `{total_file_count}`")

    st.markdown("### Active Tokens")

    for obj in sorted(active_files, key=lambda x: 900 - (datetime.utcnow() - x["LastModified"].replace(tzinfo=None)).total_seconds()):
        key = obj["Key"]
        last_modified = obj["LastModified"].replace(tzinfo=None)
        age = (now - last_modified).total_seconds()
        time_remaining = 900 - int(age)  # can be negative
        token_masked_key = key
        if "/" in key and "__" in key:
            prefix, rest = key.split("/", 1)
            token_and_filename = rest.split("__", 1)
            if len(token_and_filename) == 2:
                token, filename = token_and_filename
                if len(token) == 8:
                    masked_token = "XXXXXX" + token[-2:]
                    token_masked_key = f"{prefix}/{masked_token}__{filename}"
        file_size_mb = obj["Size"] / (1024 * 1024)
        col1, col2, col3 = st.columns([8, 1.5, 2.5])
        col1.markdown(f"**{token_masked_key}**")
        col2.markdown(f"`{file_size_mb:.2f} MB`")
        if time_remaining < 0:
            col3.markdown(
                f"<span style='font-family:monospace; color:red'>EXPIRED {-time_remaining} sec ago</span>",
                unsafe_allow_html=True
            )
        else:
            color = "green"
            if time_remaining < 180:
                color = "red"
            elif time_remaining < 450:
                color = "orange"
            col3.markdown(
                f"<span style='font-family:monospace'>Expires in <span style='color:{color}'>{time_remaining}</span> sec</span>",
                unsafe_allow_html=True
            )

    st.markdown("---")
    st.subheader("All Files in S3 (including expired)")
    all_objects_sorted = sorted(all_objects, key=lambda x: 86400 - (now - x["LastModified"].replace(tzinfo=None)).total_seconds())
    for obj in all_objects_sorted:
        key = obj["Key"]
        last_modified = obj["LastModified"].replace(tzinfo=None)
        age = (now - last_modified).total_seconds()
        time_remaining = 900 - int(age)  # 15 min TTL

        if time_remaining < 0:
            time_str = f"Expired {-int(time_remaining)} sec ago (may be deleted)"
        else:
            time_str = f"{int(time_remaining)} seconds"

        token_masked_key = key
        if "/" in key and "__" in key:
            prefix, rest = key.split("/", 1)
            token_and_filename = rest.split("__", 1)
            if len(token_and_filename) == 2:
                token, filename = token_and_filename
                if len(token) == 8:
                    masked_token = "XXXXXX" + token[-2:]
                    token_masked_key = f"{prefix}/{masked_token}__{filename}"
        file_size_mb = obj["Size"] / (1024 * 1024)
        col1, col2, col3 = st.columns([8, 1.5, 2.5])
        col1.markdown(f"**{token_masked_key}**")
        col2.markdown(f"`{file_size_mb:.2f} MB`")
        col3.markdown(
            f"<span style='font-family:monospace'>Deletes in {time_str}</span>",
            unsafe_allow_html=True
        )
    st.markdown("---")
    st.subheader("🗑️ Recent Deletions (Logged This Session)")

    if "deletion_log" in st.session_state:
        for entry in st.session_state["deletion_log"]:
            st.markdown(f"- `{entry}`")
    else:
        st.info("No deletions recorded this session.")

if __name__ == "__main__":
    st.set_page_config(page_title="Admin Console – FileHub Backend Transfers")
    st_autorefresh(interval=1000, key="auto-refresh")
    # delete_expired_files()
    list_active_filehub_objects_ui()
