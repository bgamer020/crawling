import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
import os

API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=API_KEY)

# Lấy danh sách categoryId -> categoryName
def get_video_categories(region="VN"):
    request = youtube.videoCategories().list(part="snippet", regionCode=region)
    response = request.execute()
    return {item["id"]: item["snippet"]["title"] for item in response["items"]}

# Lấy danh sách video trending
def get_trending_videos(total_results=100, region="VN"):
    categories = get_video_categories(region)
    videos, fetched = [], 0
    max_per_request = 50  # API limit
    today = datetime.today().strftime("%Y-%m-%d")

    while fetched < total_results:
        to_fetch = min(max_per_request, total_results - fetched)
        request = youtube.videos().list(
            part="snippet,statistics",
            chart="mostPopular",
            regionCode=region,
            maxResults=to_fetch
        )
        response = request.execute()

        for idx, item in enumerate(response.get("items", []), start=fetched+1):
            cat_id = item["snippet"].get("categoryId", "N/A")
            stats = item.get("statistics", {})
            publish_date = item["snippet"]["publishedAt"][:10]  # YYYY-MM-DD

            videos.append({
                "videoId": item["id"],
                "title": item["snippet"]["title"],
                "channelTitle": item["snippet"]["channelTitle"],
                "category": categories.get(cat_id, "Unknown"),
                "publishDate": publish_date,       # ngày đăng video
                "collectDate": today,              # ngày thu thập
                "region": region,                  # khu vực
                "rank": idx,                       # tạm rank theo lượt lấy
                "viewCount": stats.get("viewCount", 0),
                "likeCount": stats.get("likeCount", 0),
                "commentCount": stats.get("commentCount", 0),
            })
        fetched += len(response.get("items", []))
        if len(response.get("items", [])) < to_fetch:
            break
    return videos

# 📌 Crawl cho nhiều region
regions = ["VN", "US", "KR"]
all_videos = []

for region in regions:
    print(f"📥 Đang crawl {region} ...")
    videos = get_trending_videos(50, region)
    all_videos.extend(videos)

df_new = pd.DataFrame(all_videos)

# 📌 File CSV chung
# 📌 File CSV chung
file_name = "youtube_trending.csv"

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name, encoding="utf-8-sig")

    # Bỏ các bản ghi đã tồn tại (videoId + collectDate + region)
    merge_keys = ["videoId", "collectDate", "region"]
    df_new = df_new[~df_new.set_index(merge_keys).index.isin(df_old.set_index(merge_keys).index)]

    # Gộp dữ liệu mới + cũ
    df_final = pd.concat([df_old, df_new], ignore_index=True)
else:
    df_final = df_new

# Reset rank cho từng collectDate + region
df_final["rank"] = (
    df_final.groupby(["collectDate", "region"])
    .cumcount() + 1
)

# Ghi file
df_final.to_csv(file_name, index=False, encoding="utf-8-sig")

print(f"✅ Đã thêm {len(df_new)} video trending ({', '.join(regions)}), file hiện có {len(df_final)} bản ghi.")



