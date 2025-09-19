import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
import os

# 🔑 API KEY (nên để trong biến môi trường)
API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=API_KEY)

# 📌 Lấy video trending (để loại bỏ khỏi non-trending)
def get_trending_video_ids(region="VN", max_results=50):
    request = youtube.videos().list(
        part="id",
        chart="mostPopular",
        regionCode=region,
        maxResults=max_results
    )
    response = request.execute()
    return {item["id"] for item in response.get("items", [])}

# 📌 Lấy video theo category (không phải trending)
def get_videos_by_category(category_id, region="VN", max_results=50):
    request = youtube.search().list(
        part="snippet",
        type="video",
        regionCode=region,
        videoCategoryId=category_id,
        maxResults=max_results,
        order="viewCount"
    )
    response = request.execute()
    videos = []
    for item in response.get("items", []):
        video_id = item["id"]["videoId"]
        snippet = item["snippet"]
        videos.append({
            "videoId": video_id,
            "title": snippet["title"],
            "channelTitle": snippet["channelTitle"],
            "categoryId": category_id,
            "publishDate": snippet["publishedAt"][:10]  # YYYY-MM-DD
        })
    return videos

# 📌 Lấy statistics cho video
def get_video_statistics(video_ids):
    stats_list = []
    for i in range(0, len(video_ids), 50):  # mỗi lần gọi tối đa 50 id
        request = youtube.videos().list(
            part="statistics",
            id=",".join(video_ids[i:i+50])
        )
        response = request.execute()
        for item in response.get("items", []):
            stats = item.get("statistics", {})
            stats_list.append({
                "videoId": item["id"],
                "viewCount": int(stats.get("viewCount", 0)),
                "likeCount": int(stats.get("likeCount", 0)),
                "commentCount": int(stats.get("commentCount", 0)),
            })
    return pd.DataFrame(stats_list)

def main():
    categories = {
        "Music": "10",
        "Gaming": "20",
        "Entertainment": "24",
        "People & Blogs": "22"
    }

    region = "VN"
    today = datetime.today().strftime("%Y-%m-%d")

    # 📌 Lấy danh sách trending để loại bỏ
    trending_ids = get_trending_video_ids(region, 50)

    all_videos = []
    for name, cat_id in categories.items():
        print(f"📥 Crawl {name} ...")
        videos = get_videos_by_category(cat_id, region, 50)

        # Bỏ video trending
        videos = [v for v in videos if v["videoId"] not in trending_ids]

        # Thêm metadata
        for v in videos:
            v["collectDate"] = today
            v["region"] = region
            v["categoryName"] = name

        all_videos.extend(videos)

    if not all_videos:
        print("⚠️ Không lấy được video nào.")
        return

    # 📌 DataFrame
    df_new = pd.DataFrame(all_videos)

    # Thêm statistics
    stats_df = get_video_statistics(df_new["videoId"].tolist())
    df_new = df_new.merge(stats_df, on="videoId", how="left")

    # 📌 File lưu
    file_name = "youtube_non_trending.csv"

    if os.path.exists(file_name):
        print("📂 Có dữ liệu cũ, gộp thêm dữ liệu mới...")
        df_old = pd.read_csv(file_name, encoding="utf-8-sig")

        # Gộp dữ liệu
        df_final = pd.concat([df_old, df_new], ignore_index=True)

        # Loại bỏ trùng videoId (ưu tiên bản mới nhất)
        df_final = df_final.drop_duplicates(subset=["videoId"], keep="last")
    else:
        df_final = df_new

    # Xuất CSV
    df_final.to_csv(file_name, index=False, encoding="utf-8-sig")
    print(f"✅ Đã lưu {len(df_new)} video non-trending, file hiện có {len(df_final)} bản ghi.")

if __name__ == "__main__":
    main()

