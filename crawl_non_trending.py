import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
import os

API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=API_KEY)
crawl_date = datetime.now().strftime("%Y-%m-%d")  # Ngày crawl
crawl_time = datetime.now().strftime("%H:%M:%S")  # Giờ crawl       
region = "VN"

def get_trending_video_ids(region="VN", max_results=50):
    req = youtube.videos().list(
        part="id", chart="mostPopular",
        regionCode=region, maxResults=max_results
    )
    res = req.execute()
    return {item["id"] for item in res.get("items", [])}

def get_videos_by_keyword(keyword, max_results=16):
    req = youtube.search().list(
        part="snippet", q=keyword, type="video",
        regionCode="VN", maxResults=max_results,
        order="viewCount",
        relevanceLanguage="vi"
    )
    res = req.execute()
    videos = []
    for item in res.get("items", []):
        snippet = item["snippet"]
        videos.append({
            "videoId": item["id"]["videoId"],
            "title": snippet["title"],
            "channelTitle": snippet["channelTitle"],
            "keyword": keyword,
            "publishDate": snippet["publishedAt"][:10],
            "collectDate": crawl_date,      # 🆕 Ngày crawl
            "collectTime": crawl_time,      # 🆕 Giờ crawl
            "region": region
        })

    return videos

def get_video_statistics(video_ids):
    stats = []
    for i in range(0, len(video_ids), 50):
        req = youtube.videos().list(
            part="statistics", id=",".join(video_ids[i:i+50])
        )
        res = req.execute()
        for item in res.get("items", []):
            s = item.get("statistics", {})
            stats.append({
                "videoId": item["id"],
                "viewCount": int(s.get("viewCount", 0)),
                "likeCount": int(s.get("likeCount", 0)),
                "commentCount": int(s.get("commentCount", 0)),
                      # ✅ dùng collectDate luôn
            })
    return pd.DataFrame(stats)


def main():
    file_name = "youtube_non_trending.csv"
    keywords = ["nhạc", "game", "vlog"]
    max_per_keyword = 50  # ~50 video tổng

    if not os.path.exists(file_name):
        # 🔥 Lần đầu: crawl video + lưu thống kê ban đầu
        print("📥 Lần đầu crawl dữ liệu ...")
        trending_ids = get_trending_video_ids(region, 50)
        all_videos = []
        for kw in keywords:
            print(f"➡️ Crawl từ khóa: {kw}")
            vids = get_videos_by_keyword(kw, max_per_keyword)
            vids = [v for v in vids if v["videoId"] not in trending_ids]
            all_videos.extend(vids)

        base_df = pd.DataFrame(all_videos)
        stats_df = get_video_statistics(base_df["videoId"].tolist())
        df = base_df.merge(stats_df, on="videoId", how="left")
        df.to_csv(file_name, index=False, encoding="utf-8-sig")
        print(f"✅ Đã lưu {len(df)} video non-trending (lần đầu).")

    else:
        # 🔄 Lần sau: append số liệu mới
        print("🔄 Đang cập nhật và append lịch sử ...")
        old_df = pd.read_csv(file_name, encoding="utf-8-sig")

        # Lấy danh sách video cũ để update
        unique_videos = old_df.drop_duplicates("videoId")
        stats_df = get_video_statistics(unique_videos["videoId"].tolist())

        # Merge thông tin cố định (title, channel...) vào stats mới
        merged = stats_df.merge(
            unique_videos[["videoId","title","channelTitle","keyword","publishDate","collectDate","region"]],
            on="videoId",
            how="left"
        )

        # Ghi nối tiếp (append) vào file CSV
        merged.to_csv(file_name, mode="a", header=False, index=False, encoding="utf-8-sig")
        print(f"✅ Đã append {len(merged)} dòng thống kê mới vào file.")

if __name__ == "__main__":
    main()


