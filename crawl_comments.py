from googleapiclient.discovery import build
import pandas as pd
from tqdm import tqdm
import time

# 🔑 Thay bằng API key của bạn
API_KEY = "AIzaSyB1AJ862OVBHBDIZSASiEgCHzHo1_ramiE"

# ⚙️ Khởi tạo service YouTube
youtube = build('youtube', 'v3', developerKey=API_KEY)

# 📌 Lấy danh sách 50 video trending VN
def get_trending_videos(max_results=50, region='VN'):
    request = youtube.videos().list(
        part="snippet",
        chart="mostPopular",
        regionCode=region,
        maxResults=max_results
    )
    response = request.execute()
    videos = []

    for item in response['items']:
        videos.append({
            "videoId": item["id"],
            "title": item["snippet"]["title"],
            "category": item["snippet"]["categoryId"]
        })
    return videos

# 📌 Lấy toàn bộ comments của 1 video
def get_all_comments(video_id, title, category):
    comments = []
    next_page_token = None

    while True:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token,
                textFormat="plainText"
            )
            response = request.execute()

            for item in response["items"]:
                top_comment = item["snippet"]["topLevelComment"]["snippet"]
                comments.append({
                    "videoId": video_id,
                    "title": title,
                    "category": category,
                    "comment": top_comment["textDisplay"],
                    "commentLikes": top_comment["likeCount"]
                })

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        except Exception as e:
            print(f"⚠️ Video {video_id} bị lỗi hoặc tắt bình luận: {e}")
            break

        time.sleep(0.2)  # chống quota bị khóa

    return comments

# 🚀 Chạy crawl
print("📥 Đang lấy danh sách 50 video trending tại Việt Nam...")
videos = get_trending_videos()

all_comments = []
for video in tqdm(videos, desc="📄 Crawling comments"):
    all_comments.extend(get_all_comments(video["videoId"], video["title"], video["category"]))

# 💾 Lưu vào file comments.csv
df = pd.DataFrame(all_comments)
df.to_csv("comments.csv", index=False, encoding="utf-8-sig")

print(f"\n✅ Crawl xong! Đã lưu {len(df)} comments vào file comments.csv")
