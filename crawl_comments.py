import os
import pandas as pd
from googleapiclient.discovery import build

# ==== Cấu hình ====
API_KEY = "AIzaSyB1AJ862OVBHBDIZSASiEgCHzHo1_ramiE"
INPUT_FILE = "youtube_trending.csv"    # file video trending (có cột region)
OUTPUT_FILE = "comments.csv"           # nơi lưu comment
CRAWLED_VIDEOS_FILE = "crawled_videos.csv"  # lưu danh sách video đã crawl

# ==== Đọc danh sách video, chỉ lấy region = VN ====
df = pd.read_csv(INPUT_FILE)

if "region" in df.columns:
    df = df[df["region"] == "VN"]

if "videoId" in df.columns:
    video_ids = df["videoId"].dropna().astype(str).unique().tolist()
elif "url" in df.columns:
    df["videoId"] = df["url"].str.extract(r"v=([\w-]{11})")
    video_ids = df["videoId"].dropna().astype(str).unique().tolist()
else:
    raise ValueError("Không tìm thấy cột videoId hoặc url trong file csv")

print(f"📺 Tổng số video Việt Nam trong dữ liệu: {len(video_ids)}")

# ==== Load danh sách video đã crawl ====
if os.path.exists(CRAWLED_VIDEOS_FILE):
    crawled_df = pd.read_csv(CRAWLED_VIDEOS_FILE)
    crawled_videos = set(crawled_df["videoId"].astype(str))
else:
    crawled_videos = set()

print(f"✅ Đã crawl {len(crawled_videos)} video trước đó")

# ==== Build YouTube API ====
youtube = build("youtube", "v3", developerKey=API_KEY)

# ==== Lấy mapping categoryId -> categoryName ====
def get_category_mapping(region="VN"):
    cats = {}
    req = youtube.videoCategories().list(part="snippet", regionCode=region)
    res = req.execute()
    for item in res["items"]:
        cats[item["id"]] = item["snippet"]["title"]
    return cats

category_map = get_category_mapping()

# ==== Hàm lấy title + category của nhiều video (batch 50 id) ====
def get_video_infos(video_ids):
    infos = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            req = youtube.videos().list(part="snippet", id=",".join(batch))
            res = req.execute()
            for item in res.get("items", []):
                vid = item["id"]
                snippet = item["snippet"]
                infos[vid] = {
                    "title": snippet.get("title"),
                    "category": category_map.get(snippet.get("categoryId"), "Unknown")
                }
        except Exception as e:
            print(f"❌ Lỗi khi lấy info batch {batch}: {e}")
    return infos

# ==== Hàm crawl comment ====
def get_comments(video_id, max_comments=100):
    comments = []
    try:
        req = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            textFormat="plainText"
        )
        while req and len(comments) < max_comments:
            res = req.execute()
            for item in res.get("items", []):
                top = item["snippet"]["topLevelComment"]["snippet"]
                comments.append({
                    "comment": top.get("textDisplay"),
                    "commentLikes": top.get("likeCount", 0)
                })
                if len(comments) >= max_comments:
                    break
            req = youtube.commentThreads().list_next(req, res)
    except Exception as e:
        if "quotaExceeded" in str(e):
            print("❌ Hết quota, dừng crawl!")
            return None
        elif "commentsDisabled" in str(e):
            print(f"🚫 Video {video_id} tắt comment.")
            return []
        else:
            print(f"⚠️ Lỗi khi crawl comment video {video_id}: {e}")
            return []
    return comments

# ==== Crawl toàn bộ video ====
all_data = []
crawled_list = []

# Lấy thông tin tất cả video trước (title, category)
video_infos = get_video_infos(video_ids)

for vid in video_ids:
    if vid in crawled_videos:
        print(f"➡️ Bỏ qua {vid} (đã crawl)")
        continue

    if vid not in video_infos:
        print(f"⚠️ Không tìm thấy thông tin video {vid}")
        continue

    print(f"🚀 Crawl video {vid} ...")
    title = video_infos[vid]["title"]
    category = video_infos[vid]["category"]

    comments = get_comments(vid, max_comments=100)
    if comments is None:  # quota exceeded
        break
    if len(comments) == 0:
        print(f"⚠️ Video {vid} không có comment")
        continue

    for c in comments:
        all_data.append({
            "videoId": vid,
            "title": title,
            "category": category,
            "comment": c["comment"],
            "commentLikes": c["commentLikes"]
        })

    crawled_list.append(vid)

    # Lưu tạm comment (append thêm)
    if all_data:
        pd.DataFrame(all_data).to_csv(
            OUTPUT_FILE,
            mode="a",
            header=not os.path.exists(OUTPUT_FILE),
            index=False,
            encoding="utf-8-sig"
        )
        all_data = []  # reset để không bị ghi trùng

    # Lưu tạm danh sách video đã crawl
    pd.DataFrame({"videoId": list(crawled_videos | set(crawled_list))}).to_csv(
        CRAWLED_VIDEOS_FILE, index=False, encoding="utf-8-sig"
    )

print("🎉 Crawl xong! Kết quả đã lưu trong comments.csv")
