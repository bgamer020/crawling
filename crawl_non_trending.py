from googleapiclient.discovery import build
import pandas as pd
import datetime, os

# 1️⃣ API key: đặt trong biến môi trường
API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=API_KEY)

# 2️⃣ Đọc danh sách videoId đã có
file_path = "youtube_non_trending.csv"
df_old = pd.read_csv(file_path, encoding="utf-8-sig")

# Lấy toàn bộ videoId (loại bỏ trùng)
video_ids = df_old["videoId"].drop_duplicates().tolist()
print(f"🔎 Đang theo dõi {len(video_ids)} video")

# 3️⃣ Gọi API lấy thống kê hiện tại
stats_map = {}
cat_id_map = {}
for i in range(0, len(video_ids), 50):   # API giới hạn 50 id/lần
    resp = youtube.videos().list(
        part="statistics,snippet",
        id=",".join(video_ids[i:i+50])
    ).execute()
    for item in resp["items"]:
        vid = item["id"]
        stats = item["statistics"]
        cat_id_map[vid] = item["snippet"].get("categoryId", "")
        stats_map[vid] = {
            "title": item["snippet"]["title"],
            "channelId": item["snippet"]["channelId"],
            "channelTitle": item["snippet"]["channelTitle"],
            "publishDate": item["snippet"]["publishedAt"],
            "viewCount": stats.get("viewCount", 0),
            "likeCount": stats.get("likeCount", 0),
            "commentCount": stats.get("commentCount", 0)
        }

# 4️⃣ Bảng ánh xạ categoryId → tên
category_map = {
    "1":  "Film & Animation", "2":  "Autos & Vehicles",
    "10": "Music",            "15": "Pets & Animals",
    "17": "Sports",           "19": "Travel & Events",
    "20": "Gaming",           "22": "People & Blogs",
    "23": "Comedy",           "24": "Entertainment",
    "25": "News & Politics",  "26": "Howto & Style",
    "27": "Education",        "28": "Science & Technology",
    "29": "Nonprofits & Activism"
}

# 5️⃣ Chuẩn bị dữ liệu mới để append
now = datetime.datetime.now()
collect_date = now.strftime("%Y-%m-%d")
collect_time = now.strftime("%H:%M:%S")

rows = []
for vid in video_ids:
    s = stats_map.get(vid)
    if not s:      # video có thể bị xóa hoặc private
        continue
    rows.append({
        "videoId": vid,
        "title": s["title"],
        "channelId": s["channelId"],
        "channelTitle": s["channelTitle"],
        "category": category_map.get(str(cat_id_map.get(vid,"")), ""),
        "publishDate": s["publishDate"].split("T")[0],
        "publishTime": s["publishDate"].split("T")[1].replace("Z",""),
        "collectDate": collect_date,
        "collectTime": collect_time,
        "region": "VN",
        "viewCount": s["viewCount"],
        "likeCount": s["likeCount"],
        "commentCount": s["commentCount"]
    })

df_new = pd.DataFrame(rows, columns=[
    "videoId","title","channelId","channelTitle","category",
    "publishDate","publishTime","collectDate","collectTime","region",
    "viewCount","likeCount","commentCount"
])

# 6️⃣ Append dữ liệu mới vào file cũ
df_new.to_csv(file_path, mode="a", header=False, index=False, encoding="utf-8-sig")

print(f"✅ Đã append {len(df_new)} bản ghi mới vào {file_path}")
