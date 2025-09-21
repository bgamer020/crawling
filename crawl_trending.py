from googleapiclient.discovery import build
import pandas as pd
import datetime, os, isodate

API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=API_KEY)

def get_trending_videos(region="VN", max_results=50):
    videos = []
    next_page_token = None
    category_map = {
        "1": "Film & Animation","2": "Autos & Vehicles","10": "Music",
        "15": "Pets & Animals","17": "Sports","19": "Travel & Events",
        "20": "Gaming","22": "People & Blogs","23": "Comedy",
        "24": "Entertainment","25": "News & Politics","26": "Howto & Style",
        "27": "Education","28": "Science & Technology","29": "Nonprofits & Activism"
    }

    while True:
        req = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            chart="mostPopular",
            regionCode=region,
            maxResults=50,
            pageToken=next_page_token
        )
        resp = req.execute()

        for idx, item in enumerate(resp["items"], start=1):
            snippet = item["snippet"]
            stats   = item["statistics"]
            details = item["contentDetails"]

            duration_iso = details.get("duration","")
            td = isodate.parse_duration(duration_iso)
            duration_seconds = int(td.total_seconds())

            cat_id = snippet.get("categoryId","")
            category_name = category_map.get(str(cat_id), "")

            publish_date_full = snippet["publishedAt"]
            publish_date = publish_date_full.split("T")[0]
            publish_time = publish_date_full.split("T")[1].replace("Z","")

            now = datetime.datetime.now()
            collect_date = now.strftime("%Y-%m-%d")
            collect_time = now.strftime("%H:%M:%S")

            videos.append({
                "videoId": item["id"],
                "title": snippet["title"],
                "channelId": snippet["channelId"],
                "channelTitle": snippet["channelTitle"],
                "category": category_name,
                "publishDate": publish_date,
                "publishTime": publish_time,
                "collectDate": collect_date,
                "collectTime": collect_time,
                "region": region,
                "rank": idx,
                "viewCount": stats.get("viewCount", 0),
                "likeCount": stats.get("likeCount", 0),
                "commentCount": stats.get("commentCount", 0),
                "duration": duration_seconds
            })

        next_page_token = resp.get("nextPageToken")
        if not next_page_token or len(videos) >= max_results:
            break

    return pd.DataFrame(videos)

# ---------- Crawl 50 video trending VN, KR, US ----------
regions = ["VN","KR","US"]
dfs = []
for r in regions:
    print(f"⏳ Đang crawl 50 video trending {r} ...")
    dfs.append(get_trending_videos(region=r, max_results=50))

df_all = pd.concat(dfs, ignore_index=True)

# ---------- Append vào CSV ----------
out_file = "youtube_trending.csv"

if os.path.exists(out_file):
    # File đã có: ghi nối tiếp, không ghi header
    df_all.to_csv(out_file, mode="a", header=False, index=False, encoding="utf-8-sig")
else:
    # File chưa có: ghi mới kèm header
    df_all.to_csv(out_file, mode="w", header=True, index=False, encoding="utf-8-sig")


