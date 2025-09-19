import os
import pandas as pd
from datetime import datetime
import time
from googleapiclient.discovery import build

API_KEY = 'AIzaSyAbFvg3b2QKli5LywMHtuqyQuex4HHuq3s'  # Thay API key của bạn
REGION = 'VN'
COLLECT_DATE = datetime.now().strftime('%Y-%m-%d')
FILE_NAME = 'youtube_non_trending.csv'

youtube = build('youtube', 'v3', developerKey=API_KEY)

# Query cố định và số lượng cần mỗi loại
queries_targets = {
    'âm nhạc': ('music', 20),
    'game': ('game', 10),
    'people & blogs': ('people', 5),
    'entertainment': ('entertainment', 15)
}

def get_trending_video_ids(region_code='VN', max_results=100):
    trending_ids = set()
    request = youtube.videos().list(
        part='id',
        chart='mostPopular',
        regionCode=region_code,
        maxResults=max_results
    )
    response = request.execute()
    for item in response['items']:
        trending_ids.add(item['id'])
    return trending_ids

def search_non_trending_videos(query, max_results, trending_ids):
    videos = []
    next_page_token = None
    while len(videos) < max_results:
        request = youtube.search().list(
            part='id,snippet',
            q=query,
            type='video',
            regionCode=REGION,
            maxResults=50,
            order='date',
            pageToken=next_page_token
        )
        response = request.execute()

        video_batch = []
        id_map = {}

        for item in response['items']:
            video_id = item['id']['videoId']
            if video_id in trending_ids:
                continue
            if video_id in id_map:
                continue  # tránh trùng lặp
            id_map[video_id] = {
                'title': item['snippet']['title'],
                'channelTitle': item['snippet']['channelTitle'],
                'publishDate': item['snippet']['publishedAt']
            }
            video_batch.append(video_id)

        if not video_batch:
            break

        stats_request = youtube.videos().list(
            part='statistics',
            id=','.join(video_batch)
        )
        stats_response = stats_request.execute()

        for item in stats_response['items']:
            vid = item['id']
            stats = item.get('statistics', {})
            views = int(stats.get('viewCount', 0))
            likes = int(stats.get('likeCount', 0))
            comments = int(stats.get('commentCount', 0))
            interaction_rate = (likes + comments) / views if views > 0 else 0

            video_data = {
                'videoId': vid,
                'title': id_map[vid]['title'],
                'channelTitle': id_map[vid]['channelTitle'],
                'category': query,
                'publishDate': id_map[vid]['publishDate'],
                'collectDate': COLLECT_DATE,
                'region': REGION,
                'rank': '',
                'viewCount': views,
                'likeCount': likes,
                'commentCount': comments,
                'interactionRate': round(interaction_rate, 6),
                'growthRate': 0  # Lần đầu tiên crawl để 0
            }
            videos.append(video_data)
            if len(videos) >= max_results:
                break

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

        time.sleep(0.5)
    return videos

def update_existing_videos_stats(old_df):
    video_ids = old_df['videoId'].unique().tolist()
    videos_data = []
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i+50]
        request = youtube.videos().list(
            part='statistics,snippet',
            id=','.join(batch_ids)
        )
        response = request.execute()
        for item in response.get('items', []):
            vid = item['id']
            stats = item.get('statistics', {})
            snippet = item.get('snippet', {})

            viewCount = int(stats.get('viewCount', 0))
            likeCount = int(stats.get('likeCount', 0))
            commentCount = int(stats.get('commentCount', 0))
            interaction_rate = (likeCount + commentCount) / viewCount if viewCount > 0 else 0

            videos_data.append({
                'videoId': vid,
                'title': snippet.get('title', ''),
                'channelTitle': snippet.get('channelTitle', ''),
                'category': '',  # giữ trống, sẽ lấy từ old_df
                'publishDate': snippet.get('publishedAt', ''),
                'collectDate': COLLECT_DATE,
                'region': REGION,
                'rank': '',
                'viewCount': viewCount,
                'likeCount': likeCount,
                'commentCount': commentCount,
                'interactionRate': round(interaction_rate, 6),
                'growthRate': 0  # sẽ tính sau
            })
        time.sleep(0.5)
    return pd.DataFrame(videos_data)

def calculate_growth_rate(old_df, new_df):
    merged = pd.merge(new_df, old_df, on='videoId', suffixes=('_new', '_old'))
    # Tính growth rate dựa trên viewCount, tránh chia cho 0
    def safe_growth(new, old):
        if old == 0:
            return 0
        return (new - old) / old

    merged['growthRate'] = merged.apply(
        lambda row: safe_growth(row['viewCount_new'], row['viewCount_old']),
        axis=1
    )
    # Cập nhật các cột giữ nguyên từ old_df nếu muốn
    merged['category'] = merged['category_old']
    merged['region'] = merged['region_old']
    merged['publishDate'] = merged['publishDate_old']
    # Lấy các cột cần thiết
    result = merged[['videoId', 'title_new', 'channelTitle_new', 'category', 'publishDate', 'collectDate_new', 'region', 'rank_new',
                     'viewCount_new', 'likeCount_new', 'commentCount_new', 'interactionRate_new', 'growthRate']]
    # Đổi tên cột cho dễ dùng
    result.rename(columns={
        'title_new': 'title',
        'channelTitle_new': 'channelTitle',
        'collectDate_new': 'collectDate',
        'rank_new': 'rank',
        'viewCount_new': 'viewCount',
        'likeCount_new': 'likeCount',
        'commentCount_new': 'commentCount',
        'interactionRate_new': 'interactionRate'
    }, inplace=True)
    # Nếu growthRate NaN, đổi về 0
    result['growthRate'] = result['growthRate'].fillna(0)
    return result

def main():
    if not os.path.exists(FILE_NAME):
        # Lần đầu crawl, tạo file mới
        print("Chưa có file dữ liệu cũ, tiến hành crawl lần đầu...")
        trending_ids = get_trending_video_ids(region_code=REGION)
        all_videos = []
        for category, (query, count) in queries_targets.items():
            print(f"Lấy {count} video cho danh mục '{category}' với query '{query}'...")
            vids = search_non_trending_videos(query, count, trending_ids)
            all_videos.extend(vids)
        df = pd.DataFrame(all_videos)
        df.to_csv(FILE_NAME, index=False, encoding='utf-8-sig')
        print(f"Đã lưu dữ liệu lần đầu vào {FILE_NAME}")
    else:
        # Lần tiếp theo crawl, update dữ liệu mới và tính growth rate
        print("File dữ liệu cũ tồn tại, thu thập dữ liệu cập nhật...")
        old_df = pd.read_csv(FILE_NAME)
        new_df = update_existing_videos_stats(old_df)
        # Gán category, region, publishDate từ old_df cho new_df theo videoId
        for col in ['category', 'region', 'publishDate']:
            new_df[col] = new_df['videoId'].map(old_df.set_index('videoId')[col])
        # Tính growth rate
        result_df = calculate_growth_rate(old_df, new_df)

        # Append dữ liệu mới vào file cũ để lưu lịch sử
        combined = pd.concat([old_df, result_df], ignore_index=True)
        combined.to_csv(FILE_NAME, index=False, encoding='utf-8-sig')
        print(f"Đã cập nhật dữ liệu và append vào {FILE_NAME}. Tổng dòng hiện tại: {len(combined)}")

if __name__ == '__main__':
    main()
