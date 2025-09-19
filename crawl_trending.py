name: Run Crawlers on Schedule

on:
  schedule:
    - cron: '0 17 * * *'  # 0h VN
    - cron: '0 23 * * *'  # 6h VN
    - cron: '0 5 * * *'   # 12h VN
    - cron: '0 11 * * *'  # 18h VN
    - cron: '0 15 * * *'  # 22h VN
  workflow_dispatch:      # cho phép chạy thủ công

permissions:
  contents: write   # ✅ cho phép commit file CSV

jobs:
  run-crawlers:
    runs-on: ubuntu-latest
    env:
      YOUTUBE_API_KEY: ${{ secrets.YOUTUBE_API_KEY }}

    steps:
    - name: Checkout code
      uses: actions/checkout@v3
      with:
        fetch-depth: 0   # ✅ để pull đầy đủ lịch sử (cần cho append)

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.10'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install pandas google-api-python-client

    - name: Run non-trending crawler
      run: python crawl_non_trending.py

    - name: Run trending crawler
      run: python crawl_trending.py

    - name: Commit & Push CSV results
      uses: EndBug/add-and-commit@v9
      with:
        author_name: github-actions
        author_email: actions@github.com
        message: "Update CSV data [skip ci]"
        add: |
          youtube_trending.csv
          youtube_non_trending.csv
