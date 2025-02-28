import os, re, requests, heapq, sqlite3, threading
from flask import Flask, request, jsonify, render_template_string, send_from_directory
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from datetime import datetime
from gunicorn.app.base import BaseApplication

app = Flask(__name__, static_url_path='/images', static_folder='downloaded_images')

search_index = {}
visited = set()
max_depth = 100
last_index_update_time = None
max_depth_reached = 0
crawler_thread = None
crawl_in_progress = False
current_depth = 0
current_url = ""

def init_db():
    conn = sqlite3.connect('crawler.db')
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS pages (url TEXT PRIMARY KEY, content TEXT, depth INTEGER, last_visited TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS images (url TEXT, image_url TEXT, filename TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS videos (url TEXT, video_url TEXT, filename TEXT)')
    conn.commit()
    conn.close()

def init_visited_from_db():
    global visited
    conn = sqlite3.connect('crawler.db')
    c = conn.cursor()
    rows = c.execute('SELECT url FROM pages').fetchall()
    for row in rows:
        visited.add(row[0])
    conn.close()

def save_to_db(url, text, depth):
    conn = sqlite3.connect('crawler.db')
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO pages (url, content, depth, last_visited) VALUES (?,?,?,?)',
              (url, text, depth, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()
    conn.close()

def save_image_to_db(page_url, img_url, filename):
    conn = sqlite3.connect('crawler.db')
    c = conn.cursor()
    c.execute('INSERT INTO images (url, image_url, filename) VALUES (?,?,?)',
              (page_url, img_url, filename))
    conn.commit()
    conn.close()

def save_video_to_db(page_url, vid_url, filename):
    conn = sqlite3.connect('crawler.db')
    c = conn.cursor()
    c.execute('INSERT INTO videos (url, video_url, filename) VALUES (?,?,?)',
              (page_url, vid_url, filename))
    conn.commit()
    conn.close()

def build_search_index_from_db():
    global search_index
    conn = sqlite3.connect('crawler.db')
    c = conn.cursor()
    rows = c.execute('SELECT url, content FROM pages').fetchall()
    for row in rows:
        if row[0] not in search_index:
            search_index[row[0]] = {"content": row[1], "images": [], "videos": []}
        else:
            search_index[row[0]]["content"] = row[1]
    img_rows = c.execute('SELECT url, image_url FROM images').fetchall()
    for r in img_rows:
        if r[0] not in search_index:
            search_index[r[0]] = {"content": "", "images": [r[1]], "videos": []}
        else:
            if r[1] not in search_index[r[0]]["images"]:
                search_index[r[0]]["images"].append(r[1])
    vid_rows = c.execute('SELECT url, video_url FROM videos').fetchall()
    for v in vid_rows:
        if v[0] not in search_index:
            search_index[v[0]] = {"content": "", "images": [], "videos": [v[1]]}
        else:
            if v[1] not in search_index[v[0]]["videos"]:
                search_index[v[0]]["videos"].append(v[1])
    conn.close()

def download_page_content(url, driver):
    try:
        driver.get(url)
        return driver.page_source
    except:
        return ""

def extract_text_images_videos(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)
    images = []
    videos = []
    for img in soup.find_all('img'):
        src = img.get('src')
        if src and src.startswith('http'):
            images.append(src)
        elif src and src.startswith('/'):
            images.append(base_url + src)
    for vid in soup.find_all('video'):
        src = vid.get('src')
        if src and src.startswith('http'):
            videos.append(src)
        elif src and src.startswith('/'):
            videos.append(base_url + src)
        for source_tag in vid.find_all('source'):
            ssrc = source_tag.get('src')
            if ssrc and ssrc.startswith('http'):
                videos.append(ssrc)
            elif ssrc and ssrc.startswith('/'):
                videos.append(base_url + ssrc)
    return text, images, videos

def download_images(images, page_url):
    if not os.path.exists("downloaded_images"):
        os.mkdir("downloaded_images")
    conn = sqlite3.connect('crawler.db')
    c = conn.cursor()
    for img_url in images:
        already_saved = c.execute('SELECT COUNT(*) FROM images WHERE image_url=?', (img_url,)).fetchone()[0]
        if already_saved > 0:
            continue
        try:
            img_data = requests.get(img_url, timeout=5).content
            filename = img_url.split('/')[-1]
            path = os.path.join("downloaded_images", filename)
            with open(path, 'wb') as f:
                f.write(img_data)
            save_image_to_db(page_url, img_url, filename)
        except:
            pass
    conn.close()

def download_videos(videos, page_url):
    if not os.path.exists("downloaded_videos"):
        os.mkdir("downloaded_videos")
    conn = sqlite3.connect('crawler.db')
    c = conn.cursor()
    for vid_url in videos:
        already_saved = c.execute('SELECT COUNT(*) FROM videos WHERE video_url=?', (vid_url,)).fetchone()[0]
        if already_saved > 0:
            continue
        try:
            vid_data = requests.get(vid_url, timeout=5).content
            filename = vid_url.split('/')[-1]
            path = os.path.join("downloaded_videos", filename)
            with open(path, 'wb') as f:
                f.write(vid_data)
            save_video_to_db(page_url, vid_url, filename)
        except:
            pass
    conn.close()

def get_links(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.startswith('http'):
            links.append(href)
        elif href.startswith('/'):
            links.append(base_url + href)
    return links

def get_base_url(url):
    match = re.match(r'(https?://[^/]+)', url)
    return match.group(1) if match else url

def crawl_task():
    global visited, search_index, last_index_update_time, max_depth_reached, crawl_in_progress
    global current_depth, current_url
    init_db()
    build_search_index_from_db()
    init_visited_from_db()
    max_depth_reached = 0
    start_urls = [
        "https://www.whitehouse.gov",
        "https://www.defense.gov",
        "https://www.nsa.gov",
        "https://www.apple.com",
        "https://www.openai.com",
        "https://www.fakeopenai.co",
        "https://www.microsoft.com",
        "https://www.amazon.com",
        "https://www.pdfage.me",
        "https://www.erosolar.net"
    ]
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome(options=chrome_options)
    pq = []
    for s in start_urls:
        if s not in visited:
            heapq.heappush(pq, (0, s))
    last_depth = 0
    while pq and crawl_in_progress:
        depth, url = heapq.heappop(pq)
        current_depth = depth
        current_url = url
        if depth > max_depth:
            break
        visited.add(url)
        if depth > max_depth_reached:
            max_depth_reached = depth
        html = download_page_content(url, driver)
        base = get_base_url(url)
        text, images, videos = extract_text_images_videos(html, base)
        download_images(images, url)
        download_videos(videos, url)
        save_to_db(url, text, depth)
        for link in get_links(html, base):
            if link not in visited:
                heapq.heappush(pq, (depth+1, link))
        if depth > last_depth:
            last_depth = depth
            build_search_index_from_db()
    driver.quit()
    build_search_index_from_db()
    last_index_update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    crawl_in_progress = False

@app.route('/images/<path:filename>')
def serve_image(filename):
    return send_from_directory('downloaded_images', filename)

@app.route('/videos/<path:filename>')
def serve_video(filename):
    return send_from_directory('downloaded_videos', filename)

@app.route('/')
def index():
    return render_template_string('''
<!DOCTYPE html>
<html>
<head>
    <title>Advanced Crawler UI</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        #results { margin-top: 20px; }
        #info { margin-top: 20px; }
        button { margin-right: 10px; }
        input[type=text] { width: 300px; }
        img { display: block; margin-top: 5px; }
        video { display: block; margin-top: 5px; max-width:200px; }
    </style>
</head>
<body>
    <h1>Advanced Crawler UI</h1>
    <button onclick="startCrawl()">Start Crawl</button>
    <input type="text" id="searchQuery" placeholder="Enter search query"/>
    <button onclick="performSearch()">Search</button>
    <button onclick="performImageSearch()">Search Images</button>
    <button onclick="performVideoSearch()">Search Videos</button>
    <button onclick="history.back()">Back</button>
    <div id="info">
        <p>Last Index Update: <span id="lastIndexUpdate">N/A</span></p>
        <p>Max Depth Reached: <span id="maxDepthReached">N/A</span></p>
        <p>Crawl In Progress: <span id="crawlInProgress">N/A</span></p>
        <p>Current Depth: <span id="currentDepth">0</span></p>
        <p>Current URL: <span id="currentURL"></span></p>
    </div>
    <div id="results"></div>
    <script>
        async function startCrawl() {
            document.getElementById('results').innerHTML = 'Starting crawl...';
            let response = await fetch('/crawl', {method: 'POST'});
            let data = await response.json();
            document.getElementById('results').innerHTML = data.message;
            updateInfo();
        }
        async function performSearch() {
            let q = document.getElementById('searchQuery').value;
            if(!q) return;
            let response = await fetch('/search?q=' + encodeURIComponent(q));
            let data = await response.json();
            let html = '<h2>Results for "'+ data.query +'":</h2><ul>';
            data.results.forEach(r => {
                html += '<li><a href="'+r.url+'" target="_blank">'+r.url+'</a><p>'+r.snippet+'</p>';
                if(r.images.length > 0){
                    r.images.forEach(img => {
                        html += '<img src="'+img+'" style="max-width:200px;">';
                    });
                }
                if(r.videos.length > 0){
                    r.videos.forEach(vid => {
                        html += '<video controls><source src="'+vid+'" type="video/mp4"></video>';
                    });
                }
                html += '</li>';
            });
            html += '</ul>';
            document.getElementById('results').innerHTML = html;
        }
        async function performImageSearch() {
            let q = document.getElementById('searchQuery').value;
            if(!q) return;
            let response = await fetch('/search_images?q=' + encodeURIComponent(q));
            let data = await response.json();
            let html = '<h2>Image results for "'+ data.query +'":</h2><div>';
            data.results.forEach(img => {
                html += '<img src="'+img+'" style="max-width:200px; margin:5px;">';
            });
            html += '</div>';
            document.getElementById('results').innerHTML = html;
        }
        async function performVideoSearch() {
            let q = document.getElementById('searchQuery').value;
            if(!q) return;
            let response = await fetch('/search_videos?q=' + encodeURIComponent(q));
            let data = await response.json();
            let html = '<h2>Video results for "'+ data.query +'":</h2><div>';
            data.results.forEach(vid => {
                html += '<video controls style="max-width:200px; margin:5px;"><source src="'+vid+'" type="video/mp4"></video>';
            });
            html += '</div>';
            document.getElementById('results').innerHTML = html;
        }
        async function updateInfo() {
            let response = await fetch('/info');
            let data = await response.json();
            document.getElementById('lastIndexUpdate').innerText = data.last_index_update_time;
            document.getElementById('maxDepthReached').innerText = data.max_depth_reached;
            document.getElementById('crawlInProgress').innerText = data.crawl_in_progress;
            document.getElementById('currentDepth').innerText = data.current_depth;
            document.getElementById('currentURL').innerText = data.current_url;
            setTimeout(updateInfo, 2000);
        }
        updateInfo();
    </script>
</body>
</html>
''')

@app.route('/crawl', methods=['POST'])
def start_crawl():
    global crawler_thread, crawl_in_progress
    if crawler_thread and crawler_thread.is_alive():
        return jsonify({"message": "Crawl already in progress"}), 400
    crawl_in_progress = True
    crawler_thread = threading.Thread(target=crawl_task)
    crawler_thread.start()
    return jsonify({"message": "Crawl started"})

@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('q', '').lower()
    results = []
    for url, info in search_index.items():
        content_lower = info["content"].lower()
        if query in content_lower:
            idx = content_lower.index(query)
            start = max(0, idx - 100)
            end = min(len(info["content"]), idx + 100)
            snippet = info["content"][start:end].replace('\n',' ')
            imgs = []
            vids = []
            for i in info["images"]:
                fname = i.split('/')[-1]
                imgs.append("/images/" + fname)
            for v in info["videos"]:
                fname = v.split('/')[-1]
                vids.append("/videos/" + fname)
            results.append({"url": url, "snippet": snippet, "images": imgs, "videos": vids})
    return jsonify({"query": query, "results": results})

@app.route('/search_images', methods=['GET'])
def search_images():
    query = request.args.get('q', '').lower()
    conn = sqlite3.connect('crawler.db')
    c = conn.cursor()
    rows = c.execute('SELECT image_url, filename FROM images').fetchall()
    conn.close()
    res = []
    for img_url, fname in rows:
        if query in img_url.lower() or query in fname.lower():
            res.append("/images/" + fname)
    return jsonify({"query": query, "results": res})

@app.route('/search_videos', methods=['GET'])
def search_videos():
    query = request.args.get('q', '').lower()
    conn = sqlite3.connect('crawler.db')
    c = conn.cursor()
    rows = c.execute('SELECT video_url, filename FROM videos').fetchall()
    conn.close()
    res = []
    for vid_url, fname in rows:
        if query in vid_url.lower() or query in fname.lower():
            res.append("/videos/" + fname)
    return jsonify({"query": query, "results": res})

@app.route('/info', methods=['GET'])
def info():
    return jsonify({
        "last_index_update_time": last_index_update_time if last_index_update_time else "N/A",
        "max_depth_reached": max_depth_reached,
        "crawl_in_progress": crawl_in_progress,
        "current_depth": current_depth,
        "current_url": current_url
    })

class GunicornApp(BaseApplication):
    def __init__(self, application, options=None):
        self.application = application
        self.options = options or {}
        super(GunicornApp, self).__init__()

    def load_config(self):
        for key, value in self.options.items():
            if key in self.cfg.settings and value is not None:
                self.cfg.set(key.lower(), value)

    def load(self):
        return self.application

if __name__ == '__main__':
    options = {
        'bind': '0.0.0.0:6999',
        'workers': 4
    }
    GunicornApp(app, options).run()