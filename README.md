# zjl-danmu

在线试用：https://nineee.xyz/danmu/

会议室 / 活动用的轻量弹幕墙：观众扫码发弹幕，大屏实时滚动。前后端都在本仓库，可自行改样式、部署和 DIY。

## 能做什么

- WebSocket 实时推送弹幕与城市访客统计
- 按 IP 解析发送地区（可选，依赖 ip2region 库文件）
- 管理页 `/admin`：清屏、开关循环、调速等
- 前端静态资源本地化（Tailwind / Font Awesome），不依赖外网 CDN

## 目录

```
server/app.py          FastAPI + WebSocket 后端
server/Dockerfile      容器构建
static/                前端页面与静态资源
nginx.conf             纯静态 nginx 示例（本地预览用）
deploy/nginx-danmu.snippet.conf   反代到 API 的 nginx 片段
docs/1000-concurrent-loadtest.md   1000 并发压测的方法、脚本和结果
loadtest/                          压测脚本
data/                  运行时数据目录（数据库、ip 库不进仓库）
```

## 本地运行

需要 Python 3.12+。

```bash
pip install -r requirements.txt
# 可选：把 ip2region_v4.xdb 放到 data/，用于显示「中国广州」这类地区
# 下载：https://github.com/lionsoul2014/ip2region
uvicorn server.app:app --host 127.0.0.1 --port 8088
# 若工作目录在 server/：
# cd server && uvicorn app:app --host 127.0.0.1 --port 8088
```

打开 http://127.0.0.1:8088/ 。生产环境通常把 `app.py` 挂到 `/app/app.py`，把 `static/` 挂到 `/static`，把 `data/` 挂到 `/data`。

Docker 示例：

```bash
docker build -t zjl-danmu ./server
docker run --rm -p 8088:8088 \
  -v "$PWD/server/app.py:/app/app.py:ro" \
  -v "$PWD/static:/static:ro" \
  -v "$PWD/data:/data" \
  zjl-danmu
```

## 自行修改

页面文案、颜色、城市榜白名单、弹幕速度与队列都在 `static/index.html` 和 `server/app.py` 里，改完挂载或重建即可。

## 协议

[MIT](LICENSE)。可以自由使用、修改和再分发，请保留版权声明。
