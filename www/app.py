import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime

from aiohttp import web

def index(request):
    return web.Response(body=b'<h1>Awesome</h1>', content_type="text/html")

def article(request):
    ret = "<h2>Article</h2><br><h1>Title title title</h1>"
    return web.Response(body=ret, content_type="text/html")


async def init(loop):
    logging.info("begin init")
    app = web.Application()
    app.router.add_route('GET', '/', index)
    app.router.add_route('GET', '/article', article)
    
    srv = await loop.create_server(app._make_handler(), '127.0.0.1', 9000)
    logging.info("create server 127.0.0.1:9000 ")
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()
