# -*- coding: utf-8 -*-
import os

BOT_NAME = 'data_extractor'

SPIDER_MODULES = ['data_extractor.spiders']
NEWSPIDER_MODULE = 'data_extractor.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'quotesbot (+http://www.yourdomain.com)'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'
             
# Obey robots.txt rules
ROBOTSTXT_OBEY = False  # used CustomRobotsTxtMiddleware
ROBOTSTXT_OBEY_CUSTON = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 1
CONCURRENT_REQUESTS_PER_IP = 1

# REACTOR_THREADPOOL_MAXSIZE = 1

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 3.5    # 1500 ms of delay
RANDOMIZE_DOWNLOAD_DELAY = True   # default to true

# Disable auto throttling.
AUTOTHROTTLE_ENABLED = False

# DEFAULT_REQUEST_HEADERS = {
#   'Accept-Encoding': 'deflate',
# }

#
FEED_EXPORT_ENCODING = 'utf-8'

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    # The middleware (Order 723) needs to take precedence over HttpProxyMiddleware, which 
    # by default is at position 750, so we set the middleware positions to numbers below 750.
    'data_extractor.proxy-middleware.ProxyMiddleware': 99,
    #'data_extractor.robotstxt-middleware.CustomRobotsTxtMiddleware': 100,
    #
    'data_extractor.selenium-middleware.SeleniumDownloadMiddleware': 303,
    # 
    # 'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,    
}