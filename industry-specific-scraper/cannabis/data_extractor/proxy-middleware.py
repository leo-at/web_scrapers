try:
    from .proxy_server import ProxySetting
except:
    from proxy_server import ProxySetting

#
class ProxyMiddleware(object):
    #
    # ProxyHttpHost = "158.151.208.51:8080"
    proxy_host = ProxySetting.GetProxyHttpHost()

    # overwrite process request
    def process_request(self, request, spider):
        # Set the location of the proxy
        if self.proxy_host:
            request.meta['proxy'] = self.proxy_host