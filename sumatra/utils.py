__author__ = 'zephyre'


def build_href(refer_url, href):
    import urlparse

    c = urlparse.urlparse(href)
    if c.netloc:
        return href
    else:
        c1 = urlparse.urlparse(refer_url)
        return urlparse.urlunparse((c1.scheme, c1.netloc, c.path, c.params, c.query, c.fragment))
