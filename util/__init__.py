from urllib.parse import urlparse


def make_url(domain, location):
    url = urlparse(location)
    if url.scheme == '' and url.netloc == '':
        return domain + url.path + '?' + url.query
    elif url.scheme == '':
        return 'http://' + url.netloc + url.path + '?' + url.query
    else:
        return url.geturl()
