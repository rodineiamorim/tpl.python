import classData.py as database

class api:
  def __init__(self, url, method, body):
    self.url = url
    self.method = method
    self.body = body

