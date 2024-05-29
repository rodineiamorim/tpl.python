import classes.apoio as func

class system:
  def __init__ (self):
    self.host = "vm22-1.enivix.com.br"
    self.database = "enivix"
    self.port = 5643
    self.user = "ourlife" 
    self.password = "f4m1l14-4m0r1m"
    self.debug = -1
    self.history = ""
    self.folder = ""
    self.site = ""
    self.http = ""
    self.mail_host = "smtp.office365.com"
    self.mail_port = 587
    self.mail_username = "backend@tpl.com.br"
    self.mail_password = "PlPl4t1nuM"

  def load(self):
    i = 0
    with open("/var/oms.machine") as cfg:
      for line in cfg:
        if i==0: 
          self.debug = func.removeAcentos(line)
        if i==1:
          self.history = func.removeAcentos(line)
        if i==2:
          self.folder = func.removeAcentos(line)
        if i==3:
          self.site = func.removeAcentos(line)
        if i==4:
          self.http = func.removeAcentos(line)
        i = i + 1