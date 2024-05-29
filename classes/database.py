import psycopg2
import psycopg2.extras

class data:
  def __init__(self, sistema):
    self.host = sistema.host
    self.database = sistema.database
    self.port = sistema.port 
    self.user = sistema.user 
    self.password = sistema.password 
    self.error = ""
    self.db = 0
    self.rows = 0

    return

  def connect(self):
    try:
      self.db = psycopg2.connect(host=self.host, port=self.port, dbname=self.database, user=self.user, password=self.password, cursor_factory= psycopg2.extras.DictCursor)

      self.error = "ok"
    except psycopg2.OperationalError as e:
      self.error = "Erro de conexao com o banco: " + str(e.pgcode)
      return
    
    return self.error
  
  def query(self,query):
    with self.db.cursor() as cur:
      cur.execute(query)
      self.rows = cur.rowcount
      if( query[0:6]=="select" ):
        self.rows = cur.rowcount
        return cur.fetchall()
      else:
        self.db.commit()
        return
