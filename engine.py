# robo responsavel pela execução dos serviços

import classes.config as config
import classes.database as database
import os 
import time
from datetime import datetime

# carregando os dados parametrizados no sistema
sistema = config.system()
sistema.load()

tick = 1 # tempo padrão de execução dos ciclos (1 minuto)
first = 1 # so para saber se o motor deu a partida agora
start = datetime.now()

# montando a lista da tarefas
tasks = { "list" :
  [ 
     {"interval": 60, "runonfirst" : 1, "task" : "getcliente"} 
    ,{"interval": 10, "runonfirst" : 0, "task" : "xm-fedex"} 
  ] 
}

print("motor iniciado as " + start.strftime("%d/%m/%Y as %H:%M") )

tickPassed = 0
while True:
  time.sleep( tick )
  #time.sleep( (tick * 60) )
  
  tickPassed = tickPassed + 1
  
  for task in tasks["list"]:
    if( (task["interval"]==tickPassed) or (first==1 and task["runonfirst"]==1) ):
      if( first==1 ):
        print("partida no motor")
        
      first = 0
      
      # task getclient é majoritária e precisa ser executada por primeiro
      if( task["task"]=="getcliente" ):
        # conectando no banco
        db = database.data(sistema)
        db.connect()

      
      print("executando task", task["task"])

  
  

# conectando no banco
#db = database.data(sistema)
#db.connect()

#  dados = db.query(
#    "select p_apelido,if_cliente,if_host,if_port,if_pasta,if_usuario,if_senha,if_dhapartir \
#       from integracaofedex \
#       left join clientes on c_id=if_cliente  \
#       left join pessoas on p_id=c_pessoa \
#       " + where
#    )



