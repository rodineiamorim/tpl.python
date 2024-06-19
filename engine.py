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
clientes = [] # lista de clientes

print("motor iniciado as " + start.strftime("%d/%m/%Y as %H:%M") )

# montando a lista da tarefas
print( "carregando lista de tarefas" )
db = database.data(sistema)
db.connect()
tasks = db.query(
  "select tl_interval, tl_description, tl_clients, tl_runonfirst, tl_filename, tl_p1, tl_p2, tl_p3, tl_p4 \
     from tasklist \
    where tl_active=1 \
   order by tl_interval"
)
print( "foram enocntradas " + str(len(tasks)) + " tarefas a serem executadas" ) 

tickPassed = 0
while True:
  time.sleep( tick )
  
  tickPassed = tickPassed + 1
  
  for task in tasks:
    if( ((tickPassed % task["tsk_interval"])==0) or (first==1 and task["tsk_runonfirst"]==1) ):
      
      # =================================================================
      # task getclient é majoritária e precisa ser executada por primeiro
      # ela traz a lista dos clientes para os próximos scripts
      # =================================================================
      if( task["tsk_task"]=="getclientes()" ):
        print( "atualizando lista de clientes" )
        db = database.data(sistema)
        db.connect()
        clientes = db.query("select p_cnpj,p_apelido from clientes left join pessoas on p_id=c_pessoa where c_bloqueado=0 order by c_id")
        for c in clientes:
          print(c["p_cnpj"],c["p_apelido"])
      else:
        ##########################################
        # se nao é um script executado por cliente
        ##########################################
        if( task["tsk_clientes"]==0 ):
          os.spawnlp(os.P_NOWAIT, task["tsk_task"], task["tsk_p1"], task["tsk_p2"], task["tsk_p3"], task["tsk_p4"], task["tsk_p5"])
        else:
          for c in clientes:
            if( os.path.exists(task["tsk_task"])==True ):
              #
              # verifica se o script anterior foi concluido (no script, deve conter os.path.basename(__file__) para mandar aviso de que o comando foi concluido)
              #
              # abre registro informando que vai executar o script (o script, deve ter um comando para informar a conclusão do mesmo)
              print(f"executando {task['tsk_task']} para o cliente {c['p_apelido']}")
              os.spawnlp(os.P_NOWAIT, "python3", task["tsk_task"], c["p_cnpj"], task["tsk_p1"], task["tsk_p2"], task["tsk_p3"], task["tsk_p4"], task["tsk_p5"])
            else :
              print(f"tarefa {task['tsk_task']} nao encontrada")
            #
          #
        #
      #
    #
  #
  first = 0
  if( tickPassed>60 ):
    break
