# robo responsavel pela execução dos serviços

import classes.config as config
import classes.database as database
import os 
import time
from datetime import datetime
import subprocess

# carregando os dados parametrizados no sistema
sistema = config.system()
sistema.load()

tick = 60 # tempo padrão de execução dos ciclos (1 minuto)
first = 1 # so para saber se o motor deu a partida agora
start = datetime.now()
clientes = [] # lista de clientes

print("motor iniciado as " + start.strftime("%d/%m/%Y as %H:%M") )

tickPassed = 0
while True:
  try:
    time.sleep( tick )
    
    start = datetime.now()
    print("rodando as " + start.strftime("%d/%m/%Y as %H:%M") )
    
    # montando a lista da tarefas (a cada 10 minutos ou na primeira vez)
    if( ((tickPassed % 10)==0) or (tickPassed==0) ):
      print( "reconectando o banco" )
      db = database.data(sistema)
      db.connect()
      
      # mata processos falhos
      print("apagando lista de tarefas zumbis")
      db.query("delete from taskdetail where td_dhend is null and td_dh<=current_timestamp - interval '20 minutes'")
      
      # carregando/re-carregando as tarefas
      tasks = db.query(
        "select tl_id, tl_interval, tl_description, tl_clients, tl_runonfirst, tl_filename \
              , coalesce(tl_p1,'') tl_p1 \
              , coalesce(tl_p2,'') tl_p2 \
              , coalesce(tl_p3,'') tl_p3 \
              , coalesce(tl_p4,'') tl_p4 \
              , coalesce(tl_p5,'') tl_p5 \
          from tasklist \
          where tl_active=1 \
        order by tl_interval"
      )
      print( "foram encontradas " + str(len(tasks)) + " tarefas a serem executadas" ) 
      print( "sendo:" )
      for task in tasks:
        print(f"{task['tl_description']} executado a cada {task['tl_interval']} minutos")
    
    tickPassed = tickPassed + 1
    
    for task in tasks:
      if( ((tickPassed % task["tl_interval"])==0) or (first==1 and task["tl_runonfirst"]==1) ):
        
        # =================================================================
        # task getclient é majoritária e precisa ser executada por primeiro
        # ela traz a lista dos clientes para os próximos scripts
        # =================================================================
        if( task["tl_filename"]=="getclientes()" ):
          print( "atualizando lista de clientes" )
          db = database.data(sistema)
          db.connect()
          clientes = db.query("select p_cnpj,p_apelido from clientes left join pessoas on p_id=c_pessoa where c_bloqueado=0 order by c_id")
          db.query(f"update tasklist set tl_lastcall=current_timestamp where tl_id={task['tl_id']}")
          print("tabela de clientes atualizada, com",len(clientes),"clientes")
          print("")
        else:
          ##########################################
          # se nao é um script executado por cliente
          ##########################################
          if( task["tl_clients"]==0 ):
            os.spawnlp(os.P_NOWAIT, task["tl_filename"], task["tl_p1"], task["tl_p2"], task["tl_p3"], task["tl_p4"], task["tl_p5"])
          else:
            db = database.data(sistema)
            db.connect()
            #          
            for c in clientes:
              if( os.path.exists(task["tl_filename"])==True ):
                #
                # verificando se tl_id esta ativo
                #
                # abre registro informando que vai executar o script (o script, deve ter um comando para informar a conclusão do mesmo)
                print(f"executando {task['tl_filename']} para o cliente {c['p_apelido']}")
                comando = ["python3","/var/www/html/oms.tpl/" + task["tl_filename"], c["p_cnpj"], task["tl_p1"], task["tl_p2"], task["tl_p3"], task["tl_p4"], task["tl_p5"]]

                comando = "python3 " + task["tl_filename"] + " " + c["p_cnpj"]
                if( task["tl_p1"]!="" ):
                  comando = comando + " " + task["tl_p1"]
                if( task["tl_p2"]!="" ):
                  comando = comando + " " + task["tl_p2"]
                if( task["tl_p3"]!="" ):
                  comando = comando + " " + task["tl_p3"]
                if( task["tl_p4"]!="" ):
                  comando = comando + " " + task["tl_p4"]
                if( task["tl_p5"]!="" ):
                  comando = comando + " " + task["tl_p5"]
                  
                # registrando a ultima vez que a tarefa foi executada 
                db.query(f"update tasklist set tl_lastcall=current_timestamp where tl_id={task['tl_id']}")
                
                # verificando se ainda esta em execucao
                pids = db.query(f"select td_id,td_pid,td_comando from taskdetail where td_task={task['tl_id']} and td_comando='{comando}' and td_dhend is null")
                if( len(pids)>0 ):
                  print("comando ainda execucao")
                  continue
                  
                pid = subprocess.Popen( 
                  comando.split()
                ).pid
                
                if( pid>0 ):
                  print(f"registrando pid {pid}")
                  db.query(f"insert into taskdetail (td_task,td_pid,td_comando) values ({task['tl_id']},{pid},'{comando}')")
                  print("task registrada")
                
              else :
                print(f"tarefa {task['tl_task']} nao encontrada")
              #
            #
          #
        #
      #
    #
    first = 0
  except Exception as err:
    print("erro inesperado: ", err)
  
