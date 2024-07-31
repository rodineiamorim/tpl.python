import classes.config as config
import classes.database as database
import sys
import os
from datetime import datetime
import http.client
import json

pid = os.getpid()
print(f"PID deste processo {pid}")

# carregando os dados parametrizados no sistema
sistema = config.system()
sistema.load()

# conectando no banco
db = database.data(sistema)
db.connect()

if( db.error=="ok" ):
  observacao = 'nao havia dados'
  try:
    cnpj = ""
    todos = ""
    filtro = ""
    
    if( len(sys.argv)>1 ):
      cnpj = sys.argv[1]
      todos = "- referente ao cliente " + sys.argv[1]
    else:
      todos = "- referente a todos os clientes"
    
    print("buscando os gatilhos")
    gatilhos = db.query(
      f"select c_id,p_apelido,gwh_url,gwh_httpcode,gwh_gatilho,gwh_apartirde,sp_nome, \
               c_apikey,c_token \
         from gatilhoswebhook \
         left join clientes on c_id=gwh_cliente \
         left join pessoas on p_id=c_pessoa \
         left join statuspedido on sp_id=gwh_gatilho \
        where p_cnpj='{cnpj}' \
          and coalesce(c_bloqueado,0)=0 \
          and gwh_gatilho > 0 \
          and not gwh_apartirde is null \
      "
    )
    
    print("encontrado",len(gatilhos),"gatilhos")
    for gatilho in gatilhos:
      apartir = gatilho["gwh_apartirde"].strftime("%Y/%m/%d %H:%M")
      start = datetime.now()
      print("verificando se devo disparar as",start.strftime("%d/%m/%Y as %H:%M"),"- gatilho:",gatilho["sp_nome"] )
      
      campo = ""
      if( gatilho["gwh_gatilho"]==400 ):
        campo = "pd_tl_checkout"
        
      where = f"where pd_cliente={gatilho['c_id']} \
                  and coalesce(fwh_httpcode,0)<>200 \
                  and coalesce(fwh_sucesso,0)=0 \
                  and not {campo} is null \
                  and {campo} > \'{apartir}\' \
                order by {campo} desc "
      dados = db.query(
        f"select pd_id,pd_pedido,pd_contrato,coalesce(fwh_id,0) fwh_id,fwh_enviado,fwh_titulo \
            from pedidos \
            left join filawebhook on fwh_pedido=pd_id \
            {where} "
        )
      print("foram encontrados " + str(len(dados)) + " pedidos a notificar")
      
      # recupera o auth para consultar pedidos
      if( len(dados)>0 ):
        autenticado = 0
        print("autenticando")
        conn = http.client.HTTPSConnection("oms.tpl.com.br")
        payload = json.dumps({
          "apikey": gatilho["c_apikey"],
          "token": gatilho["c_token"],
          "email": "integracao@tpl.com.br"
        })
        headers = {
          'Content-Type': 'application/json'
        }
        conn.request("POST", "/api/get/auth", payload, headers)
        res = conn.getresponse()
        data = res.read()
        try:
          auth = json.loads( data.decode("utf-8") )
          token = auth["token"] 
          autenticado = 1
        except:
          autenticado = 0
        if( autenticado==0 ):
          print("cliente nao autenticado")
          continue
        #
        for pedido in dados:
          print( "buscando situacao do pedido:",pedido["pd_pedido"] )
          
          if( pedido["fwh_id"]==0 ):
            conn = http.client.HTTPSConnection("oms.tpl.com.br")
            payload = json.dumps({
              "auth": auth["token"],
              "order": {
                "number": pedido["pd_pedido"]
              }
            })
            headers = {
              'Content-Type': 'application/json'
            }
            conn.request("POST", "/api/get/orderdetail", payload, headers)
            res = conn.getresponse()
            data = res.read()
            try:
              obj = json.loads(data)
              enviar = data
            except:
              print("nao foi possivel recuperar os dados de situacao do pedido")
              continue
            #
          else:
            enviar = pedido["fwh_enviado"]  
            
          try:
            #
            print("enviando os dados para o servidor de destino")
            
            url = gatilho["gwh_url"]
            host = ""
            rota = ""
            if( url.index(".com.br")>0 ):
              host = url[0:url.index(".com.br")+7]
              rota = url.replace(host,"")
              host = host.replace("https://","")
            conn = http.client.HTTPSConnection(host)
            headers = {
              'Content-Type': 'application/json'
            }
            conn.request("POST", rota, enviar, headers)
            res = conn.getresponse()
            data = res.read()
            sucesso = 0
            try:
              if( res.status == gatilho["gwh_httpcode"] ):
                obj = json.loads(data)
                sucesso = 1
              #
            except:
              print("erro na conversao dos dados de retorno")
              continue
            #
            
            httpcode = res.status
            response = ""
            if( type(data) is bytes ):
              response = data.decode("utf-8")
            
            if( sucesso==1 ):
              print("registrando o sucesso",res.status)
            else: 
              print("registrando a falha",res.status)
              
            if( pedido["fwh_id"]==0 ):
              print("criando registro na fila")
              db.query(f"insert into filawebhook (fwh_pedido,fwh_gatilho,fwh_titulo,fwh_enviado,fwh_retorno,fwh_sucesso,fwh_dhdisparo,fwh_httpcode) values ({pedido['pd_id']},{gatilho['gwh_gatilho']},'{gatilho['sp_nome']}','{enviar}','{response}',{sucesso},current_timestamp,{httpcode})")
            else:
              print("atualizando disparo")
              db.query(f"update filawebhook set fwh_dhdisparo=current_timestamp,fwh_httpcode={httpcode},fwh_retorno='{response}',fwh_url='{url}',fwh_sucesso={sucesso} where fwh_id={pedido['fwh_id']}")
            #
            
            # gravar na historico pedido que o evento foi gerado
            db.query(f"update historicopedido set hp_webhook=current_timestamp where hp_pedido={pedido['pd_id']} and hp_descricao='{gatilho['sp_nome']}' and hp_webhook is null")
                        
          except Exception as err:
            print("erro na consulta do status do pedido")
            print(err)
            continue
      #  
         
      print( "fim de [" , gatilho[6], "]" )
      observacao = "ok"
    #
  except Exception as err:
    print("erro inesperado: ", err)
    observacao = "erro " + err
  
  print(f"finalizando o pid {pid}")
  db.query(f"update taskdetail set td_dhend=current_timestamp,td_log='{observacao}' where td_pid={pid} and td_dhend is null")
  print("pid encerrado")
  
else:
  print( db.error )

