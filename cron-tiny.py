import classes.apoio as apoio
import classes.config as config
import classes.database as database
import sys
import os
from datetime import datetime
from datetime import timedelta
import time
import http.client
import json
import xml.etree.ElementTree as ET
import base64 

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
    cmd = ""
    
    if( len(sys.argv)>1 ):
      cnpj = sys.argv[1]
      todos = "- referente ao cliente " + sys.argv[1]
    else:
      todos = "- referente a todos os clientes"
      
    if( len(sys.argv)>2 ):
      cmd = sys.argv[2]
    
    if( cmd=="" ):
      cmd = "consultar"  
    
    print("buscando dados do cliente")
    clientes = db.query(
      f"select p_apelido,p_cnpj,c_id,p_apelido,tiny_appkey,tiny_intervalo,tiny_estoque,tiny_rastreio,coalesce(tiny_etiqueta,0) tiny_etiqueta \
              ,c_apikey,c_token,p_fone1,tiny_situacao,tiny_produtosvia,c_composicaourl,tiny_campotransportadora, tiny_somentemarcador \
              ,tiny_faturar,tiny_buscarxml,tiny_enviaprazoentrega, c_disparapostbackrealtime,c_liberalurl \
          from integracaoTiny \
          left join clientes on c_id=tiny_cliente \
          left join pessoas on p_id=c_pessoa \
          where p_cnpj='{cnpj}' \
            and tiny_ativo=1 \
            and tiny_python=1 \
          order by p_cnpj"
    )
    
    print("encontrado",len(clientes),"cliente(s)")
    for cliente in clientes:
      
      print(cmd,"para o cliente",cliente["p_apelido"])
      
      # variaveis de uso comum
      contrato         = cliente["p_apelido"]
      cnpjcontrato     = cliente["p_cnpj"]
      idcliente        = cliente["c_id"]
      tiny_apikey      = cliente["tiny_appkey"]
      apikey           = cliente["c_apikey"]
      token            = cliente["c_token"]
      fonepadrao       = cliente["p_fone1"]
      intervalo        = cliente["tiny_intervalo"]
      situacao         = cliente["tiny_situacao"]
      itensviapedido   = cliente["tiny_produtosvia"]
      composicaourl    = cliente["c_composicaourl"]
      tiny_transporte  = cliente["tiny_campotransportadora"]
      somente_marcador = cliente["tiny_somentemarcador"]
      faturar          = cliente["tiny_faturar"]
      buscarxml        = cliente["tiny_buscarxml"]
      devolveprevisao  = cliente["tiny_enviaprazoentrega"]
      realtime         = cliente["c_disparapostbackrealtime"]
      liberaurl        = cliente["c_liberalurl"]
      
      # autenticando o cliente junto ao oms para pode processar a integracao
      autenticado = 0
      print("autenticando no oms")
      conn = http.client.HTTPSConnection("oms.tpl.com.br")
      payload = json.dumps({
        "apikey": apikey,
        "token": token,
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
      print("autenticado")
      
      if( cmd == "consultar" ):
        tentativa = 0
        pagina    = 0
        datainicio= datetime.today() - timedelta(days=intervalo)      
        inicio    = datainicio.strftime("%d/%m/%Y")
        final     = datetime.today().strftime("%d/%m/%Y")
        host      = "api.tiny.com.br"
        path      = "/api2/pedidos.pesquisa.php"
        
        print(f"lendo periodo de {inicio} até {final}")

        while( tentativa<5 ):
          pagina = pagina + 1
        
          print(f"lendo pagina {pagina}")
          
          find = f"token={tiny_apikey}&situacao={situacao}&pagina={pagina}&dataInicial={inicio}&formato=JSON"
          conn = http.client.HTTPSConnection(host)
          payload = ''
          headers = {
            'Content-Type': 'application/json'
          }
          conn.request("GET", f"{path}?{find}", payload, headers)
          res = conn.getresponse()
          data = res.read()
          
          httpcode = res.status
          if( httpcode!=200 ):
            print(f"tentativa de leitura da pagina retornou erro, httpcode {httpcode}")
            time.sleep(5)
            pagina = pagina - 1
            tentativa = tentativa + 1
            continue

          try:
            obj = json.loads(data)
          except:
            print("erro na conversao dos dados de retorno")
            continue
          
          if( obj["retorno"]["status_processamento"]!="3" ):
            if( (obj["retorno"]["codigo_erro"]=="23") or (obj["retorno"]["codigo_erro"]=="20") ):
              print("ultima pagina")
              break
            #
          #
          
          try:
            pedidos = obj["retorno"]["pedidos"]
          except:
            print("nao houve retorno de pedidos")
            continue
          
          for pedido in pedidos:
            resumo          = pedido["pedido"]
            tiny_id         = resumo["id"]
            tiny_numero     = resumo["numero"]
            tiny_ecommerce  = resumo["numero_ecommerce"]
            if( tiny_ecommerce==None ):
              tiny_ecommerce = ""
            nropedido       = tiny_numero
            tiny_data       = resumo["data_pedido"]
            tiny_nome       = resumo["nome"].upper()
            tiny_situacao   = resumo["situacao"].upper()
            tiny_valor      = resumo["valor"]
            idnota          = 0
            idexpedicao     = 0
            transportadora  = ''
            metodo          = ''
            sro             = ''
            invoice         = ''
              
            print(f"verificando se o pedido {nropedido} esta duplicado e/ou necessita de complemento")
            
            check = db.query(
              f"select  pd_id \
                      , coalesce(tiny_idpedido,0) tiny_idpedido \
                      , coalesce(tiny_idnota,0) tiny_idnota \
                      , coalesce(tiny_idexpedicao,0) tiny_idexpedicao \
                      , pd_canc_em \
              from pedidos \
              left join pedidoTiny on tiny_pedido=pd_id \
              where pd_cliente={idcliente} and pd_pedido='{nropedido}' "
            )
            
            if( len(check)>0 ):
              print("pedido existente")
              
              idnota = check[0]["tiny_idnota"]
              idpedido = check[0]["pd_id"]
              if ( idnota == 0):
                print("verificando se ha nota no tiny")
                consultapedido  = "https://api.tiny.com.br/api2/pedido.obter.php"
                find = f"token={tiny_apikey}&id={tiny_id}&formato=JSON"
                conn = http.client.HTTPSConnection(host)
                payload = ''
                headers = {
                  'Content-Type': 'application/json'
                }
                conn.request("GET", f"{consultapedido}?{find}", payload, headers)
                res = conn.getresponse()
                data = res.read()
                
                httpcode = res.status
                if( httpcode!=200 ):
                  print(f"erro ao consultar o pedido {nropedido}, httpcode {httpcode}")
                  time.sleep(5)
                  continue
                #
                
                try:
                  obj = json.loads(data)
                except:
                  print("erro na conversao dos dados de retorno")
                  continue
                
                try:
                  status = obj["retorno"]["status"]
                  codigo_erro = ""
                  if( status!="OK" ):
                    codigo_erro = obj["retorno"]["codigo_erro"]
                    
                  if (status != "OK"):
                    if (codigo_erro == "6"):
                      print("aguardar para nova execucao - tempo limite alcancado")
                      time.sleep(6)
                      continue
                    #
                    print(status)
                    continue
                  #
                except:
                  print(f"não foi possivel recuperar os dados do pedido")
                  continue
                #
                try:
                  idnota = obj["id_nota_fiscal"]
                except:
                  idnota = 0
                #  
              #
              if (idnota > 0):
                print(f"idnota: {idnota}")
              else:
                print("idnota nao gerado pelo tiny")
              
              idexpedicao = check[0]["tiny_idexpedicao"]
              if ( (idexpedicao == 0) and (idnota > 0)):
                print("verificando se houve expedicao no tiny")
                consultapedido  = "https://api.tiny.com.br/api2/expedicao.obter.php"
                find = f"token={tiny_apikey}&idObjeto={idnota}&formato=JSON&tipoObjeto=notafiscal"
                conn = http.client.HTTPSConnection(host)
                payload = ''
                headers = {
                  'Content-Type': 'application/json'
                }
                conn.request("GET", f"{consultapedido}?{find}", payload, headers)
                res = conn.getresponse()
                data = res.read()
                
                httpcode = res.status
                if( httpcode!=200 ):
                  print(f"erro ao consultar a expedicao do pedido {nropedido}, httpcode {httpcode}")
                  time.sleep(5)
                  continue
                #
                
                try:
                  obj = json.loads(data)
                except:
                  print("erro na conversao dos dados de retorno")
                  continue
                
                try:
                  idexpedicao = obj["retorno"]["expedicao"]["id"]
                except:
                  idexpedicao = 0
                #  
              #
              if (idexpedicao > 0):
                print(f"idexpedicao = {idnota}")
              else:
                print("idexpedicao nao gerado pelo tiny")
                            
              if( check[0]["tiny_idpedido"]==0 ):
                print("completando dados do pedido com informações do tiny")
                
                idenivix = check[0]["pd_id"]
                db.query(
                  f"insert into pedidotiny \
                    (tiny_pedido \
                    ,tiny_ecom_numero \
                    ,tiny_ecom_ecommerce \
                    ,tiny_idnota \
                    ,tiny_idpedido \
                      ,tiny_nome \
                    ,tiny_transportador \
                    ,tiny_metodo \
                    ,tiny_situacao \
                    ,tiny_valor \
                    ,tiny_deposito \
                    ,tiny_idexpedicao \
                    ,tiny_obs)  \
                    values \
                    ({idenivix} \
                    ,'{tiny_numero}' \
                    ,'{tiny_ecommerce}' \
                    ,{idnota} \
                    ,{tiny_id} \
                    ,'{tiny_nome}' \
                    ,'{transportadora}' \
                    ,'{metodo}' \
                    ,'{situacao}' \
                    ,0 \
                    ,0 \
                    ,{idexpedicao} \
                    ,'')"
                  )
                db.query(f"update pedidos set pd_orderid='{tiny_ecommerce}' where pd_id={idenivix} and coalesce(pd_orderid,'')=''")
              else:
                if ((idnota > 0) and (idexpedicao > 0)):
                  db.query(f"update pedidotiny set tiny_idnota={idnota},tiny_idexpedicao={idexpedicao} where tiny_pedido={idpedido}")
                if ((idnota > 0) and (idexpedicao == 0)):
                  db.query(f"update pedidotiny set tiny_idnota={idnota} where tiny_pedido={idpedido}")
                if ((idnota == 0) and (idexpedicao > 0)):
                  db.query(f"update pedidotiny set tiny_idexpedicao={idexpedicao} where tiny_pedido={idpedido}")
              #            
              continue
            else:
              print("analisando demais dados")
            # fim do IF duplicidade
              
            consultapedido  = "/api2/pedido.obter.php"
            find = f"token={tiny_apikey}&id={tiny_id}&formato=JSON"
            conn = http.client.HTTPSConnection(host)
            payload = ''
            headers = {
              'Content-Type': 'application/json'
            }
            conn.request("GET", f"{consultapedido}?{find}", payload, headers)
            res = conn.getresponse()
            data = res.read()
            
            httpcode = res.status
            if( httpcode!=200 ):
              print(f"erro ao consultar o pedido {nropedido}, httpcode {httpcode}")
              time.sleep(5)
              continue
            #
            
            try:
              detalhe = json.loads(data)
            except:
              print("erro na conversao dos dados de retorno")
              continue
            
            try:
              status = detalhe["retorno"]["status"]
              codigo_erro = ""
              if( status!="OK" ):
                codigo_erro = obj["codigo_erro"]
                
              if (status != "OK"):
                print(f"erro na leitura do pedido - {codigo_erro}")
                time.sleep(6)
                continue
              #
            except:
              print(f"erro ao buscar detalhe do pedido")
              continue
            #
            
            detalhe = detalhe["retorno"]["pedido"]
            try:
              forma_envio = apoio.removeAcentos(detalhe["forma_envio"])
            except:
              forma_envio = ""
            #
            try:
              forma_frete = apoio.removeAcentos(detalhe["forma_frete"])
            except:
              forma_frete = ""
            #
            try:
              nomeEcommerce = apoio.removeAcentos(detalhe["ecommerce"]["nomeEcommerce"])
            except:
              nomeEcommerce = ""
                                                
            # marcadores
            if( somente_marcador != ""):
              try: 
                marcadores = detalhe["marcadores"]
              except:
                marcadores = []
              if( somente_marcador[0:1]!="*" ):
                temmarcador = 0
                for mk in marcadores:
                  if (mk == somente_marcador):
                    temmarcador = 1
                    break
                  #
                #
                if (temmarcador == 9):
                  print("marcador $somente_marcador nao encontrado neste pedido, pedido nao sera integrado")
                  continue
                #
              #
            #
            
            idnota          = detalhe["id_nota_fiscal"]
            
            # destinatario
            dest_nome       = apoio.removeAcentos(detalhe["cliente"]["nome"].upper())
            dest_documento  = apoio.removeMascara(detalhe["cliente"]["cpf_cnpj"])
            dest_endereco   = apoio.removeAcentos(detalhe["cliente"]["endereco"].upper())
            dest_numero     = detalhe["cliente"]["numero"].upper()
            dest_compl      = apoio.removeAcentos(detalhe["cliente"]["complemento"].upper())
            dest_bairro     = apoio.removeAcentos(detalhe["cliente"]["bairro"].upper())
            dest_cidade     = apoio.removeAcentos(detalhe["cliente"]["cidade"].upper())
            dest_uf         = detalhe["cliente"]["uf"].upper()
            dest_fone       = apoio.removeMascara(detalhe["cliente"]["fone"])
            if( dest_fone=="" ):
              dest_fone = fonepadrao
            dest_email      = detalhe["cliente"]["email"]
            dest_cep        = apoio.removeMascara(detalhe["cliente"]["cep"])
            
            # endereco de entrega caso haja
            sucesso = 1
            try:
              if (
                (detalhe["endereco_entrega"]["nome_destinatario"] != "")
                and (detalhe["endereco_entrega"]["endereco"] != "")
                and (detalhe["endereco_entrega"]["cpf_cnpj"] != "")
                and (detalhe["endereco_entrega"]["numero"] != "")
                and (detalhe["endereco_entrega"]["bairro"] != "")
                and (detalhe["endereco_entrega"]["uf"] != "")
                and (detalhe["endereco_entrega"]["cep"] != "")
              ):
                dest_documento = apoio.removeMascara(detalhe["endereco_entrega"]["cpf_cnpj"])
                dest_endereco = apoio.removeAcentos(detalhe["endereco_entrega"]["endereco"])
                dest_numero = apoio.removeAcentos(detalhe["endereco_entrega"]["numero"])
                dest_compl = apoio.removeAcentos(detalhe["endereco_entrega"]["complemento"])
                dest_bairro = apoio.removeAcentos(detalhe["endereco_entrega"]["bairro"])
                dest_cidade = apoio.removeAcentos(detalhe["endereco_entrega"]["cidade"])
                dest_uf = apoio.removeAcentos(detalhe["endereco_entrega"]["uf"])
                dest_fone = apoio.removeMascara(detalhe["endereco_entrega"]["fone"])
                if( dest_fone=="" ):
                  dest_fone = fonepadrao
                                                
                dest_cep = apoio.removeMascara(detalhe["endereco_entrega"]["cep"])
                try:
                  dest_nome = apoio.removeAcentos(detalhe["endereco_entrega"]["nome_destinatario"])
                except:
                  dest_nome = dest_nome
              #            
            except:
              sucesso = 0
            #
            if( dest_numero=="" ):
              dest_numero = "SN"
            
            # transportadora
            try:
              transportadora = apoio.removeAcentos(detalhe["nome_transportador"])
            except:
              transportadora = ""
            transportadora_original = transportadora
            metodo = apoio.removeAcentos(detalhe["forma_envio"])
            if( (len(metodo)==1) and (transportadora=="CORREIOS") ):
              try:
                metodo = apoio.removeAcentos(detalhe["forma_frete"])
              except:
                metodo = metodo
            #
            ignoraforma=0
            if( (transportadora=="CORREIOS") and ((metodo=="PAC") or (metodo=="SEDEX")) ):
              ignoraforma=1

            try:
              if( (somente_marcador[0:1])=="*" ):
                if( somente_marcador[1:]==detalhe["forma_frete"] ):
                  print("pedido ignorado")
                  continue
                #
              #
            except:
              sucesso = 0

            if( ignoraforma==0 ):
              if (transportadora == ""):
                try:
                  transportadora = apoio.removeAcentos(detalhe["forma_frete"])
                except:
                  transportadora = transportadora

              if( transportadora.find("SEDEX")>0 ):
                metodo = "SEDEX"
              if( transportadora.find("PAC")>0 ):
                metodo = "PAC"

              if( (tiny_transporte == 3) and (forma_envio!="") ):
                if( len(forma_envio)>1 ):
                  print("mudando o nome da transportadora conforme parametro de integracao - forma envio")
                  transportadora = forma_envio
                  if( detalhe["ecommerce"]["nomeEcommerce"][0:10]=="AMAZON FBA" ):
                    transportadora = "AMAZON_FBA"
                    metodo = "";    
                  #
                  if( detalhe["ecommerce"]["nomeEcommerce"][0:4]=="MELI" ):
                    transportadora = forma_frete.upper()
                    metodo = "";    
                  #
                  if( detalhe["ecommerce"]["nomeEcommerce"][0:4]=="AMER" ):
                    transportadora = "AMERICANAS"
                    metodo = ""
                  #
                  if( detalhe["ecommerce"]["nomeEcommerce"][0:3]=="LI/" ):
                    transportadora = 'LOJA INTEGRADA'
                    metodo = ""
                  #
                #
              #
              if ( ((tiny_transporte == 1) or (tiny_transporte == 2)) and (forma_frete!="")):
                print("mudando o nome da transportadora conforme parametro de integracao - forma frete")
                transportadora = forma_frete
              #
              if (forma_frete!=""):
                if (forma_frete == "CORREIOS SEDEX"):
                  transportadora = "CORREIOS"
                  metodo = "SEDEX"
                #
                if (forma_frete == "CORREIOS PAC"):
                  transportadora = "CORREIOS"
                  metodo = "PAC"
                #
              #
              if ((metodo != "SEDEX") and (metodo != "PAC")):
                metodo = ""
            #

            if ((tiny_transporte == 2) and (forma_envio!="")) :
              if(forma_envio=="S"):
                try:
                  if( detalhe["ecommerce"]["nomeEcommerce"][0:10]=="AMAZON FBA" ):
                    transportadora = "AMAZON_FBA"
                    metodo = "";    
                except:
                  sucesso = 0
              #
              if( forma_envio=="AMAZON_DBA" ):
                transportadora = "AMAZON_DBA"
                metodo = ""
              #
              if( forma_envio=="SHEIN_ENVIOS" ):
                transportadora = "SHEIN_ENVIOS"
                metodo = ""
              #
            #

            # forçando uma pesquisa de de/para para atender necessidades do cliente
            dpt = db.query(
              f"select dpt_transportadora,p_apelido \
              from deparatransportadora \
              left join transportadoras on trn_id=dpt_transportadora \
              left join pessoas on p_id=trn_pessoa \
              where dpt_cliente={idcliente} \
              and dpt_recebe='{transportadora}'"
            )
            if (len(dpt) > 0):
              transportadora = dpt[0]["p_apelido"]
            #
            # quando a transportadora for FRETE ENIVIX
            # devo gerar um BID e pegar a mais barata
            if ( transportadora == "FRETE ENIVIX" ):
              print("gerando o BID para pegar a transportadora mais barata")
              conn = http.client.HTTPSConnection("oms.tpl.com.br")
              payload = json.dumps({
                "auth": auth["token"],
                "to": dest_cep,
                "weight": 500,
                "value" : tiny_valor
              })
              headers = {
                'Content-Type': 'application/json'
              }
              conn.request("POST", "/api/get/bid", payload, headers)
              res = conn.getresponse()
              data = res.read()
              try:
                obj = json.loads( data.decode("utf-8") )
                sucesso = 1
              except:
                sucesso = 0
              if( sucesso==0 ):
                print("nao foi possivei cotar")
                continue

              for cotacao in obj:                
                transportadora = cotacao["shipmentCompany"]
                break
            #
            #
            if (transportadora == ""):
              if( nomeEcommerce!="" ):
                if( nomeEcommerce[0:10]=="AMAZON FBA" ):
                  transportadora = "AMAZON_FBA"
                  metodo = ""
                #
                if( nomeEcommerce[0:4]=="MELI" ):
                  transportadora = forma_frete.upper()
                  metodo = ""
                #
                if( nomeEcommerce[0,4]=="AMER" ):
                  transportadora = 'AMERICANAS'
                  metodo = ""
                #
                if( nomeEcommerce[0:3]=="LI/" ):
                  transportadora = 'LOJA INTEGRADA'
                  metodo = ""
                #
              #
              #
              if( transportadora=="" ):
                transportadora = "A DEFINIR"
                metodo = ""
              #
            #

            if( (transportadora=="") or (transportadora=="A DEFINIR") ):
              if( nomeEcommerce!="" ):
                if( nomeEcommerce[0,13].upper()=="MERCADO LIVRE" ):
                  transportadora = "MERCADO LIVRE"
                  metodo = ""
                #
              #
              if( forma_envio!="" ):
                if( len(forma_envio)>12 ):
                  if( forma_envio[0,13].upper()=="SHOPEE_ENVIOS" ):
                    transportadora = "SHOPEE"
                    metodo = ""
                    try:
                      sro = detalhe["ecommerce"]["numeroPedidoEcommerce"][0:12]
                    except:
                      sro = ""
                #
              #
            #

            if (transportadora == "FRETE ENIVIX"):
              metodo = ""              

            if ((situacao == "Faturado") and (idnota == 0)):
              print( detalhe )
              print("pedido faturado mas sem nota fiscal")
              continue
            #
            xml = ""
            if (situacao == "Faturado"):
              print("lendo o xml do pedido")
              buscanota = "/api2/nota.fiscal.obter.xml.php"
              find = f"token={tiny_apikey}&id={idnota}&formato=JSON"
              conn = http.client.HTTPSConnection(host)
              payload = ''
              headers = {
                'Content-Type': 'application/json'
              }
              conn.request("GET", f"{buscanota}?{find}", payload, headers)
              res = conn.getresponse()
              data = res.read()
              
              httpcode = res.status
              if( httpcode!=200 ):
                print(f"erro ao consultar a nota do pedido {nropedido}, httpcode {httpcode}")
                time.sleep(5)
                continue
              #
              
              xml = str(data, encoding='utf-8')
              xml = xml.replace("<retorno><status_processamento>3</status_processamento><status>OK</status><xml_nfe>","")
              xml = xml.replace("</xml_nfe></retorno>","")
            
              print("buscando o id de expedicao")
              print("verificando se houve expedicao no tiny")
              consultapedido  = "https://api.tiny.com.br/api2/expedicao.obter.php"
              find = f"token={tiny_apikey}&idObjeto={idnota}&formato=JSON&tipoObjeto=notafiscal"
              conn = http.client.HTTPSConnection(host)
              payload = ''
              headers = {
                'Content-Type': 'application/json'
              }
              conn.request("GET", f"{consultapedido}?{find}", payload, headers)
              res = conn.getresponse()
              data = res.read()
              
              httpcode = res.status
              if( httpcode!=200 ):
                print(f"erro ao consultar a expedicao do pedido {nropedido}, httpcode {httpcode}")
                time.sleep(5)
                continue
              #
              
              try:
                obj = json.loads(data)
              except:
                print("erro na conversao dos dados de retorno")
                continue
              
              try:
                idexpedicao = obj["retorno"]["expedicao"]["id"]
              except:
                idexpedicao = 0
              #  
            
              if (itensviapedido == 1):
                print("determiando os itens do pedido via itens da nota")
                items = []
                """
                if (!isset($objnota->NFe->infNFe->det)) {
                  registraOcorrencia($data, $idcliente, $nropedido, "ERRO TINY", "XML/NFe SEM A TAG DET (PRODUTOS)", $p);
                  continue;
                }
                foreach ($objnota->NFe->infNFe->det as $objitem) {
                  $items[] =
                    [
                      "sku" => strtoupper((string)$objitem->prod->cProd), "amount" => (string)$objitem->prod->qCom, "unitWeight" => 0, "cubingHeight" => 0, "cubingWidth" => 0, "cubingDepth" => 0
                    ];
                }
                """
              #
            #
            
            if( xml != ""):
              invoice = base64.b64encode(xml.encode("ascii"))

            deposito = apoio.coalesce(detalhe,"deposito","")
            try:
              ecom_id = detalhe["ecommerce"]["id"]
            except:
              ecom_id = ""
            try:
              ecom_pedido = detalhe["ecommerce"]["numeroPedidoEcommerce"]
            except:
              ecom_pedido = ""
            try:
              ecom_nome = detalhe["ecommerce"]["nomeEcommerce"]
            except:
              ecom_nome = ""
            obs = apoio.removeAcentos(apoio.coalesce(detalhe,"obs",""))

            # itens 
            if (itensviapedido == 0):
              items = []
              for item in detalhe["itens"]:
                sku = item["item"]["codigo"]
                chk = db.query(f"select prd_sku from produtos where prd_cliente={idcliente} and prd_idplataforma='{sku}'")
                if (len(chk) > 0):
                  sku = chk[0]["prd_sku"]
                items.append(
                  {
                      "sku" : sku
                    , "amount" : item["item"]["quantidade"]
                    , "unitWeight" : 0
                    , "cubingHeight" : 0
                    , "cubingWidth" : 0
                    , "cubingDepth" : 0
                  })
              #
            #
            
            print("montando payload")
            enviar = \
              {
                "auth" : auth["token"]
                , "way" : 15
                , "waydescription" : "ORIGEM TINY"
                , "origin" : 2
                , "idwms" : 0
                , "order" :
                  {
                    "number" : nropedido
                    , "date" : tiny_data
                    , "deliveryTo" : 
                      {
                        "name" : dest_nome
                        , "identification" : dest_documento
                        , "phone" : dest_fone
                        , "mail" : dest_email
                        , "address" : 
                          {
                            "street" : dest_endereco
                            , "number" : dest_numero
                            , "complement" : dest_compl
                            , "neighborhood" : dest_bairro
                            , "city" : dest_cidade
                            , "state" : dest_uf
                            , "zipCode" : dest_cep
                          }
                          , "note" : f"INTEGRADO VIA TINY {obs}"
                      }, "shipping" : 
                    {
                      "company" : transportadora
                      , "method" : metodo
                      , "invoice" : invoice
                      , "tracking" : sro
                    }
                  , "marketPlaceId" : tiny_ecommerce
                  , "wharehouse" : ""
                  , "items" : items
                }
              }

            print("enviando pedido")
            conn = http.client.HTTPSConnection("oms.tpl.com.br")
            payload = str(enviar).replace("'",'"')
            payload = payload.replace('"invoice": b"', '"invoice": "')
            headers = {
              'Content-Type': 'application/text'
            }
            conn.request("POST", "/api/put/order", payload, headers)
            res = conn.getresponse()
            data = res.read()
            httpcode = res.status
            if( httpcode!=200 ):
              print( payload )              
              print(f"tentativa de envio do pedido ao oms com erro, httpcode {httpcode}")
              continue

            try:
              obj = json.loads(data)
            except:
              print("erro na conversao dos dados de retorno")
              continue

            print("registrando dados na tabela pedidotiny")
            idenivix = obj["id"]
            db.query( 
              f"insert into pedidotiny \
                (tiny_pedido \
                ,tiny_ecom_numero \
                ,tiny_ecom_ecommerce \
                ,tiny_idnota \
                ,tiny_idpedido \
                ,tiny_nome \
                ,tiny_transportador \
                ,tiny_metodo \
                ,tiny_situacao \
                ,tiny_valor \
                ,tiny_deposito \
                ,tiny_idexpedicao \
                ,tiny_obs) \
                values \
                ({idenivix} \
                ,'{tiny_numero}' \
                ,'{tiny_ecommerce}' \
                ,{idnota} \
                ,'{tiny_id}' \
                ,'{tiny_nome}' \
                ,'{transportadora}' \
                ,'{metodo}' \
                ,'{situacao}' \
                ,0 \
                ,0 \
                ,{idexpedicao} \
                ,'')"
              )
            
            """
              if( count($escala)>0 ){
                step("integrando o pedido junto ao wms - escala");
                $code = 0;
                $result = [];
                $comando = "php -f cron.escala-envia-pedidos.php $cnpjcontrato $nropedido 0 0 enviar";
                exec($comando,$result,$code);
                print_r( $result );
              }
            """
          #
        #
         
      print( "fim de leitura dos pedidos" )
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