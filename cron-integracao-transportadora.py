import classes.config as config
import classes.database as database
import sys
import os
import base64
import smtplib
from email.message import EmailMessage
import http.client
import json
from datetime import datetime

# carregando os dados parametrizados no sistema
sistema = config.system()
sistema.load()

# conectando no banco
db = database.data(sistema)
db.connect()

if( db.error=="ok" ):
  cnpj = ""
  todos = ""
  cmd = "enviar"
  filtro = ""
  
  if( len(sys.argv)>1 ):
    cnpj = "and c.p_cnpj='"+ sys.argv[1] +"'"
    todos = "- referente ao cliente " + sys.argv[1]
  else:
    todos = "- referente a todos os clientes"
    
  if( len(sys.argv)>2 ):
    cmd = sys.argv[2]    

  if( len(sys.argv)>3 ):
    filtro = " and pd_pedido='" + sys.argv[3] + "'"

  where = "and coalesce(c_bloqueado,0)=0 " + cnpj + " order by c.p_apelido, t.p_apelido"

  print( "procurando os clientes - para envio dos pedidos via integracao " + todos )
  dados = db.query(
    "select tc_cliente \
          ,c.p_apelido contrato \
          ,c.p_cnpj cnpj_contrato \
          ,c.p_nome, c.p_endereco, c.p_endereco_numero, c.p_bairro, c.p_cidade, c.p_uf, c.p_cep \
          ,tc_transportadora \
          ,t.p_apelido transportadora \
          ,tc_integracaovia \
          ,tc_iv_host \
          ,tc_iv_port \
          ,tc_iv_folder \
          ,tc_iv_username \
          ,tc_iv_password \
          ,tc_iv_inicio \
          ,tc_iv_tipodado \
          ,tc_token \
       from transportadorascliente \
       left join clientes on c_id=tc_cliente  \
       left join pessoas c on c.p_id=c_pessoa \
       left join transportadoras on trn_id=tc_transportadora \
       left join pessoas t on t.p_id=trn_pessoa \
      where tc_integracaovia>0 \
       " + where + " limit 1"
    )
  print("foram encontrados " + str(len(dados)) + " registros")
  for row in dados:
    print( "verificando dados do cliente: " + row["contrato"] + " da transportadora " + row["transportadora"] )

    idcliente     = row["tc_cliente"]
    idtransp      = row["tc_transportadora"]
    apitoken      = row["tc_token"]
    endereco      = row["tc_iv_host"]
    usuario       = row["tc_iv_username"]
    senha         = row["tc_iv_password"]
    pasta         = row["tc_iv_folder"]
    porta         = row["tc_iv_port"]
    apartir       = row["tc_iv_inicio"].strftime("%Y/%m/%d")
    tipo          = row["tc_iv_tipodado"]
    via           = row["tc_integracaovia"]
    #
    # dados do remetente
    rem_cnpj      = row["cnpj_contrato"]
    rem_nome      = row["contrato"]
    rem_endereco  = row["p_endereco"]
    rem_numero    = row["p_endereco_numero"]
    rem_bairro    = row["p_bairro"]
    rem_cidade    = row["p_cidade"]
    rem_uf        = row["p_uf"]
    rem_cep       = row["p_cep"]
    
    if( cmd == "enviar" ):
      print("procurando os pedidos a serem enviados - a partir de " + apartir)
      
      # montando mensagem - apenas informativo na tela
      msg = ""
      if( via==1 ):
        msg = "api"
      if( via==2 ):
        msg = "ftp"
      if( via==3 ):
        msg = "sftp"
      if( via==4 ):
        msg = "email"
      if( tipo==1 ):
        msg = msg + " enviando xml-nf"
      if( tipo==2 ):
        msg = msg + " enviando json"
      if( tipo==3 ):
        msg = msg + " enviando edi"
      print("buscando pediods do cliente",row["contrato"])
      print("para integracao com a transportadora",row["transportadora"],"através de",msg)

      pedidos = db.query("select pd_contrato,pd_id,pd_pedido \
          ,pd_nf_numero,pd_nf_serie,pd_nf_chave,pd_trn_volumes,pd_nf_vnf \
          ,pd_nf_pbruto,pn_arquivo,pd_nf_emissao \
          ,c.p_apelido contrato, c.p_cnpj cnpj \
          ,pdd_endereco,pdd_numero,pdd_bairro,pdd_cidade,pdd_uf,pdd_cep \
          ,pd_dest_documento,pd_dest_nome,pd_dest_email, pd_dest_fone \
        from pedidos \
        left join transportadoras on trn_id=pd_transportadora \
        left join pessoas t on t.p_id=trn_pessoa \
        left join pedidonota on pn_pedido=pd_id \
        left join clientes on c_id=pd_cliente \
        left join pessoas c on c.p_id=c_pessoa\
        left join pedidoenviado on pe_pedido=pd_id \
        left join pedidosdestinatario on pdd_pedido=pd_id \
        where pd_cliente=" + str(idcliente) + "\
        and pd_transportadora=" + str(idtransp) + "\
        and not pn_id is null \
        and pd_canc_em is null \
        and coalesce(pe_sucesso,0)=0 \
        and cast(pd_tl_inc as date)>='" + apartir + "' \
        " + filtro + "\
        order by pd_id desc "
      )
      if( len(pedidos)>0 ):
        print("foram encontrado(s) " + str(len(pedidos)) + " a ser(erem) enviado(s)")
        for pedido in pedidos:

          try:
            print("pedido " + pedido["pd_pedido"] + " referente nota " + str(pedido["pd_nf_numero"]) )
            print("gerando uma copia para transferencia")
            nome = pedido["pd_nf_chave"] + "-NFe.xml"
            copia = "/var/www/html" + sistema.folder + "/export/" + nome  
            origem_base64 = pedido["pn_arquivo"]
            conteudo = base64.b64decode(origem_base64)
            msg = ""
            erro = ""
            if( os.path .exists(copia) ):
              print("arquivo ja existe nao é necessário sobrepor")
            else:
              print("aquivo origem nao existe, sera criado com base no banco")
              arquivo = open(copia, 'w+')
              arquivo.writelines( str(conteudo) )
              arquivo.close()
              if( os.path .exists(copia) ):
                print("arquivo criado")
              else:
                print("nao foi possivel criar o arquivo")
                continue

            # envio de informacao via api
            if( via==1 ):
              #conn = http.client.HTTPSConnection("englobasistemas.com.br")
              #payload = origem_base64
              #headers = {
              #  'Content-Type': 'text/plain'
              #}
              #conn.request("POST", "/arquivos/api/GerarPedido/Gerar?apikey=" + apitoken, payload, headers)
              #res = conn.getresponse()
              #data = res.read()
              #print(data.decode("utf-8"))              
              #sucesso = 1 
              
              # md loggi
              #############################################################################################
              if( idtransp==163 ):
                volumes = pedido["pd_trn_volumes"]
                if( volumes==0 ):
                  volumes = 1
                  
                cnpjPagador = "04838701000660"
                if( pedido["pd_nf_pbruto"]>0 ):
                  peso = pedido["pd_nf_pbruto"] / 1000
                else: 
                  peso = 0.500
                  
                emissao = datetime.strptime(pedido["pd_nf_emissao"]).strftime(‘%d/%m/%Y’)
                
                conn = http.client.HTTPSConnection("ssw.inf.br")
                payload = json.dumps([
                  {
                    "cnpjTransportadora": "21930065000297",
                    "usuario": usuario,
                    "senha": senha,
                    "dados": [
                      {
                        "remetente": {
                          "cnpj": rem_cnpj,
                          "nome": rem_nome,
                          "inscr": "ISENTO",
                          "endereco": {
                            "rua": rem_endereco,
                            "numero": rem_numero,
                            "bairro": rem_bairro,
                            "cidade": rem_cidade,
                            "uf": rem_uf,
                            "cep": rem_cep
                          }
                        },
                        "destinatario": [
                          {
                            "nome": pedido["pd_dest_nome"],
                            "cnpj": pedido["pd_dest_documento"],
                            "email": pedido["pd_dest_email"],
                            "telefone": pedido["pd_dest_fone"],
                            "celular": pedido["pd_dest_fone"],
                            "endereco": {
                              "rua": pedido["pdd_endereco"],
                              "numero": pedido["pdd_numero"],
                              "bairro": pedido["pdd_bairro"],
                              "cidade": pedido["pdd_cidade"],
                              "uf": pedido["pdd_uf"],
                              "cep": pedido["pdd_cep"],
                            },
                            "nf": [
                              {
                                "cnpjPagador": cnpjPagador,
                                "condicaoFrete": "CIF",
                                "numero": pedido["pd_nf_numero"],
                                "serie": pedido["pd_nf_serie"],
                                "dataEmissao": emissao,
                                "qtdeVolumes": volumes,
                                "valorMercadoria": pedido["pd_nf_vnf"],
                                "pesoReal": peso,
                                "cubagem": 0,
                                "chaveNFe": pedido["pd_nf_chave"],
                                "pedido": pedido["pd_pedido"],
                              },
                            ]
                          }
                        ]
                      }
                    ]
                  }
                ])
                headers = {
                  'Content-Type': 'application/json'
                }
                conn.request("POST", "/api/notfis", payload, headers)
                res = conn.getresponse()
                data = res.read()

                sucesso = 0
                erro = ""
                msg = ""
                origem_base64 = ""
                try:
                  objeto = json.loads( str(data.decode("utf-8")) )
                  origem_base64 = str(data.decode("utf-8"))
                except ValueError as e:
                  erro = "erro na conversao do retorno"
                
                if( erro=="" ):
                  for result in objeto:
                    if( result["sucesso"]==0 ):
                      erro = result["mensagem"] 
                if( erro=="" ):
                  sucesso = 1

            # envio da informacao via sftp
            #########################################################################################################
            if( via==3 ):
              run = os.system("curl --connect-timeout 10 -k -u " + usuario +":" + senha + " -T " + copia + " " + endereco + ":" + str(porta) + pasta + "/" + nome)
              if( run == 0 ):
                sucesso = 1
              
            # envio da informacao via e-mail
            #########################################################################################################
            if( via==4 ):
              servidor_email = smtplib.SMTP(sistema.mail_host, sistema.mail_port)
              servidor_email.starttls()
              servidor_email.login(sistema.mail_username, sistema.mail_password)
              #
              remetente = sistema.mail_username
              destinatarios = [ endereco ]
              msg = EmailMessage()
              msg['Subject'] = 'Envio da NFe do pedido ' + pedido["pd_pedido"]
              msg['From'] = remetente
              msg['To'] = destinatarios
              msg.set_content("Ola \r\n\r\nSegue nota fiscal numero " + str(pedido["pd_nf_numero"]) + " referente o pedido " + pedido["pd_pedido"] + ".\r\n\r\n\r\nAtenciosamente,")
              msg.add_attachment(conteudo, maintype='application', subtype='xml',filename=pedido["pd_nf_chave"] + "-NFe.xml")
              #
              servidor_email.send_message(msg)
              #
              sucesso = 1
            #
            if( sucesso == 1 ):
              print("enviado com sucesso")
              je = db.query("select pe_id from pedidoenviado where pe_pedido=" + str(pedido["pd_id"]) )
              if( len(je)>0 ):
                db.query("update pedidoenviado set pv_dhenviado=current_timestamp,pf_sucesso=1,pv_arquivo='" + origem_base64 + "' where pv_pedido=" + str(pedido["pd_id"]) )
              else:
                db.query("insert into pedidoenviado (pe_pedido,pe_sucesso,pe_dhenviado,pe_tipo,pe_arquivo) values (" + str(pedido["pd_id"]) + ",1,current_timestamp," + str(tipo) + ",'"+str(origem_base64).replace("'","")+"')")
              print("envio registrado com sucesso")
              
              print("apagando arquivo temporario")
              os.remove(copia)
              if( os.path .exists(copia) ):
                print("nao foi possivel apagar o arquivo")
              else:
                print("arquivo apagado com sucesso")
            else:
              if( erro!="" ):
                msg = erro 
              else: 
                msg = "erro nao reconhecido";
              if( sucesso=="9" ): 
                msg = "caminho inexistente"
              if( sucesso=="67" ): 
                msg = "usuario/senha invalido"
              if( sucesso=="28" ):
                msg = "tempo esgotado"
              print("erro na transferenica:",msg)
                  
          except Exception as err:
            print("erro de conexao / envio: ", err)
            break

      else:
        print("nao ha pedidos a enviar para este cliente")

    print("fim deste cliente")  
  
  print( "fim de [" + cmd + "]" )
else:
  print( db.error )

