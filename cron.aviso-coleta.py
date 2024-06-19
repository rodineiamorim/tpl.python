import classes.config as config
import classes.database as database
import sys
import os
import base64
import smtplib
from email.message import EmailMessage
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

  where = "and coalesce(c_bloqueado,0)=0 " + cnpj + " order by c.p_apelido, t.p_apelido "

  print( "procurando os clientes - para notificar as transportadoras sobre coleta " + todos )
  dados = db.query(
    "select tc_cliente \
          ,c.p_apelido contrato \
          ,c.p_cnpj cnpj_contrato \
          ,c.p_nome, c.p_endereco, c.p_endereco_numero, c.p_bairro, c.p_cidade, c.p_uf, c.p_cep \
          ,tc_transportadora \
          ,t.p_apelido transportadora \
          ,tc_emailcoleta, tc_avisocoletah1, tc_avisocoletah2, tc_avisocoletah3 \
          ,up.p_apelido as unidade \
          ,up.p_endereco unid_endereco, up.p_endereco_numero as unid_endereconumero \
       from transportadorascliente \
       left join clientes on c_id=tc_cliente  \
       left join pessoas c on c.p_id=c_pessoa \
       left join transportadoras on trn_id=tc_transportadora \
       left join pessoas t on t.p_id=trn_pessoa \
       left join unidades u on u.u_id=c_unidade \
       left join pessoas up on up.p_id=u.u_pessoa \
      where coalesce(tc_emailcoleta,'')<>'' \
        and tc_ativo=1 \
       " + where
    )
  print("foram encontrados " + str(len(dados)) + " registros")
  for row in dados:
    print( "verificando dados do cliente: " + row["contrato"] + " da transportadora " + row["transportadora"] )

    idcliente     = row["tc_cliente"]
    idtransp      = row["tc_transportadora"]
    nometransp      = row["transportadora"]
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
    #
    # centro de distribuicao
    
    #########################################################################
    # fluxo de envio de pedidos
    #########################################################################
    if( cmd == "enviar" ):
      agora = datetime.now()
      data = agora.strftime("%Y/%m/%d")
      hora = agora.strftime("%H:00")

      print(f"verificando se ha pedidos a serem coletados as {hora}")
      
      pedidos = db.query(f"select pd_id,pd_pedido,pd_nf_numero,pd_nf_serie,pd_nf_chave \
        from pedidos \
        left join transportadoras on trn_id=pd_transportadora \
        left join pessoas t on t.p_id=trn_pessoa \
        left join avisocoleta on av_pedido=pd_id and av_data>='{data}' and av_hora>='{hora}' \
        where pd_cliente={idcliente} \
        and pd_transportadora={idtransp} \
        and pd_canc_em is null \
        and not pd_tl_wms is null \
        and not pd_tl_nota is null \
        and av_id is null \
        and pd_tl_coleta is null \
        {filtro} \
        order by pd_id desc "
      )
      
      if( len(pedidos)>0 ):
        local = row["unidade"]
        local_coleta  = row["unid_endereco"] + ", " +row["unid_endereconumero"]

        print("foram encontrado(s) " + str(len(pedidos)) + " a ser(erem) enviado(s)")
        
        body = ""
        qpedidos = 0
        for pedido in pedidos:
          body = body + f"CHAVE NF-e {pedido['pd_nf_chave']} \r\n"
          qpedidos = qpedidos + 1
          
        if( body != "" ):
          print( "enviando e-mail de aviso para coleta" )
          
          servidor_email = smtplib.SMTP(sistema.mail_host, sistema.mail_port)
          servidor_email.starttls()
          servidor_email.login(sistema.mail_username, sistema.mail_password)
          #
          remetente = sistema.mail_username
          destinatarios = []
          lista = row["tc_emailcoleta"]
          print( lista )
          lista = lista.split(";")
          for l in lista :
            destinatarios.append( l )
          msg = EmailMessage()
          msg['Subject'] = 'Aviso de coleta - ' + rem_nome
          msg['From'] = remetente
          msg['To'] = destinatarios
          msg.set_content(f"Prezado {nometransp}\r\n\r\n \
            Informamos que temos pedidos disponíveis para coletas em nosso centro logístico.\r\n\r\n \
            Quantidade de pedidos \r\n \
            {qpedidos} pedido(s)\r\n\r\n \
            Local de Coleta:\r\n \
            Centro:{local}\r\n \
            Endereco: {local_coleta}\r\n\r\n \
            Instruções para Coleta: \r\n \
            - O representante de apresentar um documento de identificação com foto. \r\n\r\n \
            Para qualquer dúvida ou mais informações, estamos à disposição pelo telefone (Número de telefone) Ou pelo E-mail (E-mail de contato). \r\n\r\n \
            Agradecemos a sua preferência e confiança em nossos serviços logísticos. \r\n \
            \r\n \
            \r\n \
            Atenciosamente,")
          #
          r = servidor_email.send_message(msg)
          #
          print( "registrando envio de aviso de coleta" )
          for pedido in pedidos:
            db.query(f"insert into avisocoleta (av_pedido,av_data,av_hora,av_email,av_transportadora) values ({pedido['pd_id']},'{data}','{hora}','{row['tc_emailcoleta']}',{idtransp})")
            db.query(f"insert into historicoPedido (hp_cliente,hp_pedido,hp_statuscodigo,hp_codigo,hp_descricao,hp_complemento,hp_arquivo,hp_dhinicio,hp_transportadora) values ({idcliente},{pedido['pd_id']},10003,'*','TRANSP. NOTIFICADA SOBRE COLETA DISPONIVEL','','',current_timestamp,{idtransp})")

      else:
        print("nao ha pedidos a enviar para este cliente/transportadora")

    print("fim deste cliente")  
  
  print( "fim de [" + cmd + "]" )
else:
  print( db.error )

