import classes.config as config
import classes.database as database
import sys
import os
import base64


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
    todos = "- referente ao cliente {sys.argv[1]}"
  else:
    todos = "- referente a todos os clientes"

  if( len(sys.argv)>2 ):
    cmd = sys.argv[2]

  if( len(sys.argv)>3 ):
    filtro = " and pd_pedido='" + sys.argv[3] + "'"

  where = "where c_bloqueado=0 " + cnpj + " order by p_apelido"

  print( "procurando os clientes - para envio dos XMLs via FTP " + todos )
  dados = db.query(
    "select p_apelido,if_cliente,if_host,if_port,if_pasta,if_usuario,if_senha,if_dhapartir \
       from integracaofedex \
       left join clientes on c_id=if_cliente  \
       left join pessoas on p_id=c_pessoa \
       " + where
    )

  for row in dados:
    print( "verificando dados do cliente: " + row["p_apelido"] )

    idcliente = row["if_cliente"]
    endereco  = row["if_host"]
    usuario   = row["if_usuario"]
    senha     = row["if_senha"]
    pasta     = row["if_pasta"]
    porta     = row["if_port"]
    apartir   = row["if_dhapartir"].strftime("%Y/%m/%d")
    
    if( cmd == "enviar" ):
      print("procurando os pediods a serem enviaos - gerados e nao enviados a partir de " + apartir)
      print("dados de conexao: host: ",endereco," pasta: ", pasta, ", porta: ",porta, ", usuario: ",usuario," senha: ",senha)

      pedidos = db.query("select pd_contrato,pd_id,pd_pedido,pd_nf_chave,pn_arquivo \
          ,c.p_apelido contrato, c.p_cnpj cnpj \
        from pedidos \
        left join transportadoras on trn_id=pd_transportadora \
        left join pessoas t on t.p_id=trn_pessoa \
        left join pedidonota on pn_pedido=pd_id \
        left join clientes on c_id=pd_cliente \
        left join pessoas c on c.p_id=c_pessoa \
        left join pedidofedex on pf_pedido=pd_id \
        where pd_cliente=" + str(idcliente) + "\
        and t.p_apelido like '%FEDEX%' \
        and not pn_id is null \
        and coalesce(pf_sucesso,0)=0 \
        and cast(pd_tl_inc as date)>='" + apartir + "' \
        " + filtro + "\
        order by pd_id desc "
      )
      if( len(pedidos)>0 ):
        print("foram encontrado(s) " + str(len(pedidos)) + " a ser(erem) enviado(s)")
        for pedido in pedidos:

          try:

            print("gerando uma copia para transferencia")
            nome = pedido["pd_nf_chave"] + "-NFe.xml"
            copia = "/var/www/html" + sistema.folder + "/export/" + nome  
            if( os.path .exists(copia) ):
              print("arquivo ja existe nao é necessário sobrepor")
            else:
              print("aquivo origem nao existe, sera criado com base no banco")
              conteudo = base64.b64decode(pedido["pn_arquivo"])
              arquivo = open(copia, 'w+')
              arquivo.writelines( str(conteudo) )
              arquivo.close()
              if( os.path .exists(copia) ):
                print("arquivo criado")
              else:
                print("nao foi possivel criar o arquivo")
                continue
            
            sucesso = os.system("curl --connect-timeout 10 -k -u " + usuario +":" + senha + " -T " + copia + " " + endereco + ":" + str(porta) + pasta + "/" + nome)

            if( sucesso == 0 ):
              print("enviado com sucesso")
              je = db.query("select pf_id from pedidofedex where pf_pedido=" + str(pedido["pd_id"]) )
              if( len(je)>0 ):
                db.query("update pedidofedex set pf_dhenviado=current_timestamp,pf_sucesso=1 where pf_pedido=" + str(pedido["pd_id"]) )
              else:
                db.query("insert into pedidofedex (pf_pedido,pf_sucesso,pf_dhenviado) values (" + str(pedido["pd_id"]) + ",1,current_timestamp)")
              print("envio registrado com sucesso")
              
              print("apagando arquivo temporario")
              os.remove(copia)
              if( os.path .exists(copia) ):
                print("nao foi possivel apagar o arquivo")
              else:
                print("arquivo apagado com sucesso")
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

