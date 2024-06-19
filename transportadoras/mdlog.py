import http.client
from datetime import datetime
import json

class transportadora:
  def __init__(self):
    self.username = ""
    self.password = ""
    self.urlbase = "ssw.inf.br"
    self.urlenvio = "/api/notfis"
    self.urlstatus = "/api/trackingpag"
    self.cnpjPagador = ""


  ###################################################################################
  # metodo para definicao de pacote - que sera usado para envio ou consulta de status
  ###################################################################################
  def remetente(self,objeto):
    self.remetente = objeto
    self.username = objeto["usuario"]
    self.password = objeto["senha"]
    #self.cnpjPagador = "04838701000660"
    self.cnpjPagador = objeto["unidade"]


  ###################################################################################
  # metodo para definicao de pacote - que sera usado para envio ou consulta de status
  ###################################################################################
  def pacote(self,objeto):
    self.pedido = objeto
    
  
  ###################################################################################
  # metodo para envio do pacote
  ###################################################################################
  def envio(self):
    dadosb64 = ""
    sucesso = 0
    erro = ""
    volumes = self.pedido["pd_trn_volumes"]
    if( volumes==0 ):
      volumes = 1
    if( volumes==None ):
      volumes = 1
      
    pbruto = self.pedido["pd_nf_pbruto"]
    if( pbruto == None ):
      pbruto = 500
    if( pbruto>0 ):
      peso = pbruto / 1000
    else: 
      peso = 0.500
      
    emissao = self.pedido["pd_nf_emissao"].strftime("%d/%m/%Y")
    documento = self.pedido["pd_dest_documento"].replace("/","")
    documento = documento.replace(".","")
    documento = documento.replace("-","")
    
    conn = http.client.HTTPSConnection("ssw.inf.br")
    payload = json.dumps([
      {
        "cnpjTransportadora": "21930065000297",
        "usuario": self.username,
        "senha": self.password,
        "dados": [
          {
            "remetente": {
              "cnpj": self.remetente["rem_cnpj"],
              "nome": self.remetente["rem_nome"],
              "inscr": "ISENTO",
              "endereco": {
                "rua": self.remetente["rem_endereco"],
                "numero": self.remetente["rem_numero"],
                "bairro": self.remetente["rem_bairro"],
                "cidade": self.remetente["rem_cidade"],
                "uf": self.remetente["rem_uf"],
                "cep": self.remetente["rem_cep"]
              }
            },
            "destinatario": [
              {
                "nome": self.pedido["pd_dest_nome"],
                "cnpj": documento,
                "email": self.pedido["pd_dest_email"],
                "telefone": self.pedido["pd_dest_fone"],
                "celular": self.pedido["pd_dest_fone"],
                "endereco": {
                  "rua": self.pedido["pdd_endereco"],
                  "numero": self.pedido["pdd_numero"],
                  "bairro": self.pedido["pdd_bairro"],
                  "cidade": self.pedido["pdd_cidade"],
                  "uf": self.pedido["pdd_uf"],
                  "cep": self.pedido["pdd_cep"],
                },
                "nf": [
                  {
                    "cnpjPagador": self.cnpjPagador,
                    "condicaoFrete": "CIF",
                    "numero": self.pedido["pd_nf_numero"],
                    "serie": self.pedido["pd_nf_serie"],
                    "dataEmissao": emissao,
                    "qtdeVolumes": volumes,
                    "valorMercadoria": self.pedido["pd_nf_vnf"],
                    "pesoReal": peso,
                    "cubagem": 0,
                    "chaveNFe": self.pedido["pd_nf_chave"],
                    "pedido": self.pedido["pd_pedido"],
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
    
    try:
      dadosb64 = str(data.decode("utf-8"))
      objeto = json.loads( dadosb64 )
    except ValueError as e:
      erro = "erro na conversao do retorno"    
    
    sucesso = 0
    if( erro=="" ):
      if( "sucesso" in objeto ):
        erro = objeto["mensagem"]
      else:
        for result in objeto:
          if( result["sucesso"]==0 ):
            erro = result["mensagem"] 
        print( payload )  
      #
    if( erro=="" ):
      sucesso = 1
    
    return {"sucesso": sucesso, "erro": erro, "dadosb64": dadosb64}


  ###################################################################################
  # metodo para envio consulta do status do pacote
  ###################################################################################
  def status(self,db):
    conn = http.client.HTTPSConnection(self.urlbase)
    payload = json.dumps({
      "cnpj": self.cnpjPagador,
      "sigla_emp": self.remetente["rem_cnpj"],
      "pedido": self.pedido["pedido"]
    })
    headers = {
      'Content-Type': 'application/json'
    }
    sucesso = 0
    erro = ""
    historico = []
    try:
      conn.request("POST", self.urlstatus, payload, headers)
      res = conn.getresponse()
      data = res.read()
      try:
        objeto = json.loads( data )
        sucesso = objeto["success"]
      except ValueError as e:
        erro = "erro na conversao do retorno"
    except ValueError as e:
      erro = "erro de conexao a MDLOG"
      print( "nao foi possivel conectar no host para consulta do status")
      
    if( objeto["success"]==0 ):
      erro = objeto["message"] 
      if( erro=="Nenhum documento localizado" ):
        datahora = datetime.now()
        datahora = datahora.strftime("%Y/%m/%d %H:%M")
        historico.append({"dh":datahora,"cidade":"", "codtransp":"*", "macro":9999, "ocorrencia":"AGUARDANDO EMISSAO DO CTE NA TRANSPORTADORA", "complemento":"EM DIGITACAO", "tipo":0, "sp":9999})
        sucesso = 1
        erro = ""
    else:
      for row in objeto["tracking"]:     
        datahora = row["data_hora"].replace("T"," ")
        cidade = row["cidade"]
        ocorrencia = row["ocorrencia"]
        if( ocorrencia.index("(")>0 ):
          ocorrencia = ocorrencia[0:ocorrencia.index("(")-1]
        detalhe = row["descricao"]
        tipo = 0
        macro = 0
        codigo = "*"
        sp = 0
        dados = db.query("select st_statuspedido,st_tipo,sp_id from statustransportadora left join statuspedido on sp_api=st_statuspedido where st_transportadora=163 and st_nome='"+ocorrencia+"'")
        if( len(dados)>0 ):
          macro = dados[0]["st_statuspedido"]
          tipo = dados[0]["st_tipo"]
          sp = dados[0]["sp_id"]
        historico.append({"dh":datahora,"cidade":cidade, "codtransp":codigo, "macro":macro, "ocorrencia":ocorrencia, "complemento":detalhe, "tipo":tipo, "sp":sp})
            
    retorno = {"sucesso":sucesso, "erro":erro, "historico":historico}
    
    return retorno