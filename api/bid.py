import sys

sys.path.append("../")

import classes.config as config
import classes.database as database
import base64

def sigla(cep):
  destino = int(cep)
  ufdestino = ""
  if ((destino >= 69900000) and (destino <= 69999999)):
    ufdestino = 'AC'
  if ((destino >= 57000000) and (destino <= 57999999)):
    ufdestino = 'AL'
  if ((destino >= 69000000) and (destino <= 69299999)):
     ufdestino = 'AM'
  if ((destino >= 69400000) and (destino <= 69899999)):
     ufdestino = 'AM'
  if ((destino >= 68900000) and (destino <= 68999999)):
     ufdestino = 'AP'
  if ((destino >= 40000000) and (destino <= 48999999)):
     ufdestino = 'BA'
  if ((destino >= 60000000) and (destino <= 63999999)):
     ufdestino = 'CE'
  if ((destino >= 70000000) and (destino <= 72799999)):
     ufdestino = 'DF'
  if ((destino >= 73000000) and (destino <= 73699999)):
     ufdestino = 'DF'
  if ((destino >= 29000000) and (destino <= 29999999)):
     ufdestino = 'ES'
  if ((destino >= 72800000) and (destino <= 72999999)):
     ufdestino = 'GO'
  if ((destino >= 73700000) and (destino <= 76799999)):
     ufdestino = 'GO'
  if ((destino >= 65000000) and (destino <= 65999999)):
     ufdestino = 'MA'
  if ((destino >= 30000000) and (destino <= 39999999)):
     ufdestino = 'MG'
  if ((destino >= 79000000) and (destino <= 79999999)):
     ufdestino = 'MS'
  if ((destino >= 78000000) and (destino <= 78899999)):
     ufdestino = 'MT'
  if ((destino >= 66000000) and (destino <= 68899999)):
     ufdestino = 'PA'
  if ((destino >= 58000000) and (destino <= 58999999)):
     ufdestino = 'PB'
  if ((destino >= 50000000) and (destino <= 56999999)):
     ufdestino = 'PE'
  if ((destino >= 64000000) and (destino <= 64999999)):
     ufdestino = 'PI'
  if ((destino >= 80000000) and (destino <= 87999999)):
     ufdestino = 'PR'
  if ((destino >= 20000000) and (destino <= 28999999)):
     ufdestino = 'RJ'
  if ((destino >= 59000000) and (destino <= 59999999)):
     ufdestino = 'RN'
  if ((destino >= 76800000) and (destino <= 76999999)):
     ufdestino = 'RO'
  if ((destino >= 69300000) and (destino <= 69399999)):
     ufdestino = 'RR'
  if ((destino >= 90000000) and (destino <= 99999999)):
     ufdestino = 'RS'
  if ((destino >= 88000000) and (destino <= 89999999)):
     ufdestino = 'SC'
  if ((destino >= 49000000) and (destino <= 49999999)):
     ufdestino = 'SE'
  if ((destino >= 100000) and (destino <= 19999999)):
     ufdestino = 'SP'
  if ((destino >= 77000000) and (destino <= 77999999)):
     ufdestino = 'TO'
  return ufdestino


def cotar(carrinho):
  
  # retorno padrao 
  cotacao = {}
  httpcode = 200
  
  # validando tag's obrigatorias
  if( not "auth" in carrinho ):
    httpcode = 402
    return cotacao, f"{httpcode} dados recebidos invalidos"
  
  uf = "**"
  if( not "to" in carrinho ):  
    httpcode = 402
    return cotacao, f"{httpcode} tag 'to' nao informado"
  else:
    uf = sigla(carrinho["to"])
  if( uf=="" ):
    httpcode = 408
    return cotacao, f"{httpcode} CEP invalido"
  destino = int(carrinho["to"])

  gramas = 0
  if( not "weight" in carrinho ):  
    httpcode = 402
    return cotacao, f"{httpcode} tag 'weigth' nao informada"
  else:
    gramas = carrinho["weight"]
  if( gramas== 0):
    httpcode = 409
    return cotacao, f"{httpcode} peso invalido"
  if (float(gramas) < 1):
    gramas = (float(gramas) * 1000)

  valor = 0
  if( not "value" in carrinho ):  
    httpcode = 402
    return cotacao, f"{httpcode} tag 'value' nao informado"
  else:
    valor = carrinho["value"]
  
  auth = carrinho["auth"]
  if( auth=="" ):
    httpcode = 500
    return cotacao, f"{httpcode} dados de autenticacao informados incorretamente"
    
  b64 = base64.b64decode(auth).decode('utf-8')
  usuario = b64.split('-')
  if( len(usuario)<8 ):
    httpcode = 500
    return cotacao, f"{httpcode} autenticacao corrompida"
  
  # pegando informacoes da autenticacao
  email = usuario[0]
  apikey = usuario[6]
  token = usuario[7]

  # verificando a uf de destino
  uf = "**"
  if( not "to" in carrinho ):  
    httpcode = 402
    return cotacao, f"{httpcode} CEP nao informado"
  else:
    uf = sigla(carrinho["to"])
  
  # carregando os dados parametrizados no sistema
  sistema = config.system()
  sistema.load()

  # conectando no banco
  db = database.data(sistema)
  db.connect()
  
  # validando usuario
  dados = db.query( 
    f"select c_id \
      ,c.p_apelido \
      ,c_bloqueado \
      ,c.p_id \
      ,c.p_cnpj \
      ,coalesce(c_unidademaster,c_unidade) c_unidade \
      ,u.p_apelido \
      ,u.p_cep \
      ,c_markup \
      ,coalesce(c_bidcacheado,0) c_bidcacheado \
      ,coalesce(c_servidorbid,0) c_servidorbid \
      ,c_bidpesocubado \
      ,c.p_cidade \
      ,c_bidimpostos \
      ,c_debugbid \
      ,c_bid_insereprazodescricao \
      ,coalesce(fretefixotransportadora,0) fretefixotransportadora \
      ,coalesce(fretefixouf,0) fretefixouf \
      ,imp_icms \
      ,imp_iss \
    from clientes \
    left join pessoas c on c.p_id=c_pessoa \
    left join unidades on u_id=c_unidade \
    left join pessoas u on u.p_id=u_pessoa \
    left join regrafrete r on r.rf_cliente=c_id \
    left join impostos on imp_uforigem=u.p_uf and imp_ufdestino='{uf}' \
    left join (select rfft_cliente, 1 fretefixotransportadora from regrafretefixotransportadora limit 1) ff on ff.rfft_cliente=c_id \
    left join (select rffu_cliente, 1 fretefixouf from regrafretefixouf limit 1) ffu on ffu.rffu_cliente=c_id \
    where c_apikey='{apikey}' \
      and c_token='{token}' \
    "
  )
  if( len(dados)==0 ):
    httpcode = 501
    return cotacao, f"{httpcode} autenticacao invalida"
  #
  if( dados[0]["c_bloqueado"]==1 ):
    httpcode = 502
    return cotacao, f"{httpcode} cliente bloqueado"
  #
  origem = int(dados[0]["c_unidade"])
  if (origem == 0) :
    httpcode = 407
    return cotacao, f"{httpcode} origem nao definida"
  #  
  markup = (100 - dados[0]["c_markup"]) / 100
  idcliente = dados[0]["c_id"]
  bidcacheado = dados[0]["c_bidcacheado"]
  pesocubado = dados[0]["c_bidpesocubado"]
  debugbid = dados[0]["c_debugbid"]
  insereprazo = dados[0]["c_bid_insereprazodescricao"]
  fretefixotransportadora = dados[0]["fretefixotransportadora"];
  fretefixouf = dados[0]["fretefixouf"];
  fatorcubagem = 167
  icms = dados[0]["imp_icms"]
  iss = dados[0]["imp_iss"]  
  bidimpostos = dados[0]["c_bidimpostos"]
  limit = ""
  orderby = "order by pv_valor"
  orderarray = "value"
  produtos = []
  
  #
  dados = db.query( \
    f"select coalesce(rf_tipo,0) regra \
      ,coalesce(rf_valor,0) valor \
      ,rf_retira \
      ,rf_vmin \
      ,rf_vmax \
      ,rf_termina \
      ,coalesce(rf_prazoextra,0) rf_prazoextra \
      ,rf_freteminimo \
      ,coalesce(rf_adicionapc,0) rf_adicionapc \
      ,coalesce(rf_adicionavlr,0) rf_adicionavlr \
      ,rf_adicionaexceto \
      ,rf_excuf \
      ,rf_ordem \
      ,rf_qtdretorno \
      ,rf_tipofrete \
      ,rf_excluirtransp \
      ,rf_somatransp1 \
      ,rf_somavalor1 \
      ,rf_excuftransp \
      ,rf_pcregiao \
      ,rf_transpsemimposto \
      ,rf_addvlrporfaixa \
      ,rf_fretegratis \
      ,rf_fretefixo \
      ,rf_fretefixoacima \
      ,rf_veconomico \
      ,rf_vexpresso \
      ,rf_textoeconomico \
      ,rf_textoexpresso \
      ,rf_avvlrtransp \
      ,rf_avvlrtransppeso \
      ,rf_aumentaprazo_uf_transp \
      ,rf_aumentaprazo_uf_transp_obs \
      ,rf_aumentavalor_uf_transp \
    from regrafrete r \
    left join (select rfft_cliente, 1 fretefixotransportadora from regrafretefixotransportadora limit 1) ff on ff.rfft_cliente=rf_cliente \
    left join (select rffu_cliente, 1 fretefixouf from regrafretefixouf limit 1) ffu on ffu.rffu_cliente=rf_cliente \
    where rf_cliente={idcliente} \
    and ((rf_vmin=0 and rf_vmax=0) or ((rf_vmin>0 and {valor}>rf_vmin) or (rf_vmax>0 and {valor}<rf_vmax))) \
    and ((rf_comeca <= current_timestamp and rf_termina >= current_timestamp) or rf_id is null) \
    ")
  if (len(dados) == 0):
    httpcode = 505
    return cotacao, f"{httpcode} nao ha regra de frete ou transportadora disponivel para o peso {gramas} gramas"
  
  regra = dados[0]["regra"]
  regravalor = dados[0]["valor"]
  regravmin = dados[0]["rf_vmin"]
  regravmax = dados[0]["rf_vmax"]
  retira = dados[0]["rf_retira"]
  regratermina = dados[0]["rf_termina"]
  prazoregra = dados[0]["rf_prazoextra"]
  freteminimo = dados[0]["rf_freteminimo"]
  adicionavlr = dados[0]["rf_adicionavlr"]
  adicionapc = dados[0]["rf_adicionapc"]
  adicionaexceto = dados[0]["rf_adicionaexceto"]
  excuf = dados[0]["rf_excuf"]
  tipofrete = dados[0]["rf_tipofrete"]
  excluirtransp = dados[0]["rf_excluirtransp"]
  excuftransp = dados[0]["rf_excuftransp"]
  somatransp1 = dados[0]["rf_somatransp1"]
  somavalor1 = dados[0]["rf_somavalor1"]
  pcregiao = dados[0]["rf_pcregiao"]
  traspsemimposto = dados[0]["rf_transpsemimposto"]
  addvlrporfaixa = dados[0]["rf_addvlrporfaixa"]
  fretegratis = dados[0]["rf_fretegratis"]
  fretefixo = dados[0]["rf_fretefixo"]
  fretefixoacima = dados[0]["rf_fretefixoacima"]
  veconomico = dados[0]["rf_veconomico"]
  vexpresso = dados[0]["rf_vexpresso"]
  textoeconomico = dados[0]["rf_textoeconomico"]
  textoexpresso = dados[0]["rf_textoexpresso"]
  addvlrportranspfx = dados[0]["rf_avvlrtransp"]
  addvlrportransppeso = dados[0]["rf_avvlrtransppeso"]
  aumentaprazo_uf_transp = dados[0]["rf_aumentaprazo_uf_transp"]
  aumentoprazo_uf_transp_obs = dados[0]["rf_aumentaprazo_uf_transp_obs"]
  aumentovalor_uf_transp = dados[0]["rf_aumentavalor_uf_transp"]
  
  ordertype = 0
  if( "ordertype" in carrinho) :
    ordertype = carrinho["ordertype"]

  # montando dimensões para calcular peso cubado
  dimensionInfo = 0
  dimensionWidth = 0
  dimensionHeight = 0
  dimensionLength = 0
  dimensionM3 = 0
  if ("dimensions" in carrinho):
    if ((carrinho["dimensions"]["width"] > 0) and (carrinho["dimensions"]["height"] > 0) and (carrinho["dimensions"]["length"] > 0)):
      dimensionInfo = 1
      dimensionWidth = carrinho["dimensions"]["width"] / 100
      dimensionHeight = carrinho["dimensions"]["height"] / 100
      dimensionLength = carrinho["dimensions"]["length"] / 100
      dimensionM3 = (((dimensionWidth * dimensionHeight * dimensionLength) * 167) * 1000)
  
  # caso haja informações sobre a origem da cotação    
  if( "data" in carrinho ):
    origin = carrinho["data"]

  # montando condicao para filtrar tipo do frete
  tf_analise = int(tipofrete)
  if (int(tf_analise) == 0):
    tipofrete = "0,1,2"
  elif (tf_analise == 1):
    tipofrete = "0,1"
  elif (tipofrete == "2"):
    tipofrete = "0,2"
    
  # gerando percentuais de imostos
  if (bidimpostos == 0):
    icms = 0
    iss = 0
  if (icms > 0):
    icms = (100 - icms) / 100
  if (iss > 0):
    iss = (100 - iss) / 100

  # definindo qual a ordem do retorno (prazo ou valor)
  if (dados[0]["rf_ordem"] == 1):
    orderby = "order by pv_prazo"
    orderarray = "deadline"
  
  # limite de retorno na cotacao
  if (int(dados[0]["rf_qtdretorno"]) > 0):
    limit = "limit " . dados[0]["rf_qtdretorno"]
      
  """
  -> MODELO DE COTACAO COM PRODUTOS
  {
    "to" => "CEP"
    ,"weight" => 0
    ,"auth" => ""
    ,"value" => 0
    ,"products" => [
      {
        "productQty": 1,
        "PlanUuId": "FRI20RS",
        "productName": "Frigideira 20cm",
        "productCompany": 465,
      }
    ],  
  }
  """

  # controle de requisicoes/minuto
  #$rr = $ol_banco->query("select req_id,req_qtdade from requisicoes where req_cliente=$idcliente and req_requisicao=0 and req_ano=".date("Y")." and req_mes=".date("m")." and req_dia=".date("d")." and req_hora=".date("H")." and req_minuto=".date("i"));
  #if( count($rr)==0 ){
  #  $ol_bancoprimario->query("insert into requisicoes (req_cliente,req_requisicao,req_ano,req_mes,req_dia,req_hora,req_minuto,req_qtdade) values ($idcliente,0,".date("Y").",".date("m").",".date("d").",".date("H").",".date("i").",1)");
  #} 
  #else {
  #  $ol_bancoprimario->query("update requisicoes set req_qtdade=req_qtdade+1 where req_id=".$rr[0]["req_id"]);
  #  if( $rr[0]["req_qtdade"]>120 ){
  #    header($_SERVER["SERVER_PROTOCOL"] . " 503 excesso de requisicoes, efetuado ".$rr[0]["req_qtdade"]." requisicoes neste minuto (limite 120 por minuto)");
  #    exit;  
  #  }
  #}
  #
    
  # gravando
  #if( $debugbid==1 ){
  #  $ol_bancoprimario->conecta();
  #  $ol_bancoprimario->query("insert into lixo (recebido,cliente,regra,regravalor) values ('$recebido',$idcliente,'0','0')");
  #  if( $ol_bancoprimario->erro()!="" ){
  #    echo $ol_bancoprimario->erro();
  #    exit;
  #  }
  #  if($origin!=""){
  #    $ol_bancoprimario->query("insert into lixo (recebido,cliente,regra,regravalor) values ('$origin',$idcliente,'original','0')");
  #  }
  #}
  #

  # define em que servidor - banco - sera feito a cotacao
  #if ($dados[0]["c_servidorbid"] == 1) {
  #  $databid = new ol_banco("vm23-1.enivix.com.br", "postgres", "f4m1l14-4m0r1m", "enivix", "postgre", 5643);
  #  $databid->connecta();
  #} else
  #  $databid = $ol_banco;

  # calculando um novo peso (por produto)
  novopeso = gramas
  if (pesocubado == 1) :
    lista = "*"
    if ('products' in carrinho) :
      lista = ""
      sep = ""
      for tmp in carrinho["products"]:
        if (lista != ""):
          sep = ","
        lista = f"{lista}{sep}'{tmp['PlanUuId']}'"
      #
      produtos = db.query(f"select prd_sku,prd_altura,prd_largura,prd_comprimento,0 pesocubado from produtos where prd_cliente={idcliente} and prd_sku in ({lista})")
      novopeso = 0
      for i in range(len(produtos)):
        produtos[i]["pesocupado"] = ((produtos[i]["prd_altura"] / 100) * (produtos[i]["prd_largura"] / 100) * (produtos[i]["prd_comprimento"] / 100))
        novopeso += ((produtos[i]["prd_altura"] / 100) * (produtos[i]["prd_largura"] / 100) * (produtos[i]["prd_comprimento"] / 100))
      
      novopeso = (novopeso * 100);
    #
    if ('items' in carrinho) :
      lista = ""
      kits = 0
      oldkit = ""
      s = ""
      pesoitens = 0
      for tmp in carrinho["items"]:
        if ((oldkit != tmp["kit"]) and (tmp["kit"] != tmp["sku"])) :
          kits = kits + 1
          oldkit = tmp["kit"]
        #
        pesoitens += tmp["unitWeight"]
        lista += s + "'" + tmp["sku"] + "'"
        s = ","
      #
      if (kits == 0):
        kits = 1
      if (pesoitens > gramas):
        gramas = pesoitens
      produtos = db.query(f"select prd_sku,prd_altura,prd_largura,prd_comprimento,0 pesocubado from produtos where prd_cliente={idcliente} and prd_sku in ({lista})")
      novopeso = 0
      chk = 0
      for i in range(len(produtos)):
        if (((produtos[i]["prd_altura"] / 100) * (produtos[i]["prd_largura"] / 100) * (produtos[i]["prd_comprimento"] / 100)) > chk) :
          chk = ((produtos[i]["prd_altura"] / 100) * (produtos[i]["prd_largura"] / 100) * (produtos[i]["prd_comprimento"] / 100))
          novopeso += ((produtos[i]["prd_altura"] / 100) * (produtos[i]["prd_largura"] / 100) * (produtos[i]["prd_comprimento"] / 100))
        #
        produtos[i]["pesocupado"] = ((produtos[i]["prd_altura"] / 100) * (produtos[i]["prd_largura"] / 100) * (produtos[i]["prd_comprimento"] / 100))
      #
      if (kits > 0):
        novopeso = novopeso * kits
    #
    if (lista == "*"):
      pesocubado = 0
  #
  if (dimensionInfo == 1) :
    if (dimensionM3 > gramas):
      gramas = dimensionM3
  #

  # lista de sku's para regra de frete
  listaSku = "";
  listaKit = "";
  if 'items' in carrinho:
    for tmp in carrinho:
      if( 'sku' in tmp):
        listaKit += tmp["sku"] + ";"
      if( 'kit' in tmp):
        listaSku += tmp["kit"] + ";"
    #
  #

  """
  if( $bidcacheado==1 ){
    $sql = "select cot_id,cot_resultado from cotacao where cot_cliente=$idcliente and cot_cep=$destino and cot_peso={gramas} order by cot_id desc limit 1";
    $cache = $databid->query($sql);
    if( count($cache) ){
      header($_SERVER["SERVER_PROTOCOL"] . " 200 OK");
      header('Content-Type: application/json; charset=utf-8');
      $bids = json_decode($cache[0]["cot_resultado"]);  
      echo json_encode($bids);
      exit;
    }
  }
  """

  sql = f"select tfc_nome \
    ,tfc_seguro \
    ,tfc_gris \
    ,tfc_apelido \
    ,tfc_transportadora \
    ,pv_valor \
    ,pv_prazo \
    ,pv_gris \
    ,pv_seguro \
    ,pv_observacao \
    ,pv_tabela \
    ,pv_pricepercent \
    ,pv_pesoextra \
    ,pv_ipeso \
    ,fcu_uf \
    ,ci_id \
    ,bpe_extra \
    ,tc_sameday \
    ,case when {pesocubado}=1 then ({novopeso}*trn_fatorpesocubado)*1000 else -1 end pesocubado  \
    from tabelafretecomercial c \
    left join padraovtex v on tfc_id=pv_tabela  \
    left join faixacepuf on {destino} between fcu_icep and fcu_fcep \
    left join cotacaoignora on ci_cliente={idcliente} and ci_transportadora=tfc_transportadora and ci_uf=fcu_uf \
    left join bidprazoextra on bpe_transportadora=tfc_transportadora and current_date between bpe_inicio and bpe_final \
    left join transportadoras on trn_id=tfc_transportadora \
    left join transportadorascliente on tc_cliente={idcliente} and tc_transportadora=tfc_transportadora \
    left join cepbloqueado on cb_transportadora=tfc_transportadora and {destino} between cb_icep and cb_fcep \
    where pv_origem={origem} \
    and cb_id is null \
    and {destino} between pv_icep and pv_fcep \
    and coalesce(bpe_extra,0)<100 \
    and case when {pesocubado}=1 \
        then \
          case when trn_pesoisencao>0 then case when ({novopeso}*trn_fatorpesocubado)*1000  < trn_pesoisencao then {gramas} else ({novopeso}*trn_fatorpesocubado)*1000 end \
              when trn_pesoisencao=0 then case when ({novopeso}*trn_fatorpesocubado)*1000 > {gramas} then ({novopeso}*trn_fatorpesocubado)*1000 else {gramas} end \
              else {gramas} \
          end \
        else {gramas} \
        end between pv_ipeso and pv_fpeso \
        and (tfc_limitepeso>=case \
                              when {pesocubado}=1 \
                              then \
                              case when trn_pesoisencao>0 then case when ({novopeso}*trn_fatorpesocubado)*1000  < trn_pesoisencao then {gramas} else ({novopeso}*trn_fatorpesocubado)*1000 end \
                                    when trn_pesoisencao=0 then case when ({novopeso}*trn_fatorpesocubado)*1000 > {gramas} then ({novopeso}*trn_fatorpesocubado)*1000 else {gramas} end \
                                    else {gramas} \
                                end \
                              else {gramas} \
                              end or tfc_limitepeso=0) \
    and (tfc_limitevalor>={valor} or tfc_limitevalor=0) \
    and pv_tipo in ({tipofrete}) \
    and tfc_transportadora in (select tc_transportadora from transportadorascliente  \
                                left join transportadoras on trn_id=tc_transportadora \
                                where tc_cliente={idcliente} \
                                and tc_transportadora=tfc_transportadora \
                                and tc_ativo=1 \
                                and coalesce(tc_contratoproprio,0)=0 \
                                and (case when {pesocubado}=1 \
                                      then \
                                      case when trn_pesoisencao>0 then case when ({novopeso}*trn_fatorpesocubado)*1000  < trn_pesoisencao then {gramas} else ({novopeso}*trn_fatorpesocubado)*1000 end \
                                            when trn_pesoisencao=0 then case when ({novopeso}*trn_fatorpesocubado)*1000 > {gramas} then ({novopeso}*trn_fatorpesocubado)*1000 else {gramas} end \
                                            else {gramas} \
                                        end \
                                      else {gramas} \
                                      end between 0 and trn_limitepeso or trn_limitepeso=0) \
                                and ({valor} between 0 and trn_limitevalor or trn_limitevalor=0)) \
    and ci_id is null \
    {orderby} \
    {limit}"
  regioes = db.query(sql)
  if (len(regioes) == 0) :
    httpcode = 410
    return cotacao, f"{httpcode} CEP nao contemplado por nenhuma regiao ou regra de frete"
  #

  # se tiver frete fixo por transportadora
  fft = [];
  if (fretefixotransportadora == 1) :
    fft = db.query(f"select rfft_transportadora,rfft_uf,rfft_valor from regrafretefixotransportadora where rfft_cliente={idcliente}")
  #

  # se tiver frete fixo por UF
  ffuf = []
  if (fretefixouf == 1):
    ffuf = db.query(f"select rffu_uf,rffu_valor from regrafretefixouf where rffu_cliente={idcliente}")
  #

  somase = []
  if (somatransp1 != ""):
    somase = somatransp1.split(",")
    
  opcoes = []
  old = ""

  for regiao in regioes:
    sameday = regiao["tc_sameday"];
    
    # se estiver excluindo um estado/transportadora da cotação
    # SP-MG;3,RJ-ES-MG-4
    if (excuftransp != "") :
      l = excuftransp.split(",")
      pula = False
      for leu in l:
        lex = leu.split(";")
        if (not type(lex)==list) :
          break
        #
        if (len(lex) == 0) :
          break
        #
        le = lex[0].split("-")
        lidt = leu.split(";")
        if (len(lidt)<1):
          continue
        for le2 in le:
          if ((lidt[1] == regiao["tfc_transportadora"]) and (le2 == sigla(destino))) :
            pula = True
            break
          #
        #
        if (pula == True):
          break
      #
      if (pula == True):
        continue
    #

    #if( $regra==3 ){
    #  if( ";".$regiao["tfc_apelido"].";"== $retira )
    #    continue;
    #}
    if (retira != "") :
      l = retira.split(";")
      b = 0
      for l1 in l:
        if (l1 == regiao["tfc_apelido"].upper() ) :
          b = 1
          break
        #
      #
      if (b == 1):
        continue
    #
    tabelacomercial = regiao["pv_tabela"]

    if (old != regiao["tfc_apelido"]) :
      old = regiao["tfc_apelido"]
    else :
      continue
    #

    extraWeight = 0
    extraValue = 0
    if (float(regiao["pv_pesoextra"]) > 0) :
      extraWeight = (gramas - regiao["pv_ipeso"])
      extraValue = regiao["pv_pesoextra"] * (extraWeight / 1000)
    #

    v = regiao["pv_valor"] + extraValue
    fixo = 0
    if (regra == 6) :
      foreach ($fft as $row) {
        if ($row["rfft_transportadora"] == $regiao["tfc_transportadora"]) {
          $faz = 1;
          if ($row["rfft_uf"] != "") {
            $faz = 0;
            if (uf($destino, $row["rfft_uf"]) == 1)
              $faz = 1;
          }
          if ($faz == 1) {
            $v = number_format($row["rfft_valor"], 2, ".", "");
            $fixo = 1;
          }
        }
      }
    }
    if ($regra == 7) {
      foreach ($ffuf as $row) {
        $faz = 1;
        if ($row["rffu_uf"] != "") {
          $faz = 0;
          if (uf($destino, $row["rffu_uf"]) == 1)
            $faz = 1;
        }
        if ($faz == 1) {
          $v = number_format($row["rffu_valor"], 2, ".", "");
          $fixo = 1;
        }
      }
    }
    if (count($somase) > 0) {
      foreach ($somase as $ss) {
        if ($ss == $regiao["tfc_transportadora"]) {
          $v += $somavalor1;
        }
      }
    }

    $o = $v;
    $gris = 0;
    $seguro = 0;
    $grisseguro = 0;
    $observacao = "";
    $prazo = $regiao["pv_prazo"];
    $prazoextra = ($regiao["bpe_extra"]);
    $prazo += $prazoextra;
    $prazo += $prazoregra;
    if ($sameday == 1)
      $prazo = 1;
    else {
      $hoje = strtotime("now");
      $original = $hoje;
      $mudou = 1;
      while( $mudou==1 ){
        $mudou = 0;
        switch( date("w", $hoje) ){
          case "0": $prazo++; $mudou=1; break;
          case "6": $prazo+=2; $mudou=1; break;
          default:
            $feriados = $databid->query("select fer_id from feriados where fer_unidade=$origem and fer_dia=".date("d",$hoje)." and fer_mes=".date("m",$hoje)." and fer_ano in (0,".date("Y",$hoje).")");
            if( count($feriados)>0 ){
              $prazo++;
              $mudou = 1;
            }
            break;
        }
        $hoje = strtotime("+$prazo days");
      }
    }

    if( $aumentaprazo_uf_transp!= "" ){
      # 4;RS;2,3;RS;3
      $l = explode(",", $aumentaprazo_uf_transp);
      foreach ($l as $leu) {
        $lex = explode(";", $leu);
        if (!is_array($lex)) {
          break;
        }
        if (count($lex) == 0) {
          break;
        }
        $le = $lex[0]; # id transp
        $lufs = explode("-", $lex[1]); # vlr da faixa
        $ladd = $lex[2];
        foreach ($lufs as $le2) {
          if( ($le2 == sigla($obj->to) and ($le==$regiao["tfc_transportadora"])) ) {
            $prazo += intval($ladd);
            $observacao = $aumentoprazo_uf_transp_obs;
            break;
          }
        }
      }
    }

    $impostos = 1;
    if ($regiao["pv_observacao"] != "")
      $observacao = $regiao["pv_observacao"];
    if ($fixo == 0) {
      if (floatval($markup) > 0) {
        $v = ($v / $markup);
        if ($regra == "8") {
          $markup = 0;
        }
        if (isset($obj->value)) {
          if (floatval($obj->value) > 0) {
            #
            if (floatval($regiao["pv_pricepercent"]) == 0) {
              if (floatval($regiao["tfc_seguro"]) > 0)
                $seguro = number_format(($obj->value * (floatval($regiao["tfc_seguro"]) / 100)), 2, ".", "");
              if ($regiao["pv_seguro"] > 0)
                $seguro = number_format(($obj->value * (floatval($regiao["pv_seguro"]) / 100)), 2, ".", "");
              #
              if (floatval($regiao["tfc_gris"]) > 0)
                $gris = number_format($obj->value * (floatval($regiao["tfc_gris"]) / 100), 2, ".", "");
              if ($regiao["pv_gris"] > 0)
                $gris = number_format(($obj->value * (floatval($regiao["pv_gris"]) / 100)), 2, ".", "");
              #
              $grisseguro = number_format((floatval($gris) + floatval($seguro)), 2, ".", "");
              $seguro = 0;
              $gris = 0;
            } else {
              if ($regiao["pv_pricepercent"] > 0)
                $grisseguro = number_format(($obj->value * (floatval($regiao["pv_pricepercent"]) / 100)), 2, ".", "");
            }
          }
        }
      }

      # frete gratis
      # SP-MG-RJ;0-99;0.00
      #,SC-RS;0-99;1.00
      $achoufretegratis = false;
      if ($fretegratis != "") {
        $l = explode(",", $fretegratis);
        $achei = false;
        foreach ($l as $leu) {
          $lex = explode(";", $leu);
          if (!is_array($lex)) {
            break;
          }
          if (count($lex) == 0) {
            break;
          }
          $le = explode("-", $lex[0]); # lista de estados
          $fxs = explode("-", $lex[1]); # vlr da faixa
          $vlrfx = $lex[2];
          foreach ($le as $le2) {
            if ($le2 == sigla($obj->to) and (($obj->value >= $fxs[0]) and ($obj->value <= $fxs[1]))) {

              $achei = true;
              $achoufretegratis = true;

              if (($regra != "8")) {
                $v = $vlrfx;
                $seguro = 0;
                $gris = 0;
                $grisseguro = 0;
                $impostos = 0;
                $markup = 0;
                $achei = true;
                $regiao["tfc_apelido"] = "Frete gratis [id:" . $regiao["tfc_transportadora"] . "]";
              } else {
                if (substr($regiao["tfc_apelido"], 0, 14) == "Entrega Padrao") {
                  $v = $vlrfx;
                  $seguro = 0;
                  $gris = 0;
                  $grisseguro = 0;
                  $markup = 0;
                  $impostos = 0;
                }
                if (substr($regiao["tfc_apelido"], 0, 16) != "Entrega Expressa")
                  $regiao["tfc_apelido"] = "Entrega padrao - $textoeconomico [id:" . $regiao["tfc_transportadora"] . "]";
              }
              break;
            }
          }
          if ($achei == true)
            break;
        }
      }
      #

      $aplica = true;
      if ($regratermina != "") {
        if (strtotime($regratermina) <= strtotime(date("Y-m-d H:i")))
          $aplica = false;
      }
      if ($aplica) {
        if ($regra == 3) {
          if (($regiao["fcu_uf"] == "") or (($regiao["fcu_uf"] != "") and ($regiao["fcu_uf"] != $excuf))) {
            $v = $regravalor;
            $seguro = 0;
            $gris = 0;
            $grisseguro = 0;
            $impostos = 0;
          }
        }
        if ($regra == 4) {
          if ((";" . $regiao["tfc_apelido"] . ";" != $retira) and ($obj->value >= $regravmin)) {
            if (($regiao["fcu_uf"] == "") or (($regiao["fcu_uf"] != "") and ($regiao["fcu_uf"] != $excuf))) {
              $v = $regravalor;
              $seguro = 0;
              $gris = 0;
              $grisseguro = 0;
              $impostos = 0;
            }
          }
        }
        if ($regra == 40) {
          $fez = 0;
          if ((";" . $regiao["tfc_apelido"] . ";" != $retira) and ($obj->value >= $regravmin)) {
            if (($regiao["fcu_uf"] == "") or (($regiao["fcu_uf"] != "") and ($regiao["fcu_uf"] != $excuf))) {
              $fez = 1;
              $v = $regravalor;
              $seguro = 0;
              $gris = 0;
              $grisseguro = 0;
              $impostos = 0;
            }
          }
          if ($fez == 0) {
            if (($obj->value >= $regravmin)) {
              $regiao["tfc_apelido"] = "Frete Gratis (" . $regiao["tfc_apelido"] . ")";
              $v = 0;
              $seguro = 0;
              $gris = 0;
              $grisseguro = 0;
              $impostos = 0;
            }
          }
        }
      }
      $v = (floatval($v) + floatval($seguro) + floatval($gris) + floatval($grisseguro));

      if (($impostos == 1) and ($regra != "8")) {
        $tmp2 = $traspsemimposto;
        if ($tmp2 == "")
          $tmp2 = "2";
        $tmp3 = explode(";", $tmp2);
        $tmp4 = true;
        foreach ($tmp3 as $tmp5) {
          if ($tmp5 == $regiao["tfc_transportadora"]) {
            $tmp4 = false;
            break;
          }
        }
        if (($tmp4)) {
          if ($icms > 0) {
            $v = $v / $icms;
          }
          if ($iss > 0) {
            $v = $v / $iss;
          }
        } else
          $impostos = 0;
      }

      # aumento de % por regiao/uf
      # ex: '35;SP-RJ-MG-ES,5;AC-AL-AP-AM-BA-CE-DF-GO-MA-MT-MS-PA-PB-PR-PE-PI-RN-RS-RO-RR-SC-SE-TO'
      if ($pcregiao != "") {
        $pcs = explode(",", $pcregiao);
        foreach ($pcs as $p) {
          $a = explode(";", $p);
          $b = floatval($a[0]);
          $c = explode("-", $a[1]);
          $sai = false;
          foreach ($c as $d) {
            if ($d == sigla($obj->to)) {
              $adicionapc = $b;
              $sai = true;
              break;
            }
          }
          if ($sai)
            break;
        }
      }

      # aumento de valor por faixa de preços
      # ex: '0-250;5,250-350;10,350-600;25,600-1000;25,1000-999999;35'
      if ($addvlrporfaixa != "") {
        $pcs = explode(",", $addvlrporfaixa);
        foreach ($pcs as $p) {
          $avpf_faixa = explode(";", $p);
          $avpf_vlrs = explode("-", $avpf_faixa[0]);
          $avpf_de = floatval($avpf_vlrs[0]);
          $avpf_ate = floatval($avpf_vlrs[1]);
          $avpf_vlr = floatval($avpf_faixa[1]);

          if ((floatval($obj->value) >= $avpf_de) and (floatval($obj->value) <= $avpf_ate)) {
            $adicionavlr = $avpf_vlr;
            break;
          }
        }
      }

      if( $aumentovalor_uf_transp!= "" ){
        # 4;RS;2,3;RS;3
        $l = explode(",", $aumentovalor_uf_transp);
        foreach ($l as $leu) {
          $lex = explode(";", $leu);
          if (!is_array($lex)) {
            break;
          }
          if (count($lex) == 0) {
            break;
          }
          $le = $lex[0]; # id transp
          $lufs = explode("-", $lex[1]); # vlr da faixa
          $ladd = $lex[2];
          foreach ($lufs as $le2) {
            if( ($le2 == sigla($obj->to) and ($le==$regiao["tfc_transportadora"])) ) {
              $v += floatval($ladd);
              break;
            }
          }
        }
      }

      # aumento de valor por faixa de preços e transportadoras
      # ex: '3;0-100;10,3;100-200;5,4;0-100;3,4-100-9999;1'
      # <id transp>;<vlr inicial>-<valor final>;<valor a adicionar>,...
      $adicionavlr2 = 0;
      if ($addvlrportranspfx != "") {
        $pcs = explode(",", $addvlrportranspfx);
        foreach ($pcs as $p) {
          $avpf_faixa = explode(";", $p);
          $avpf_idtransp = $avpf_faixa[0];        
          $avpf_vlrs = explode("-", $avpf_faixa[1]);
          $avpf_de = floatval($avpf_vlrs[0]);
          $avpf_ate = floatval($avpf_vlrs[1]);
          $avpf_vlr = floatval($avpf_faixa[2]);

          if ((floatval($gramas) >= $avpf_de) and (floatval($gramas) <= $avpf_ate) and $avpf_idtransp==$regiao["tfc_transportadora"]) {
            $adicionavlr2 = $avpf_vlr;
            $v = $v + $adicionavlr2;
            break;
          }
        }
      }

      # aumento de valor por faixa de preços e transportadoras
      # ex: '3;0-100;10,3;100-200;5,4;0-100;3,4-100-9999;1'
      # <id transp>;<vlr inicial>-<valor final>;<valor a adicionar>,...
      $adicionavlr3 = 0;
      if ($addvlrportransppeso != "") {
        $pcs = explode(",", $addvlrportransppeso);
        foreach ($pcs as $p) {
          $avpf_faixa = explode(";", $p);
          $avpf_idtransp = $avpf_faixa[0];        
          $avpf_vlrs = explode("-", $avpf_faixa[1]);
          $avpf_de = floatval($avpf_vlrs[0]);
          $avpf_ate = floatval($avpf_vlrs[1]);
          $avpf_vlr = floatval($avpf_faixa[2]);

          if ((floatval($gramas) >= $avpf_de) and (floatval($gramas) <= $avpf_ate) and $avpf_idtransp==$regiao["tfc_transportadora"]) {
            $adicionavlr3 = $avpf_vlr;
            $v = $v + $adicionavlr3;
            break;
          }
        }
      }

      # adicionando valores
      $adicionar = true;
      $zzz = 0;
      if (($adicionaexceto != "") and (($adicionapc > 0) or ($adicionavlr > 0))) {
        $tmp = explode(";", $adicionaexceto);
        foreach ($tmp as $tmp1) {
          if ($tmp1 == $regiao["tfc_transportadora"]) {
            $adicionar = false;
            break;
          }
        }
      }
      if ($adicionar == true) {
        if ($adicionavlr > 0) {
          $v = number_format($v + $adicionavlr, 2, ".", "");
        }
        if ($adicionapc > 0) {
          $pc = $adicionapc / 100;
          $zzz = $v * $pc;
          $v = $v + $zzz;
        }
      }

      if (($obj->value > $fretefixoacima) and ($fretefixoacima > 0)) {
        $v = $fretefixo;
        $seguro = 0;
        $gris = 0;
        $grisseguro = 0;
        $impostos = 0;
      }
    }
    /*  
    #---- debug
    echo "------------------------------\r\n";
    echo $regiao["tfc_apelido"]."\r\n";
    echo $regiao["tfc_transportadora"]."\r\n";
    echo "sem markup ".$regiao["pv_valor"]."\r\n";
    echo "o markup $markup \r\n";      
    echo "advalorem $grisseguro \r\n";      
    echo "valor + pc $zzz \r\n";      
    echo "adicionar $adicionar \r\n";      
    echo "adicionar vlr $adicionavlr \r\n";      
    echo "com markup $v \r\n";
    */

    if ($v < $freteminimo)
      $v = $freteminimo;

    $complementCompany = "";
    if( $insereprazo==1 )
      $complementCompany = " (" . ($sameday != 1 ? ($prazo . " dia" . (($prazo > 1) ? "s" : "")) : "sameday") . ")";
    if ($regra == 8)
      $complementCompany = "";
    $opcoes[] =
      [
        "shipmentCompany" => $regiao["tfc_apelido"] . $complementCompany,
        "deadline" => $prazo == null ? 0 : intval($prazo),
        "value" => number_format($v, 2, ".", ""),
        "observation" => $observacao,
        "safe" => $seguro,
        "cost" => $gris,
        "safecost" => $grisseguro,
        "role" => $regra,
        "extra" => intval($prazoextra),
        "extraWeight" => number_format(floatval($extraWeight), 0, ".", ""),
        "extraValue" => number_format(floatval($extraValue), 0, ".", ""),
        #"weight" => intval($gramas),
        "icms" => $icms,
        "iss" => $iss,
        "aplicou" => $impostos,
        "weightm3" => intval($dimensionInfo == 1 ? $dimensionM3 : $regiao["pesocubado"]),
        "shipmentId" => $regiao["tfc_transportadora"],
        "state" => sigla($obj->to)
      ];

    if (($achoufretegratis) and ($regra != "8"))
      break;
  }
  if (count($opcoes) == 0) {
    registraOcorrencia($ol_banco, $dados[0]["c_id"], "*", "peso nao suportado", "peso nao suportado", $recebido);
    header($_SERVER["SERVER_PROTOCOL"] . " 411 peso nao suportado");
    exit;
  }

  # se houver mais de uma opçao remove a que se enquara na regra abaixo
  $listaexc = [];
  if (($excluirtransp != "") and (count($opcoes) > 1)) {
    $listaexc = explode(",", $excluirtransp);
  }

  $ordenado = arraySort($opcoes, $orderarray);

  $opcoes = $ordenado;
  $bids = [];
  foreach ($opcoes as $bid) {

    $continua = 1;
    foreach ($listaexc as $exc) {
      if ($exc == $bid["shipmentId"]) {
        $continua = 0;
        break;
      }
    }
    if ($continua == 0)
      continue;

    #
    if ($idcliente == 14) {
      $bid["shipmentCompany"] = str_replace("Platinum", "Correios", $bid["shipmentCompany"]);
    }

    # nao permite duplicicado
    $duplicado = false;
    foreach ($bids as $x) {
      if ($x["shipmentCompany"] == $bid["shipmentCompany"]) {
        $duplicado = true;
        break;
      }
    }
    if ($duplicado)
      continue;
    #

    if ((count($bids) == 0) and ($regra == "1")) {
      # regra 0 = é o frete mais barato sem custo para o cliente
      $bids =
        [
          [
            "shipmentCompany" => $bid["shipmentCompany"],
            "deadline" => $bid["deadline"],
            "value" => 0,
            "observation" => $bid["observation"],
            "safe" => $bid["safe"],
            "cost" => $bid["cost"],
            "safecost" => $bid["safecost"],
            "shipmentId" => $bid["shipmentId"],
            "role" => $regra
          ]
        ];
      break;
    }
    if ((count($bids) == 0) and ($regra == "2")) {
      # regra 2 = é o frete mais barato com custo fixo para o cliente
      $bids =
        [
          [
            "shipmentCompany" => $bid["shipmentCompany"],
            "deadline" => $bid["deadline"],
            "value" => number_format((floatval($regravalor) + floatval($bid["safe"]) + floatval($bid["cost"])), 2, ".", ""),
            "observation" => $bid["observation"],
            "safe" => $bid["safe"],
            "cost" => $bid["cost"],
            "safecost" => $bid["safecost"],
            "shipmentId" => $bid["shipmentId"],
            "role" => $regra
          ]
        ];
      break;
    }
    $bids[] = $bid;
  }

  # gravando a cotacao
  #if( $debugbid==1 ){
  #  if( $ol_bancoprimario==null )
  #    $$ol_bancoprimario->conecta();
  #  $ol_bancoprimario->query("insert into cotacao (cot_cliente,cot_cep,cot_peso,cot_idchamada,cot_resultado) values ($idcliente,$destino,$gramas,'','".json_encode($bids)."')");
  #  $sql = "select cot_id from cotacao where cot_cliente=$idcliente and cot_cep=$destino and cot_peso=$gramas order by cot_id desc limit 1";
  #  $idcotacao = $databid->query($sql);
  #  if( count($idcotacao)>0 )
  #    $idcotacao = intval($idcotacao[0]["cot_id"]);
  #  else 
  #    $idcotacao = -1;
  #  foreach( $bids as &$b ){
  #    $b["idprice"] = $idcotacao;
  #    #$b["qty"] = count($bids);
  #  }
  #  unset($b);
  #}
  #
  header($_SERVER["SERVER_PROTOCOL"] . " 200 OK");
  header('Content-Type: application/json; charset=utf-8');
  echo json_encode($bids);



  return dados
  if( db.error=="ok" ):
    return {"code":200}
  else:
    return {"code":400}